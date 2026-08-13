package executor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/credprovider"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PackageResolver handles file://, http(s)://, and nacos:// package URIs.
type PackageResolver struct {
	ImportDir  string // e.g. /tmp/import
	ExtractDir string // e.g. /tmp/import/extracted

	// CredClient is used when a nacos:// package URI includes ?authType=sts-agentteams
	// (STS via agentteams-credential-provider). Optional for user/password or none.
	CredClient credprovider.Client
}

func NewPackageResolver(importDir string) *PackageResolver {
	// 逻辑说明：在导入根下建立固定 `extracted` 缓存目录并返回 resolver；目录创建为尽力操作，真正写入失败会在 ResolveAndExtract 中带上下文报告。
	extractDir := filepath.Join(importDir, "extracted")
	os.MkdirAll(extractDir, 0755)
	return &PackageResolver{ImportDir: importDir, ExtractDir: extractDir}
}

// Resolve downloads or locates a package and returns the local path.
// For nacos:// URIs the result is a directory; for all others it is a ZIP file.
// Supported schemes: file://, http://, https://, nacos://
func (p *PackageResolver) Resolve(ctx context.Context, uri string) (string, error) {
	// 逻辑说明：解析 package URI 并按 file/http/nacos/oss/相对 MinIO 路径分派下载器；日志先移除 URL 密码，各路径利用本地缓存，下载失败不返回部分路径。
	if uri == "" {
		return "", nil
	}
	logger := log.FromContext(ctx)

	parsed, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("invalid package URI %q: %w", uri, err)
	}
	safeURI := safePackageURI(uri)
	scheme := parsed.Scheme
	if scheme == "" {
		scheme = "minio"
	}
	logger.Info("package resolve started", "package", safeURI, "scheme", scheme)

	switch parsed.Scheme {
	case "file":
		resolved, err := p.resolveFile(parsed)
		if err != nil {
			return "", err
		}
		logger.Info("package pull completed", "package", safeURI, "scheme", scheme, "path", resolved, "format", packagePathFormat(resolved))
		return resolved, nil
	case "http", "https":
		logger.Info("package pull started", "package", safeURI, "scheme", scheme)
		resolved, err := p.resolveHTTP(ctx, uri)
		if err != nil {
			return "", err
		}
		logger.Info("package pull completed", "package", safeURI, "scheme", scheme, "path", resolved, "format", packagePathFormat(resolved))
		return resolved, nil
	case "nacos":
		logger.Info("package pull started", "package", safeURI, "scheme", scheme)
		resolved, err := p.resolveNacos(ctx, parsed)
		if err != nil {
			return "", err
		}
		logger.Info("package pull completed", "package", safeURI, "scheme", scheme, "path", resolved, "format", packagePathFormat(resolved))
		return resolved, nil
	case "oss":
		logger.Info("package pull started", "package", safeURI, "scheme", scheme)
		resolved, err := p.resolveOSS(ctx, parsed)
		if err != nil {
			return "", err
		}
		logger.Info("package pull completed", "package", safeURI, "scheme", scheme, "path", resolved, "format", packagePathFormat(resolved))
		return resolved, nil
	default:
		// Treat as relative MinIO path (e.g. "packages/alice.zip")
		// Use content-addressable cache: download to /tmp/import/{md5}.zip
		// If the same content already exists locally, skip re-download.
		storagePrefix := os.Getenv("AGENTTEAMS_STORAGE_PREFIX")
		if storagePrefix == "" {
			storagePrefix = "agentteams/agentteams-storage"
		}
		minioPath := fmt.Sprintf("%s/agentteams-config/%s", storagePrefix, uri)

		// Get remote file's ETag (MD5) via mc stat
		etag := getMinIOETag(ctx, minioPath)
		if etag == "" {
			// Fallback: use URI hash if mc stat fails
			h := sha256.Sum256([]byte(uri))
			etag = fmt.Sprintf("%x", h[:8])
		}

		destPath := filepath.Join(p.ImportDir, etag+".zip")
		if _, err := os.Stat(destPath); err == nil {
			logger.Info("package pull cache hit", "package", safeURI, "scheme", scheme, "minioPath", minioPath, "path", destPath, "etag", etag, "format", "zip")
			return destPath, nil // cache hit, same content
		}

		logger.Info("package pull started", "package", safeURI, "scheme", scheme, "minioPath", minioPath, "path", destPath, "etag", etag)
		cmd := exec.CommandContext(ctx, "mc", "cp", minioPath, destPath)
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("failed to download %s from MinIO: %s: %w", minioPath, string(out), err)
		}
		logger.Info("package pull completed", "package", safeURI, "scheme", scheme, "minioPath", minioPath, "path", destPath, "etag", etag, "format", "zip")
		return destPath, nil
	}
}

// ResolveAndExtract downloads/locates a package, extracts it, and returns the
// extracted directory path. Typical layout (SOUL.md is optional; DeployWorkerConfig
// supplies a default SOUL when missing):
//
//	{extractDir}/{name}/
//	├── config/
//	│   ├── SOUL.md (optional)
//	│   └── AGENTS.md (optional)
//	├── skills/ (optional)
//	└── Dockerfile (optional)
func (p *PackageResolver) ResolveAndExtract(ctx context.Context, uri, name string) (string, error) {
	// 逻辑说明：先解析来源；Nacos 已物化为目录时直接返回，zip 则清理该 Agent 的旧提取目录、重建后调用 unzip 覆盖解压，失败保留命令诊断。
	if uri == "" {
		return "", nil
	}
	logger := log.FromContext(ctx)

	resolved, err := p.Resolve(ctx, uri)
	if err != nil {
		return "", fmt.Errorf("resolve package: %w", err)
	}

	// If Resolve already returned a directory (e.g. nacos://), use it directly.
	if info, err := os.Stat(resolved); err == nil && info.IsDir() {
		return resolved, nil
	}

	// Otherwise treat as ZIP and extract.
	destDir := filepath.Join(p.ExtractDir, name)
	os.RemoveAll(destDir)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", fmt.Errorf("create extract dir: %w", err)
	}

	logger.Info("package extract started", "name", name, "package", safePackageURI(uri), "archive", resolved, "targetDir", destDir, "format", "zip")
	cmd := exec.CommandContext(ctx, "unzip", "-q", "-o", resolved, "-d", destDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("extract ZIP %s: %s: %w", resolved, string(out), err)
	}

	return destDir, nil
}

// DeployToMinIO seeds extracted package contents to the worker's agent space.
// Package files are initialization defaults: a later reconcile must not overwrite
// runtime-mutated files for the same worker. Controller-owned generated files are
// still overwritten by DeployWorkerConfig after this package layer is applied.
//
// To avoid a race with the background MinIO→local sync (which could overwrite local
// files between the local write and the mc mirror push), we push to MinIO FIRST from
// the extracted directory (immune to background sync), then copy to the local agent dir.
func (p *PackageResolver) DeployToMinIO(ctx context.Context, extractedDir, workerName string, excludeMemory bool, storage oss.StorageClient) error {
	// 逻辑说明：扫描包内 config/skills/crons，按“对象存储先、本地 Agent 目录后”且仅目标不存在时播种，避免同步竞态与覆盖运行期修改；更新模式保留 Controller 管理文件，关键 I/O 失败中止。
	logger := log.FromContext(ctx)
	agentDir := fmt.Sprintf("/root/agentteams-fs/agents/%s", workerName)
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		return fmt.Errorf("create agent dir: %w", err)
	}

	storagePrefix := os.Getenv("AGENTTEAMS_STORAGE_PREFIX")
	if storagePrefix == "" {
		storagePrefix = "agentteams/agentteams-storage"
	}
	minioBase := fmt.Sprintf("%s/agents/%s", storagePrefix, workerName)
	agentPrefix := fmt.Sprintf("agents/%s", workerName)
	if err := seedPackageDirectoryObject(ctx, storage, agentPrefix); err != nil {
		return err
	}

	// Collect transformed config files and subdirectory names from the package.
	type fileEntry struct {
		name string
		data []byte
	}
	var configFiles []fileEntry
	var configSubdirs []string

	configDir := filepath.Join(extractedDir, "config")
	if info, err := os.Stat(configDir); err == nil && info.IsDir() {
		logger.Info("package config directory detected", "worker", workerName, "path", configDir)
		entries, _ := os.ReadDir(configDir)
		for _, e := range entries {
			if e.IsDir() {
				configSubdirs = append(configSubdirs, e.Name())
				continue
			}
			if excludeMemory && e.Name() == "MEMORY.md" {
				continue
			}
			src := filepath.Join(configDir, e.Name())
			data, err := os.ReadFile(src)
			if err != nil {
				logger.Error(err, "package config file read failed; skipping", "worker", workerName, "path", src, "file", e.Name())
				continue
			}
			if e.Name() == "AGENTS.md" {
				fileSize := int64(len(data))
				if info, err := e.Info(); err == nil {
					fileSize = info.Size()
				}
				logger.Info("package AGENTS.md detected", "worker", workerName, "path", src, "fileSizeBytes", fileSize, "loadedBytes", len(data), "hasBuiltinMarkers", strings.Contains(string(data), "<!-- agentteams-builtin-start -->"), "willWrapBuiltinMarkers", !strings.Contains(string(data), "<!-- agentteams-builtin-start -->"))
				data = wrapWithBuiltinMarkers(data)
				logger.Info("package AGENTS.md prepared for storage", "worker", workerName, "path", src, "bytes", len(data), "hasBuiltinMarkers", strings.Contains(string(data), "<!-- agentteams-builtin-start -->"))
			}
			configFiles = append(configFiles, fileEntry{name: e.Name(), data: data})
		}
	} else {
		logger.Info("package config directory not found; checking root SOUL.md fallback", "worker", workerName, "path", configDir)
		// Fallback: SOUL.md at root level
		src := filepath.Join(extractedDir, "SOUL.md")
		if data, err := os.ReadFile(src); err == nil {
			configFiles = append(configFiles, fileEntry{name: "SOUL.md", data: data})
		}
	}

	// Collect transformed crons data (if present).
	skillsDir := filepath.Join(extractedDir, "skills")
	cronsDir := filepath.Join(extractedDir, "crons")
	var cronData []byte
	if info, err := os.Stat(cronsDir); err == nil && info.IsDir() {
		if raw, err := os.ReadFile(filepath.Join(cronsDir, "jobs.json")); err == nil {
			trimmed := strings.TrimSpace(string(raw))
			if strings.HasPrefix(trimmed, "[") {
				cronData = []byte(fmt.Sprintf(`{"version":1,"jobs":%s}`, trimmed))
			} else {
				cronData = raw
			}
		}
	}

	configFileNames := make([]string, 0, len(configFiles))
	for _, f := range configFiles {
		configFileNames = append(configFileNames, f.name)
	}
	sort.Strings(configFileNames)
	sort.Strings(configSubdirs)
	skillNames := topLevelDirNames(skillsDir)
	packageFiles, packageFileCount, packageFileListTruncated := listPackageFiles(extractedDir, 200)
	hasCrons := cronData != nil
	logger.Info("package layout detected", "worker", workerName, "extractedDir", extractedDir, "format", packagePathFormat(extractedDir), "configFiles", configFileNames, "configSubdirs", configSubdirs, "skills", skillNames, "hasCrons", hasCrons, "packageFileCount", packageFileCount, "packageFiles", packageFiles, "packageFileListTruncated", packageFileListTruncated, "isUpdate", excludeMemory)

	// ── Phase 1: Push to MinIO FIRST from extracted dir (immune to background sync) ──

	// Config files — when excludeMemory is true (update path), skip SOUL.md and AGENTS.md
	// because DeployWorkerConfig handles them with proper inline override priority.
	var pushedConfigFiles []string
	var skippedConfigFiles []string
	for _, f := range configFiles {
		if excludeMemory && (f.name == "SOUL.md" || f.name == "AGENTS.md") {
			skippedConfigFiles = append(skippedConfigFiles, f.name)
			continue
		}
		useStorageClient := storage != nil
		target := agentPrefix + "/" + f.name
		if !useStorageClient {
			target = minioBase + "/" + f.name
		}
		seeded, err := putPackageObjectSeedOnly(ctx, storage, useStorageClient, target, f.data)
		if err != nil {
			return fmt.Errorf("push %s to MinIO: %w", f.name, err)
		}
		if f.name == "AGENTS.md" && seeded {
			logger.Info("package AGENTS.md pushed to storage", "worker", workerName, "target", target, "bytes", len(f.data), "storageClient", useStorageClient)
		}
		if seeded {
			pushedConfigFiles = append(pushedConfigFiles, f.name)
		}
	}
	if len(skippedConfigFiles) > 0 {
		sort.Strings(skippedConfigFiles)
		logger.Info("package config files skipped during package deploy", "worker", workerName, "files", skippedConfigFiles, "reason", "update path keeps SOUL.md and AGENTS.md under DeployWorkerConfig control")
	}
	if len(pushedConfigFiles) > 0 {
		sort.Strings(pushedConfigFiles)
		logger.Info("package config files pushed to storage", "worker", workerName, "files", pushedConfigFiles, "target", minioBase, "controllerManagedTarget", agentPrefix, "controllerManagedStorageClient", storage != nil)
	}
	if len(configSubdirs) > 0 && storage != nil {
		var seededSubdirFiles []string
		for _, dirName := range configSubdirs {
			srcDir := filepath.Join(configDir, dirName)
			if err := filepath.WalkDir(srcDir, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() {
					return nil
				}
				if entry.Type()&os.ModeSymlink != 0 {
					return nil
				}
				rel, err := filepath.Rel(configDir, path)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				target := agentPrefix + "/" + rel
				seeded, err := putPackageFileSeedOnly(ctx, storage, path, target)
				if err != nil {
					return err
				}
				if seeded {
					seededSubdirFiles = append(seededSubdirFiles, rel)
				}
				return nil
			}); err != nil {
				return fmt.Errorf("seed config directory %s to storage: %w", dirName, err)
			}
		}
		if len(seededSubdirFiles) > 0 {
			sort.Strings(seededSubdirFiles)
			logger.Info("package config subdir files seeded to storage", "worker", workerName, "files", seededSubdirFiles, "target", agentPrefix)
		}
	}
	if len(configSubdirs) > 0 && storage != nil {
		for _, dirName := range configSubdirs {
			srcDir := filepath.Join(configDir, dirName)
			if err := filepath.WalkDir(srcDir, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
					return nil
				}
				rel, err := filepath.Rel(configDir, path)
				if err != nil {
					return err
				}
				target := agentPrefix + "/" + filepath.ToSlash(rel)
				_, err = putPackageFileSeedOnly(ctx, storage, path, target)
				return err
			}); err != nil {
				return fmt.Errorf("seed config directory %s to storage: %w", dirName, err)
			}
		}
	}

	// Skills
	if info, err := os.Stat(skillsDir); err == nil && info.IsDir() {
		target := minioBase + "/skills/"
		if storage != nil {
			target = agentPrefix + "/skills"
			if err := seedDirToStorage(ctx, storage, skillsDir, target); err != nil {
				return fmt.Errorf("seed skills to storage: %w", err)
			}
		} else {
			mcCmd := exec.CommandContext(ctx, "mc", "mirror", skillsDir+"/", minioBase+"/skills/", "--overwrite")
			if out, err := mcCmd.CombinedOutput(); err != nil {
				return fmt.Errorf("mc mirror skills to MinIO: %s: %w", string(out), err)
			}
		}
		logger.Info("package skills pushed to storage", "worker", workerName, "skills", skillNames, "target", target)
	}

	// Crons
	if cronData != nil {
		target := minioBase + "/.openclaw/cron/jobs.json"
		if storage != nil {
			target = agentPrefix + "/.openclaw/cron/jobs.json"
			if _, err := putPackageObjectSeedOnly(ctx, storage, true, target, cronData); err != nil {
				return fmt.Errorf("seed crons to storage: %w", err)
			}
		} else {
			if err := mcPut(ctx, target, cronData); err != nil {
				return fmt.Errorf("push crons to MinIO: %w", err)
			}
		}
		logger.Info("package crons pushed to storage", "worker", workerName, "target", target)
	}

	// ── Phase 2: Seed local package files without overwriting runtime changes ──

	var localConfigFiles []string
	for _, f := range configFiles {
		if excludeMemory && (f.name == "SOUL.md" || f.name == "AGENTS.md") {
			continue
		}
		if _, err := writeFileSeedOnly(filepath.Join(agentDir, f.name), f.data); err != nil {
			return fmt.Errorf("seed local %s: %w", f.name, err)
		}
		localConfigFiles = append(localConfigFiles, f.name)
	}
	if len(localConfigFiles) > 0 {
		sort.Strings(localConfigFiles)
		logger.Info("package config files copied to local agent dir", "worker", workerName, "files", localConfigFiles, "target", agentDir)
	}
	for _, dirName := range configSubdirs {
		src := filepath.Join(configDir, dirName)
		dst := filepath.Join(agentDir, dirName)
		if err := copyDirSeedOnly(src, dst); err != nil {
			return fmt.Errorf("seed local config directory %s: %w", dirName, err)
		}
	}

	// Skills
	if info, err := os.Stat(skillsDir); err == nil && info.IsDir() {
		destSkills := filepath.Join(agentDir, "skills")
		if err := copyDirSeedOnly(skillsDir, destSkills); err != nil {
			return fmt.Errorf("seed local skills: %w", err)
		}
	}

	// Crons
	if cronData != nil {
		destCron := filepath.Join(agentDir, ".openclaw", "cron")
		if _, err := writeFileSeedOnly(filepath.Join(destCron, "jobs.json"), cronData); err != nil {
			return fmt.Errorf("seed local crons: %w", err)
		}
	}

	logger.Info("package files deployed", "worker", workerName, "storageTarget", minioBase, "controllerManagedTarget", agentPrefix, "localTarget", agentDir, "controllerManagedStorageClient", storage != nil)
	return nil
}

// mcPut writes data to a MinIO path via temp file + mc cp.
func mcPut(ctx context.Context, minioPath string, data []byte) error {
	// 逻辑说明：把内存数据先写临时文件，再通过受 context 控制的 `mc cp` 上传目标并清理临时文件；创建、写入或命令失败带阶段返回，不记录数据正文。
	tmpFile, err := os.CreateTemp("", "agentteams-deploy-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	tmpFile.Close()

	cmd := exec.CommandContext(ctx, "mc", "cp", tmpName, minioPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mc cp to %s: %s: %w", minioPath, string(out), err)
	}
	return nil
}

func putPackageObjectSeedOnly(ctx context.Context, storage oss.StorageClient, useStorageClient bool, target string, data []byte) (bool, error) {
	// 逻辑说明：选择 StorageClient 或 mc 实现；StorageClient 路径先检查目标，存在返回 false 保留用户修改，仅明确缺失才写入并返回 true。
	if !useStorageClient {
		return true, mcPut(ctx, target, data)
	}
	if err := storage.Stat(ctx, target); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, storage.PutObject(ctx, target, data)
}

func putPackageFileSeedOnly(ctx context.Context, storage oss.StorageClient, localPath, target string) (bool, error) {
	// 逻辑说明：对目标对象执行存在性门禁，存在不覆盖，明确 NotExist 才上传本地文件；返回 bool 表示本次是否实际播种。
	if err := storage.Stat(ctx, target); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, storage.PutFile(ctx, localPath, target)
}

func seedPackageDirectoryObject(ctx context.Context, storage oss.StorageClient, agentPrefix string) error {
	// 逻辑说明：StorageClient 可用时为 Agent 前缀播种隐藏占位对象以建立逻辑目录；目标已存在不会覆盖，nil client 直接跳过兼容 mc 路径。
	if storage == nil {
		return nil
	}
	prefix := strings.TrimSuffix(agentPrefix, "/") + "/"
	if _, err := putPackageObjectSeedOnly(ctx, storage, true, prefix+".agentteams-keep", []byte("")); err != nil {
		return fmt.Errorf("seed worker directory marker: %w", err)
	}
	return nil
}

func seedDirToStorage(ctx context.Context, storage oss.StorageClient, srcDir, dstPrefix string) error {
	// 逻辑说明：递归遍历源码目录、跳过目录与符号链接，把普通文件相对路径映射到对象前缀并以 seed-only 上传；遍历/路径/上传错误立即停止。
	return filepath.WalkDir(srcDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		target := strings.TrimSuffix(dstPrefix, "/") + "/" + filepath.ToSlash(rel)
		_, err = putPackageFileSeedOnly(ctx, storage, path, target)
		return err
	})
}

func writeFileSeedOnly(path string, data []byte) (bool, error) {
	// 逻辑说明：本地目标存在时不覆盖，只有确认 NotExist 才创建父目录并以 0644 写入；返回是否实际播种，其他 stat 错误保留。
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, err
	}
	return true, os.WriteFile(path, data, 0644)
}

func copyDirSeedOnly(srcDir, dstDir string) error {
	// 逻辑说明：递归重建目录结构、跳过符号链接，并用 seed-only 写普通文件；已有目的文件保持不变，任一遍历或读取失败终止。
	return filepath.WalkDir(srcDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(dstDir, rel)
		if entry.IsDir() {
			return os.MkdirAll(dst, 0755)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = writeFileSeedOnly(dst, data)
		return err
	})
}

// wrapWithBuiltinMarkers wraps user AGENTS.md content with agentteams-builtin markers.
// Uses the same format as builtin-merge.sh (BUILTIN_HEADER + BUILTIN_END) so that
// upgrade-builtins.sh can fill the builtin section without destroying user content.
// If markers already exist, the content is returned as-is.
// YAML frontmatter (---...---) is preserved before the markers.
func wrapWithBuiltinMarkers(data []byte) []byte {
	// 逻辑说明：已有内置边界时原样返回；否则识别并保留 YAML frontmatter，在用户正文前插入与升级脚本一致的受管区域标记，返回新字节切片。
	content := string(data)
	if strings.Contains(content, "<!-- agentteams-builtin-start -->") {
		return data // already has markers
	}

	var frontmatter, body string
	// Extract YAML frontmatter if present
	if strings.HasPrefix(content, "---\n") {
		if end := strings.Index(content[4:], "\n---\n"); end >= 0 {
			fmEnd := 4 + end + 4 // past the closing "---\n"
			frontmatter = content[:fmEnd]
			body = content[fmEnd:]
		} else {
			body = content
		}
	} else {
		body = content
	}

	// Match the exact format from builtin-merge.sh BUILTIN_HEADER + BUILTIN_END
	wrapped := ""
	if frontmatter != "" {
		wrapped += frontmatter + "\n"
	}
	wrapped += "<!-- agentteams-builtin-start -->\n" +
		"> ⚠️ **DO NOT EDIT** this section. It is managed by AgentTeams and will be automatically\n" +
		"> replaced on upgrade. To customize, add your content **after** the\n" +
		"> `<!-- agentteams-builtin-end -->` marker below.\n" +
		"\n" +
		"<!-- agentteams-builtin-end -->\n\n" +
		body
	return []byte(wrapped)
}

// WriteInlineConfigs writes inline identity/soul/agents content to the agent directory.
// For copaw and hermes runtimes, identity is merged into SOUL.md since neither
// supports a separate IDENTITY.md file.
// This function is called AFTER DeployToMinIO so inline fields override package files.
func WriteInlineConfigs(agentDir, runtime, identity, soul, agents string) error {
	// 逻辑说明：确保 Agent 目录后按 runtime 写身份文件：CoPaw/Hermes 把 identity 合并进 SOUL，其他 runtime 分开写；AGENTS 加受管标记，inline 最后写入以覆盖包默认。
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		return fmt.Errorf("create agent dir %s: %w", agentDir, err)
	}

	mergeIdentityIntoSoul := strings.EqualFold(runtime, "copaw") ||
		strings.EqualFold(runtime, "hermes")

	if mergeIdentityIntoSoul {
		// CoPaw / Hermes: merge identity into soul (prepend)
		merged := ""
		if identity != "" {
			merged += strings.TrimSpace(identity) + "\n\n"
		}
		if soul != "" {
			merged += strings.TrimSpace(soul)
		}
		if merged != "" {
			if err := os.WriteFile(filepath.Join(agentDir, "SOUL.md"), []byte(merged+"\n"), 0644); err != nil {
				return fmt.Errorf("write SOUL.md: %w", err)
			}
		}
	} else {
		// OpenClaw: write IDENTITY.md and SOUL.md separately
		if identity != "" {
			if err := os.WriteFile(filepath.Join(agentDir, "IDENTITY.md"), []byte(strings.TrimSpace(identity)+"\n"), 0644); err != nil {
				return fmt.Errorf("write IDENTITY.md: %w", err)
			}
		}
		if soul != "" {
			if err := os.WriteFile(filepath.Join(agentDir, "SOUL.md"), []byte(strings.TrimSpace(soul)+"\n"), 0644); err != nil {
				return fmt.Errorf("write SOUL.md: %w", err)
			}
		}
	}

	if agents != "" {
		wrapped := wrapWithBuiltinMarkers([]byte(strings.TrimSpace(agents)))
		if err := os.WriteFile(filepath.Join(agentDir, "AGENTS.md"), wrapped, 0644); err != nil {
			return fmt.Errorf("write AGENTS.md: %w", err)
		}
	}

	return nil
}

// getMinIOETag returns the ETag (content MD5) of a MinIO object via mc stat.
// Returns empty string if mc stat fails.
func getMinIOETag(ctx context.Context, minioPath string) string {
	// 逻辑说明：调用 `mc stat --json` 并从成功输出提取、清理 ETag 作为内容缓存键；命令失败或格式缺失返回空串，让上层改用 URI hash。
	cmd := exec.CommandContext(ctx, "mc", "stat", "--json", minioPath)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	// mc stat --json outputs {"etag":"xxx",...}
	// Simple extraction without json dependency
	s := string(out)
	if idx := strings.Index(s, `"etag":"`); idx >= 0 {
		rest := s[idx+8:]
		if end := strings.Index(rest, `"`); end >= 0 {
			etag := rest[:end]
			// Remove quotes and dashes from ETag
			etag = strings.ReplaceAll(etag, "-", "")
			if etag != "" {
				return etag
			}
		}
	}
	return ""
}

func safePackageURI(raw string) string {
	// 逻辑说明：解析 URL 后删除 userinfo 中的密码、最多保留用户名再返回日志安全 URI；无法解析或无 userinfo 时原样返回。
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	if username := u.User.Username(); username != "" {
		u.User = url.User(username)
	} else {
		u.User = nil
	}
	return u.String()
}

func packagePathFormat(path string) string {
	// 逻辑说明：通过 stat 与扩展名把落地路径分类为 directory/zip/file；不可访问返回 unknown，只用于结构化日志。
	info, err := os.Stat(path)
	if err != nil {
		return "unknown"
	}
	if info.IsDir() {
		return "directory"
	}
	if strings.EqualFold(filepath.Ext(path), ".zip") {
		return "zip"
	}
	return "file"
}

func topLevelDirNames(dir string) []string {
	// 逻辑说明：读取目录并只收集一级子目录名，排序后返回用于包布局日志；读取失败返回 nil，不影响实际部署。
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func listPackageFiles(root string, limit int) ([]string, int, bool) {
	// 逻辑说明：容错遍历包文件，统计总数并只保存前 limit 个相对路径，排序后同时返回是否截断；单个遍历错误忽略，仅用于有界诊断。
	var files []string
	count := 0
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		count++
		if len(files) < limit {
			if rel, err := filepath.Rel(root, path); err == nil {
				files = append(files, rel)
			}
		}
		return nil
	})
	sort.Strings(files)
	return files, count, count > len(files)
}

// --- Private resolve methods ---

// resolveOSS downloads a package from MinIO/OSS storage.
// URI format: oss://agentteams-config/packages/{name}-{md5}.zip
// The filename contains the content hash, so it's naturally content-addressable:
// same hash → same content → cache hit.
func (p *PackageResolver) resolveOSS(ctx context.Context, u *url.URL) (string, error) {
	// 逻辑说明：从 oss URI 生成对象路径和安全本地文件名；命中内容地址缓存直接返回，否则用 mc 下载到 ImportDir，失败附命令输出。
	// oss://agentteams-config/packages/alice-abc123.zip → agentteams-config/packages/alice-abc123.zip
	ossPath := strings.TrimPrefix(u.Host+u.Path, "/")
	filename := filepath.Base(ossPath)

	// Content-addressable cache: filename includes MD5, so same file = cache hit
	destPath := filepath.Join(p.ImportDir, filename)
	if _, err := os.Stat(destPath); err == nil {
		return destPath, nil // cache hit
	}

	// Download from MinIO
	storagePrefix := os.Getenv("AGENTTEAMS_STORAGE_PREFIX")
	if storagePrefix == "" {
		storagePrefix = "agentteams/agentteams-storage"
	}
	minioPath := fmt.Sprintf("%s/%s", storagePrefix, ossPath)

	cmd := exec.CommandContext(ctx, "mc", "cp", minioPath, destPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("failed to download oss://%s from MinIO: %s: %w", ossPath, string(out), err)
	}

	return destPath, nil
}

func (p *PackageResolver) resolveFile(u *url.URL) (string, error) {
	// 逻辑说明：优先查找 ImportDir 中同 basename 的导入文件，缺失再接受 URI 指向的现有宿主文件；两处都不存在返回列明候选路径的错误。
	filename := filepath.Base(u.Path)
	localPath := filepath.Join(p.ImportDir, filename)

	if _, err := os.Stat(localPath); err != nil {
		if _, err2 := os.Stat(u.Path); err2 != nil {
			return "", fmt.Errorf("file package not found at %s or %s", localPath, u.Path)
		}
		return u.Path, nil
	}
	return localPath, nil
}

func (p *PackageResolver) resolveHTTP(ctx context.Context, uri string) (string, error) {
	// 逻辑说明：为 URL 派生本地 zip 名并复用已有缓存；未命中时执行受 context 控制的 GET，只接受 200，流式写入失败会删除残缺文件。
	filename := filepath.Base(uri)
	if !strings.HasSuffix(filename, ".zip") {
		filename += ".zip"
	}
	destPath := filepath.Join(p.ImportDir, filename)

	if _, err := os.Stat(destPath); err == nil {
		return destPath, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request for %s: %w", uri, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download %s: %w", uri, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s returned status %d", uri, resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("failed to create %s: %w", destPath, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		os.Remove(destPath)
		return "", fmt.Errorf("failed to write %s: %w", destPath, err)
	}

	return destPath, nil
}

// resolveNacos downloads a Worker template from Nacos via the AgentSpec client API.
// URI format: nacos://[user:pass@]host:port/{namespace}/{agentspec-name}[/{version}][?authType=...]
// Optional query: authType=nacos|sts-agentteams|none (empty = auto from userinfo, same as NewNacosAIClient).
// The Nacos server address (and optional credentials) are extracted from the URI authority.
func (p *PackageResolver) resolveNacos(ctx context.Context, u *url.URL) (string, error) {
	// 逻辑说明：解析 namespace/spec/version 或 label，清理旧目标后按 URI 认证构造 Nacos client 并物化 AgentSpec；可选 SHA-256 常量时间比较，最后验证目标确为目录。
	// Parse URI path segments: /{namespace}/{agentspec-name}/{version}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid nacos URI: expected nacos://[user:pass@]host:port/{namespace}/{agentspec-name}[/{version}], got %s", u.String())
	}

	// Build the Nacos server address from the URI authority (host, port, userinfo).
	nacosAddr := u.Host
	if u.User != nil {
		nacosAddr = u.User.String() + "@" + u.Host
	}

	namespace := parts[0]
	specName := parts[1]
	version := ""
	if len(parts) >= 3 {
		version = parts[2]
	}

	outputDir := filepath.Join(p.ImportDir, "nacos")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create nacos import dir %s: %w", outputDir, err)
	}
	destPath := filepath.Join(outputDir, specName)
	if err := os.RemoveAll(destPath); err != nil {
		return "", fmt.Errorf("failed to clean previous nacos package %s: %w", destPath, err)
	}

	authType := strings.TrimSpace(u.Query().Get("authType"))
	client, err := NewNacosAIClient(ctx, nacosAddr, namespace, authType, p.CredClient)
	if err != nil {
		return "", err
	}

	label := ""
	if strings.HasPrefix(version, "label:") {
		label = strings.TrimPrefix(version, "label:")
		version = ""
	}

	expectedDigest := strings.TrimSpace(u.Query().Get("expectedDigest"))
	if expectedDigest != "" {
		if err := validateNacosDigest(expectedDigest); err != nil {
			return "", err
		}
	}
	actualDigest, err := client.GetAgentSpecWithDigest(ctx, specName, outputDir, version, label)
	if err != nil {
		return "", fmt.Errorf("fetch agentspec %s from nacos failed: %w", specName, err)
	}
	if expectedDigest != "" && subtle.ConstantTimeCompare(
		[]byte(expectedDigest),
		[]byte(actualDigest),
	) != 1 {
		return "", fmt.Errorf(
			"agentspec digest mismatch: expected %s, got %s",
			expectedDigest,
			actualDigest,
		)
	}

	info, err := os.Stat(destPath)
	if err != nil {
		return "", fmt.Errorf("agentspec download finished but %s was not created: %w", destPath, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("agentspec output %s is not a directory", destPath)
	}

	return destPath, nil
}

func validateNacosDigest(value string) error {
	// 逻辑说明：严格要求 `sha256:` 加 64 位小写十六进制；长度、解码或大小写不符均拒绝，避免比较多种非规范摘要表示。
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("invalid expectedDigest: must use sha256:<hex>")
	}
	raw := strings.TrimPrefix(value, prefix)
	if len(raw) != sha256.Size*2 {
		return fmt.Errorf("invalid expectedDigest: must use sha256:<hex>")
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return fmt.Errorf("invalid expectedDigest: must use lowercase sha256 hex")
	}
	if raw != strings.ToLower(raw) {
		return fmt.Errorf("invalid expectedDigest: must use lowercase sha256 hex")
	}
	return nil
}

// ValidateNacosURIOptions configures Nacos preflight so it matches runtime
// PackageResolver behavior. The auth type is read from the nacos:// URI query:
//
//	?authType=nacos|sts-agentteams|none
//
// or omitted for the same auto-detection as NewNacosAIClient.
type ValidateNacosURIOptions struct {
	// CredClient is required when the URI includes authType=sts-agentteams; it
	// should be the same credprovider.Client wired into PackageResolver
	// (HTTP client to agentteams-credential-provider /issue).
	CredClient credprovider.Client
}

// ValidateNacosURI checks that a nacos:// URI is well-formed, the server is
// reachable, and any embedded credentials are accepted.  It is intended as a
// preflight check before persisting a Worker resource.
func ValidateNacosURI(ctx context.Context, raw string, opts ValidateNacosURIOptions) error {
	// 逻辑说明：验证 nacos scheme/host/路径/可选 digest，按运行时同样规则构造 client 完成 TCP 与认证预检，再确认目标 AgentSpec/version/label 在线。
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid nacos URI %q: %w", raw, err)
	}
	if u.Scheme != "nacos" {
		return fmt.Errorf("invalid nacos URI %q: scheme must be nacos://", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid nacos URI %q: missing host", raw)
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return fmt.Errorf("invalid nacos URI %q: expected nacos://[user:pass@]host:port/{namespace}/{agentspec-name}[/{version}]", raw)
	}
	if expectedDigest := strings.TrimSpace(u.Query().Get("expectedDigest")); expectedDigest != "" {
		if err := validateNacosDigest(expectedDigest); err != nil {
			return err
		}
	}

	nacosAddr := u.Host
	if u.User != nil {
		nacosAddr = u.User.String() + "@" + u.Host
	}
	namespace := parts[0]
	specName := parts[1]
	version := ""
	if len(parts) >= 3 {
		version = parts[2]
	}

	label := ""
	if strings.HasPrefix(version, "label:") {
		label = strings.TrimPrefix(version, "label:")
		version = ""
	}

	authType := strings.TrimSpace(u.Query().Get("authType"))

	// newNacosAIClient validates the address format, connects, and
	// performs login (or STS) when credentials are present — same as resolveNacos.
	client, err := NewNacosAIClient(ctx, nacosAddr, namespace, authType, opts.CredClient)
	if err != nil {
		return fmt.Errorf("nacos preflight check failed for %q: %w", raw, err)
	}
	if err := client.CheckAgentSpecExists(ctx, specName, version, label); err != nil {
		return fmt.Errorf("nacos preflight check failed for %q: %w", raw, err)
	}
	return nil
}
