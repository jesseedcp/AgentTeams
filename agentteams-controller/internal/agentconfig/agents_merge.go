package agentconfig

import (
	"strings"
)

// MergeBuiltinSection merges a builtin source into a target markdown document.
// It preserves user content after the builtin-end marker.
//
// Behavior:
//   - If target is empty: returns marker-wrapped source
//   - If target has markers: replaces builtin section, preserves user content
//   - If target has no markers: overwrites with marker-wrapped source
func MergeBuiltinSection(target, source string) string {
	// 逻辑说明：用最新内置段替换 marker 区域，同时保留 marker 后用户内容；旧无 marker 文件整体作为用户内容迁移。
	if target == "" {
		return wrapWithMarkers(source, "")
	}

	if strings.Contains(target, BuiltinStart) {
		userContent := extractUserContent(target)
		return wrapWithMarkers(source, userContent)
	}

	// Legacy file without markers — wrap source with markers, preserve target as user content
	return wrapWithMarkers(source, target)
}

// ExtractFrontmatter separates YAML frontmatter from the body.
// Returns (frontmatter, body). If no frontmatter, frontmatter is empty.
func ExtractFrontmatter(content string) (string, string) {
	// 逻辑说明：只把首行开始且有闭合分隔符的 YAML 识别为 frontmatter；不完整内容原样作为正文返回。
	lines := strings.Split(content, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return "", content
	}

	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			fm := strings.Join(lines[:i+1], "\n")
			body := ""
			if i+1 < len(lines) {
				body = strings.Join(lines[i+1:], "\n")
			}
			return fm, strings.TrimLeft(body, "\n")
		}
	}

	return "", content
}

func wrapWithMarkers(source, userContent string) string {
	// 逻辑说明：去掉 source frontmatter 后写入唯一内置 marker 区，再按原顺序附加并规范化用户内容换行。
	_, body := ExtractFrontmatter(source)

	var b strings.Builder
	b.WriteString(BuiltinHeader)
	b.WriteString("\n")
	b.WriteString(strings.TrimRight(body, "\n"))
	b.WriteString("\n\n")
	b.WriteString(BuiltinEnd)
	b.WriteString("\n")

	if userContent != "" {
		b.WriteString("\n")
		b.WriteString(strings.TrimRight(userContent, "\n"))
		b.WriteString("\n")
	}

	return b.String()
}

func extractUserContent(target string) string {
	// 逻辑说明：取最后一个结束 marker 之后的内容，避开 header 示例中的 marker 文本；仅空白则视为无用户段。
	// Use LastIndex because BuiltinHeader references the end marker in backticks
	idx := strings.LastIndex(target, BuiltinEnd)
	if idx < 0 {
		return ""
	}
	after := target[idx+len(BuiltinEnd):]
	after = strings.TrimLeft(after, "\n")
	if strings.TrimSpace(after) == "" {
		return ""
	}
	return after
}

// MergeSoulTemplate merges a rendered SOUL.md template into existing SOUL.md content.
// Template content is wrapped in soul-template markers; content outside markers is preserved.
//
// Behavior:
//   - If target is empty: returns marker-wrapped template
//   - If target has markers: replaces template section, preserves package/user content
//   - If target has no markers: prepends marker-wrapped template, keeps existing as user content
func MergeSoulTemplate(target, rendered string) string {
	// 逻辑说明：替换已管理的 SOUL 模板段并保留外部内容；旧文件无 marker 时把旧内容接到新模板之后。
	if target == "" {
		return wrapSoulTemplate(rendered, "")
	}

	if strings.Contains(target, SoulTemplateStart) {
		userContent := extractSoulUserContent(target)
		return wrapSoulTemplate(rendered, userContent)
	}

	return wrapSoulTemplate(rendered, target)
}

func wrapSoulTemplate(rendered, userContent string) string {
	// 逻辑说明：以固定 marker 包裹渲染身份模板并规范化尾换行，再附加不可覆盖的用户/包内容。
	var b strings.Builder
	b.WriteString(SoulTemplateHeader)
	b.WriteString("\n")
	b.WriteString(strings.TrimRight(rendered, "\n"))
	b.WriteString("\n\n")
	b.WriteString(SoulTemplateEnd)
	b.WriteString("\n")

	if userContent != "" {
		b.WriteString("\n")
		b.WriteString(strings.TrimRight(userContent, "\n"))
		b.WriteString("\n")
	}

	return b.String()
}

func extractSoulUserContent(target string) string {
	// 逻辑说明：只抽取最后一个 SOUL 结束 marker 后的非空内容，使下次模板更新保持幂等。
	idx := strings.LastIndex(target, SoulTemplateEnd)
	if idx < 0 {
		return ""
	}
	after := target[idx+len(SoulTemplateEnd):]
	after = strings.TrimLeft(after, "\n")
	if strings.TrimSpace(after) == "" {
		return ""
	}
	return after
}
