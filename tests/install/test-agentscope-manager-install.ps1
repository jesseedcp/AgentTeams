# Windows 安装器的静态契约测试：读取 PowerShell 安装器、verify 脚本和 Makefile，确认默认 Manager 始终是 AgentScope。
# 这里不运行 Docker 或联网安装；Assert-Contains/NotContains 用于发现镜像名、参数和退役分支在发布前发生漂移。
# 通过标准是安装入口、验证命令和构建目标三者一致，避免“脚本能解析但安装了错误 runtime”。
$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$installerPath = Join-Path $root "install\agentteams-install.ps1"
$verifyPath = Join-Path $root "install\agentteams-verify.sh"
$makefilePath = Join-Path $root "Makefile"
$installer = Get-Content -Raw -LiteralPath $installerPath
$verify = Get-Content -Raw -LiteralPath $verifyPath
$makefile = Get-Content -Raw -LiteralPath $makefilePath

function Assert-Contains {
    param(
        [string] $Text,
        [string] $Pattern,
        [string] $Label
    )
    if (-not $Text.Contains($Pattern)) {
        throw "$Label is missing: $Pattern"
    }
}

function Assert-Absent {
    param(
        [string] $Text,
        [string] $Pattern,
        [string] $Label
    )
    if ($Text.Contains($Pattern, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label still contains: $Pattern"
    }
}

Assert-Contains $installer 'AGENTTEAMS_MANAGER_RUNTIME=agentscope' "PowerShell installer"
Assert-Contains $installer 'http://127.0.0.1:18799/readyz' "PowerShell installer"
Assert-Contains $installer 'QWENPAW_WORKER_IMAGE' "PowerShell installer"
Assert-Absent $installer ('OPEN' + 'HUMAN_WORKER_IMAGE') "PowerShell installer"
Assert-Contains $installer '${ctrlEnvPrefix}CINNY_PUBLIC_URL' "PowerShell installer"
Assert-Contains $verify 'http://127.0.0.1:18799/readyz' "verification script"

@(
    "MANAGER_COPAW_IMAGE",
    "AGENTTEAMS_INSTALL_MANAGER_COPAW_IMAGE",
    "Step-ManagerRuntime",
    "AGENTTEAMS_FORCE_LEGACY",
    "openclaw gateway health"
) | ForEach-Object {
    Assert-Absent $installer $_ "PowerShell installer"
}

@(
    "MANAGER_COPAW_IMAGE",
    "LOCAL_MANAGER_COPAW",
    "build-manager-copaw",
    "push-manager-copaw",
    "push-native-manager-copaw"
) | ForEach-Object {
    Assert-Absent $makefile $_ "Makefile"
}

Assert-Contains $makefile `
    'build: build-manager build-worker build-copaw-worker build-hermes-worker build-qwenpaw-worker build-agentteams-controller' `
    "Makefile"
Assert-Contains $makefile `
    'push: push-manager push-worker push-copaw-worker push-hermes-worker push-qwenpaw-worker push-agentteams-controller push-embedded' `
    "Makefile"

Write-Output "PASS: PowerShell installer exposes one AgentScope Manager and four Worker runtimes"
