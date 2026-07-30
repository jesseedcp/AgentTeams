$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$installerPath = Join-Path $root "install\agentteams-install.ps1"
$source = Get-Content -Raw -LiteralPath $installerPath
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseInput(
    $source,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count) {
    throw ($parseErrors | ForEach-Object Message | Out-String)
}

$requiredFunctions = @(
    "ConvertTo-NormalizedVersion",
    "Test-VersionLessThan",
    "Test-UseLegacyImageEnv",
    "Get-ControllerEnvPrefix",
    "Get-ControllerStoragePrefix"
)
$definitions = $ast.FindAll(
    {
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $requiredFunctions -contains $node.Name
    },
    $true
)
if ($definitions.Count -ne $requiredFunctions.Count) {
    throw "Version compatibility functions are missing from the PowerShell installer"
}
foreach ($definition in $definitions) {
    Invoke-Expression $definition.Extent.Text
}

function Assert-Equal {
    param($Actual, $Expected, [string]$Label)
    if ($Actual -ne $Expected) {
        throw "$Label mismatch: expected '$Expected', got '$Actual'"
    }
}

@(
    @("1.2.0.beta.1", "v1.2.0-beta.1"),
    @("v1.2.0-beta.1", "v1.2.0-beta.1"),
    @("1.1.2+build.7", "v1.1.2+build.7"),
    @("1.1", "v1.1.0"),
    @("latest", "latest")
) | ForEach-Object {
    Assert-Equal `
        (ConvertTo-NormalizedVersion $_[0]) `
        $_[1] `
        "normalize $($_[0])"
}

$script:AGENTTEAMS_KNOWN_STABLE_VERSION = "latest"
@("v1.0.0", "v1.1.2", "v1.1.9+build.7") | ForEach-Object {
    Assert-Equal (Test-UseLegacyImageEnv $_) $true "legacy $_"
}
@("v1.2.0", "v1.2.0-rc.1", "v1.3.0", "garbage") | ForEach-Object {
    Assert-Equal (Test-UseLegacyImageEnv $_) $false "current $_"
}

$script:AGENTTEAMS_KNOWN_STABLE_VERSION = "v1.1.2"
Assert-Equal (Test-UseLegacyImageEnv "latest") $true "legacy latest"
$script:AGENTTEAMS_KNOWN_STABLE_VERSION = "v1.2.0"
Assert-Equal (Test-UseLegacyImageEnv "latest") $false "current latest"

$legacyPrefix = "HIC" + "LAW_"
Assert-Equal (Get-ControllerEnvPrefix "v1.1.2") $legacyPrefix "legacy prefix"
Assert-Equal (Get-ControllerEnvPrefix "v1.2.0") "AGENTTEAMS_" "current prefix"
Assert-Equal (Get-ControllerEnvPrefix "garbage") "AGENTTEAMS_" "invalid prefix"
Assert-Equal `
    (Get-ControllerStoragePrefix "v1.1.2") `
    ("hic" + "law/agentteams-storage") `
    "legacy storage prefix"
Assert-Equal `
    (Get-ControllerStoragePrefix "v1.2.0") `
    "agentteams/agentteams-storage" `
    "current storage prefix"
Assert-Equal `
    (Get-ControllerStoragePrefix "garbage") `
    "agentteams/agentteams-storage" `
    "invalid storage prefix"

$blockMatch = [regex]::Match(
    $source,
    '(?s)        # Controller env args.*?        if \(\$script:AGENTTEAMS_TIMEZONE\)'
)
if (-not $blockMatch.Success) {
    throw "PowerShell controller environment block was not found"
}
$block = $blockMatch.Value
@(
    "REGISTRATION_TOKEN",
    "MINIO_USER",
    "MINIO_PASSWORD",
    "MANAGER_IMAGE",
    "WORKER_IMAGE",
    "COPAW_WORKER_IMAGE",
    "HERMES_WORKER_IMAGE",
    "QWENPAW_WORKER_IMAGE",
    "MATRIX_DOMAIN",
    "MATRIX_URL",
    "MINIO_ENDPOINT",
    "CONTROLLER_URL",
    "DOCKER_NETWORK"
) | ForEach-Object {
    if (-not $block.Contains(('${ctrlEnvPrefix}' + $_ + '='))) {
        throw "Controller env $_ does not use the selected prefix"
    }
    if ($block.Contains(('"AGENTTEAMS_' + $_ + '=')) -or
        $block.Contains(('"' + $legacyPrefix + $_ + '='))) {
        throw "Controller env $_ also has a fixed prefix"
    }
}

if (-not $block.Contains(
    '"${ctrlEnvPrefix}STORAGE_PREFIX=$storagePrefix"'
)) {
    throw "Controller storage prefix does not use the selected value"
}

Write-Output "PASS: PowerShell installer selects one controller env contract by image version"
