$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$installer = Join-Path $root "install\agentteams-install.ps1"
$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("agentteams-win-install-" + [guid]::NewGuid().ToString("N"))
$fakeBin = Join-Path $tempRoot "bin"
$envFile = Join-Path $tempRoot "agentteams-manager.env"
$workspace = Join-Path $tempRoot "workspace"
$dockerLog = Join-Path $tempRoot "docker.log"
$stdoutLog = Join-Path $tempRoot "stdout.log"
$stderrLog = Join-Path $tempRoot "stderr.log"

function Assert-Equal {
    param([string]$Actual, [string]$Expected, [string]$Label)
    if ($Actual -ne $Expected) {
        throw "$Label mismatch: expected '$Expected', got '$Actual'"
    }
}

try {
    New-Item -ItemType Directory -Path $fakeBin, $workspace -Force | Out-Null

    @'
@echo off
echo %*>>"%FAKE_DOCKER_LOG%"
if /I "%1"=="version" (
  echo 28.0.0
  exit /b 0
)
if /I "%1"=="ps" (
  echo agentteams-manager
  exit /b 0
)
if /I "%1"=="inspect" (
  echo running
  exit /b 0
)
if /I "%1"=="run" (
  echo fake-container-id
  exit /b 0
)
if /I "%1"=="exec" (
  echo {}
  exit /b 0
)
exit /b 0
'@ | Set-Content -LiteralPath (Join-Path $fakeBin "docker.cmd") -Encoding ascii

    @"
AGENTTEAMS_LANGUAGE=en
AGENTTEAMS_LLM_PROVIDER=qwen
AGENTTEAMS_LLM_API_KEY=test-api-key-preserved
AGENTTEAMS_DEFAULT_MODEL=qwen3.5-plus
AGENTTEAMS_ADMIN_USER=test-admin
AGENTTEAMS_ADMIN_PASSWORD=test-password-preserved
AGENTTEAMS_LOCAL_ONLY=1
AGENTTEAMS_PORT_GATEWAY=29380
AGENTTEAMS_PORT_CONSOLE=29301
AGENTTEAMS_PORT_ELEMENT_WEB=29388
AGENTTEAMS_MATRIX_DOMAIN=matrix-local.agentteams.io:29380
AGENTTEAMS_MATRIX_CLIENT_DOMAIN=matrix-client-local.agentteams.io
AGENTTEAMS_AI_GATEWAY_DOMAIN=aigw-local.agentteams.io
AGENTTEAMS_FS_DOMAIN=fs-local.agentteams.io
AGENTTEAMS_MATRIX_E2EE=0
AGENTTEAMS_MATRIX_APPSERVICE_ENABLED=true
AGENTTEAMS_DEFAULT_WORKER_RUNTIME=qwenpaw
AGENTTEAMS_WORKER_IDLE_TIMEOUT=720
AGENTTEAMS_DATA_DIR=agentteams-test-data
AGENTTEAMS_WORKSPACE_DIR=$workspace
AGENTTEAMS_HOST_SHARE_DIR=$tempRoot
"@ | Set-Content -LiteralPath $envFile -Encoding utf8

    $pwsh = (Get-Command pwsh).Source
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $pwsh
    $start.WorkingDirectory = $root
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.ArgumentList.Add("-NoProfile")
    $start.ArgumentList.Add("-NonInteractive")
    $start.ArgumentList.Add("-File")
    $start.ArgumentList.Add($installer)
    $start.ArgumentList.Add("manager")
    $start.ArgumentList.Add("-NonInteractive")
    $start.ArgumentList.Add("-EnvFile")
    $start.ArgumentList.Add($envFile)

    $start.Environment["PATH"] = "$fakeBin;$($start.Environment["PATH"])"
    $start.Environment["FAKE_DOCKER_LOG"] = $dockerLog
    $start.Environment["AGENTTEAMS_ENV_FILE"] = $envFile
    $start.Environment["AGENTTEAMS_NON_INTERACTIVE"] = "1"
    $start.Environment["AGENTTEAMS_UPGRADE_KEEP_ALL"] = "1"
    $start.Environment["AGENTTEAMS_YOLO"] = "1"
    $start.Environment["AGENTTEAMS_VERSION"] = "test"
    $start.Environment["AGENTTEAMS_MOUNT_SOCKET"] = "1"
    $start.Environment["AGENTTEAMS_WELCOME_TIMEOUT"] = "0"
    $start.Environment["AGENTTEAMS_INSTALL_EMBEDDED_IMAGE"] = "agentteams/agentteams-embedded:test"
    $start.Environment["AGENTTEAMS_INSTALL_MANAGER_IMAGE"] = "agentteams/manager:test"
    $start.Environment["AGENTTEAMS_INSTALL_WORKER_IMAGE"] = "agentteams/worker-agent:test"
    $start.Environment["AGENTTEAMS_INSTALL_COPAW_WORKER_IMAGE"] = "agentteams/copaw-worker:test"
    $start.Environment["AGENTTEAMS_INSTALL_HERMES_WORKER_IMAGE"] = "agentteams/hermes-worker:test"
    $start.Environment["AGENTTEAMS_INSTALL_QWENPAW_WORKER_IMAGE"] = "agentteams/qwenpaw-worker:test"
    $start.Environment["AGENTTEAMS_INSTALL_OPENHUMAN_WORKER_IMAGE"] = "agentteams/openhuman-worker:disabled"
    $null = $start.Environment.Remove("AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN")
    $null = $start.Environment.Remove("AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN")

    $process = [Diagnostics.Process]::Start($start)
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()
    $stdoutTask.Result | Set-Content -LiteralPath $stdoutLog
    $stderrTask.Result | Set-Content -LiteralPath $stderrLog
    if ($process.ExitCode -ne 0) {
        throw "Installer exited with $($process.ExitCode). See $stdoutLog and $stderrLog"
    }

    $saved = @{}
    Get-Content -LiteralPath $envFile | ForEach-Object {
        if ($_ -match "^([^#=][^=]*)=(.*)$") {
            $saved[$Matches[1].Trim()] = $Matches[2].Trim()
        }
    }

    Assert-Equal $saved["AGENTTEAMS_LLM_API_KEY"] "test-api-key-preserved" "LLM API key"
    Assert-Equal $saved["AGENTTEAMS_ADMIN_USER"] "test-admin" "admin username"
    Assert-Equal $saved["AGENTTEAMS_ADMIN_PASSWORD"] "test-password-preserved" "admin password"
    Assert-Equal $saved["AGENTTEAMS_PORT_CINNY"] "29388" "Cinny port"
    if ($saved.ContainsKey("AGENTTEAMS_PORT_ELEMENT_WEB")) {
        throw "Legacy Element port was written back"
    }

    $asToken = $saved["AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN"]
    $hsToken = $saved["AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN"]
    if ($asToken -notmatch "^[a-f0-9]{64}$") {
        throw "AppService as_token was not generated and persisted"
    }
    if ($hsToken -notmatch "^[a-f0-9]{64}$") {
        throw "AppService hs_token was not generated and persisted"
    }
    if ($asToken -eq $hsToken) {
        throw "AppService tokens must be independently generated"
    }

    $runCommand = Get-Content -LiteralPath $dockerLog | Where-Object { $_ -like "run *" } | Select-Object -Last 1
    if ($runCommand -notlike "*AGENTTEAMS_MATRIX_APPSERVICE_ENABLED=true*") {
        throw "Controller run command did not enable Matrix AppService"
    }
    if ($runCommand -notlike "*AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN=$asToken*") {
        throw "Controller run command did not receive the persisted as_token"
    }
    if ($runCommand -notlike "*AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN=$hsToken*") {
        throw "Controller run command did not receive the persisted hs_token"
    }
    if ($runCommand -notlike "*AGENTTEAMS_CINNY_HOMESERVER_URL=http://127.0.0.1:29380*") {
        throw "Controller run command did not receive the Cinny homeserver URL"
    }
    if ($runCommand -notlike "*AGENTTEAMS_CINNY_PUBLIC_URL=http://127.0.0.1:29388*") {
        throw "Controller run command did not receive the public Cinny discovery URL"
    }
    if ($runCommand -notlike "*AGENTTEAMS_CINNY_URL=http://127.0.0.1:8088*") {
        throw "Controller run command did not receive the internal Cinny route URL"
    }
    if ($runCommand -notlike "*AGENTTEAMS_PORT_CINNY=29388*") {
        throw "Controller run command did not receive the canonical Cinny port"
    }
    if ($runCommand -notlike "*29388:8088*") {
        throw "Controller run command did not preserve the legacy UI port for Cinny"
    }
    if ($runCommand -like "*AGENTTEAMS_ELEMENT_HOMESERVER_URL*") {
        throw "Controller run command emitted the legacy Element homeserver variable"
    }

    Write-Output "PASS: Windows keep-all upgrade migrates Cinny settings and preserves credentials"
}
finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}
