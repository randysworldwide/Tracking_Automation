# ============================================================
#  run_suredone_export.ps1
#  Called by Windows Task Scheduler every hour.
#  Place this file next to the suredone_export folder.
# ============================================================

$ScriptDir    = $PSScriptRoot
$PythonScript = Join-Path $ScriptDir "suredone_export\suredone_export.py"
$LogDir       = Join-Path $ScriptDir "logs"
$LogFile      = Join-Path $LogDir ("suredone_export_" + (Get-Date -Format "yyyyMMdd") + ".log")

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

function Write-Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]  $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

Write-Log "===== Suredone export starting ====="

if (-not (Test-Path $PythonScript)) {
    Write-Log "ERROR: Script not found at $PythonScript"
    exit 1
}

# Read SFTP password from user-level environment variable
$env:SFTP_PASSWORD = [System.Environment]::GetEnvironmentVariable("SFTP_PASSWORD", "User")

if (-not $env:SFTP_PASSWORD) {
    Write-Log "WARNING: SFTP_PASSWORD is not set. Upload will fail."
}

# Find Python
$Python = $null
foreach ($candidate in @("py", "python", "python3")) {
    try {
        $ver = & $candidate --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            $Python = $candidate
            Write-Log "Using Python: $candidate ($ver)"
            break
        }
    } catch { }
}

if (-not $Python) {
    Write-Log "ERROR: Python not found. Install Python and ensure it is on PATH."
    exit 1
}

# Run the export
$start = Get-Date

& $Python $PythonScript 2>&1 | ForEach-Object {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')]  $_"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

$exitCode = $LASTEXITCODE
$elapsed  = [math]::Round(((Get-Date) - $start).TotalSeconds, 1)

Write-Log "===== Export finished - exit code $exitCode (${elapsed}s) ====="

# Prune logs older than 30 days
Get-ChildItem $LogDir -Filter "suredone_export_*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -Force

exit $exitCode
