# ============================================================
#  install_task.ps1
#  Run this ONCE (no admin needed) to register the scheduled task.
#
#  Usage (normal PowerShell window):
#    cd "C:\Users\james.campbell-harri\OneDrive - RANDYS Worldwide\Documents\Claude\Tracking Automation"
#    .\install_task.ps1
# ============================================================

$TaskName   = "Suredone Shipping Export"
$ScriptDir  = $PSScriptRoot
$RunnerPath = Join-Path $ScriptDir "run_suredone_export.ps1"

# --- STEP 1: Save SFTP password as a user-level environment variable ---

$sftp = Read-Host "Enter SFTP password for sftp.suredone.com (leave blank to skip if already set)"

if ($sftp -ne "") {
    [System.Environment]::SetEnvironmentVariable("SFTP_PASSWORD", $sftp, "User")
    Write-Host "SFTP_PASSWORD saved." -ForegroundColor Green
} else {
    Write-Host "Skipping SFTP_PASSWORD - using existing value." -ForegroundColor Yellow
}

# --- STEP 2: Remove old task if it exists ---

schtasks /delete /tn "$TaskName" /f 2>$null

Write-Host ""
Write-Host "Registering scheduled task: $TaskName ..."

# --- STEP 3: Register task using schtasks.exe ---

$psArgs = "-NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""$RunnerPath"""
$trArg  = "powershell.exe $psArgs"

$result = & schtasks /create /tn "$TaskName" /tr "$trArg" /sc HOURLY /mo 1 /f

if ($LASTEXITCODE -eq 0) {
    Write-Host "Task registered successfully." -ForegroundColor Green
} else {
    Write-Host "ERROR: Task registration failed. Output: $result" -ForegroundColor Red
    exit 1
}

# --- STEP 4: Optional test run ---

Write-Host ""
$run = Read-Host "Run it right now to verify? (y/n)"

if ($run -eq "y") {
    Write-Host "Starting task..." -ForegroundColor Cyan
    & schtasks /run /tn "$TaskName"
    Start-Sleep -Seconds 8

    $info = & schtasks /query /tn "$TaskName" /fo LIST /v 2>&1
    $info | Select-String "Last Run Time|Last Result|Status" | ForEach-Object { Write-Host $_.Line }

    $logDir = Join-Path $ScriptDir "logs"
    Write-Host ""
    Write-Host "Logs folder: $logDir"
    if (Test-Path $logDir) {
        Get-ChildItem $logDir | Sort-Object LastWriteTime -Descending | Select-Object -First 3 | ForEach-Object {
            Write-Host "  $($_.Name)  ($([math]::Round($_.Length/1KB,1)) KB)"
        }
    } else {
        Write-Host "  (no log file yet - the script may still be running)"
    }
}

Write-Host ""
Write-Host "Done. The task will now run every hour on the hour." -ForegroundColor Green
