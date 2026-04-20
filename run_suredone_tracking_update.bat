@echo off
REM ─────────────────────────────────────────────────────────────
REM  run_suredone_tracking_update.bat
REM
REM  Finds SureDone orders with missing tracking numbers,
REM  looks them up in GP, and updates SureDone directly via API.
REM
REM  Options:
REM    --days 30        scan orders from last N days (default 30)
REM    --all            scan ALL SureDone orders ever (slow)
REM    --dry-run        show what would change, don't write anything
REM ─────────────────────────────────────────────────────────────

set SCRIPT_DIR=%~dp0

echo.
echo  SureDone Tracking Updater
echo  ════════════════════════════════════════════
echo.

py -m pip install -r "%SCRIPT_DIR%scripts\requirements.txt" --quiet

py "%SCRIPT_DIR%scripts\suredone_tracking_update.py" %*

if errorlevel 1 (
    echo.
    echo  *** Script exited with an error. See above. ***
)

echo.
pause
