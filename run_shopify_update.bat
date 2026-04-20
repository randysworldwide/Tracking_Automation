@echo off
REM ─────────────────────────────────────────────────────────────
REM  run_shopify_update.bat
REM
REM  Finds Shopify orders missing ERP metafields, looks them up
REM  in GP, and fills them in directly via the Shopify API.
REM
REM  Options (edit below or pass on command line):
REM    --days 90        scan orders from last N days (default 90)
REM    --all            scan ALL orders ever
REM    --store zumbrota only process a specific store
REM ─────────────────────────────────────────────────────────────

set SCRIPT_DIR=%~dp0

echo.
echo  Shopify ERP Metafield Filler
echo  ════════════════════════════════════════════
echo.

REM Install / update dependencies quietly
py -m pip install pyodbc requests --quiet

REM Run the script — pass any extra args through
py "%SCRIPT_DIR%scripts\shopify_update.py" %*

if errorlevel 1 (
    echo.
    echo  *** Script exited with an error. See above. ***
)

echo.
pause
