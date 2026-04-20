@echo off
REM ─────────────────────────────────────────────────────────────
REM  run_matrixify_filler.bat
REM  Drag-and-drop a Matrixify export xlsx onto this file,
REM  or just double-click to auto-detect the newest xlsx nearby.
REM ─────────────────────────────────────────────────────────────

set SCRIPT_DIR=%~dp0
set PYTHON=py

echo.
echo  Matrixify ERP Filler
echo  ════════════════════════════════════════════
echo.

echo  Installing/checking dependencies...
py -m pip install pandas openpyxl pyodbc --quiet
if errorlevel 1 (
    echo  WARNING: pip install had issues, continuing anyway...
)
echo.

if "%~1"=="" (
    echo  No file dragged — looking for newest .xlsx in script folder...
    %PYTHON% "%SCRIPT_DIR%scripts\matrixify_erp_filler.py"
) else (
    echo  Input file: %~1
    %PYTHON% "%SCRIPT_DIR%scripts\matrixify_erp_filler.py" "%~1"
)

if errorlevel 1 (
    echo.
    echo  *** Script failed. See error above. ***
) else (
    echo.
    echo  Output files are in: %SCRIPT_DIR%matrixify_output\
)

echo.
pause
