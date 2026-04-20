@echo off
title Suredone Export
echo.
echo ============================================================
echo   Suredone Shipping Export
echo ============================================================
echo.

REM Install dependencies if needed
echo Checking dependencies...
py -m pip install -r "%~dp0scripts\requirements.txt" --quiet
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Could not install dependencies. Make sure Python is installed.
    pause
    exit /b 1
)

echo.
echo Running export...
echo.
py "%~dp0scripts\suredone_export.py"

echo.
if %errorlevel% equ 0 (
    echo SUCCESS - press any key to close
) else (
    echo FAILED - see error above - press any key to close
)
pause > nul
