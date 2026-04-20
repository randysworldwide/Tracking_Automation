@echo off
title Suredone Export - 5 Business Days
echo.
echo ============================================================
echo   Suredone Shipping Export - Last 5 Business Days
echo ============================================================
echo.

echo Checking dependencies...
py -m pip install -r "%~dp0scripts\requirements.txt" --quiet
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Could not install dependencies. Make sure Python is installed.
    pause
    exit /b 1
)

echo.
echo Running export (last 5 business days)...
echo.
py "%~dp0scripts\suredone_export.py" --business-days 5

echo.
if %errorlevel% equ 0 (
    echo SUCCESS - press any key to close
) else (
    echo FAILED - see error above - press any key to close
)
pause > nul
