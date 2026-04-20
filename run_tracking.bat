@echo off
cd /d "C:\Users\james.campbell-harri\OneDrive - RANDYS Worldwide\Documents\Claude\Tracking Automation"
py scripts\suredone_tracking_update.py --days 10 > exports\tracking_run_log.txt 2>&1
echo Exit code: %ERRORLEVEL% >> exports\tracking_run_log.txt
