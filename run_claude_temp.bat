@echo off
cd /d "C:\Users\james.campbell-harri\OneDrive - RANDYS Worldwide\Documents\Claude\Tracking Automation"
"C:\Users\james.campbell-harri\AppData\Local\Programs\Python\Python313\python.exe" scripts\suredone_tracking_update.py --days 10 > run_output_claude.txt 2>&1
echo EXIT_CODE:%ERRORLEVEL% >> run_output_claude.txt
