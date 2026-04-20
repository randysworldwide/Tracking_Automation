Set-Location 'C:\Users\james.campbell-harri\OneDrive - RANDYS Worldwide\Documents\Claude\Tracking Automation'
C:\Users\james.campbell-harri\AppData\Local\Programs\Python\Python313\python.exe suredone_tracking_update.py --days 10 > output.log 2>&1
Write-Host "EXIT_CODE: $LASTEXITCODE"
Get-Content output.log
