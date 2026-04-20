@echo off
echo.
echo  Shopify Token Setup
echo  ════════════════════════════════════════════
echo  This will open a browser to authorize the app
echo  and save your access token to config.py.
echo.
echo  Installing dependencies...
py -m pip install requests --quiet
echo.
py "%~dp0scripts\get_shopify_token.py" --store zumbrotadrivetrain
echo.
pause
