@echo off
setlocal
cd /d "%~dp0"
python .\src\minagro_weekly_workflow.py capture-auth
echo.
pause
