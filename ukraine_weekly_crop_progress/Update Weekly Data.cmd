@echo off
setlocal
cd /d "%~dp0"

set "FROM=%~1"
if not defined FROM set "FROM=2024-01-05"

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TO=%%i"

echo Running smart weekly sync from %FROM% through %TO%
echo The script will scan for missing historical and recent weekly gaps, then fill only those.
python .\src\minagro_weekly_workflow.py weekly-update --history-from %FROM% --to %TO%
echo.
pause
