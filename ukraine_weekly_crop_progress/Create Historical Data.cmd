@echo off
setlocal
cd /d "%~dp0"

set "FROM=%~1"
if not defined FROM (
  set /p FROM=Start date in YYYY-MM-DD [2024-01-05]: 
)
if not defined FROM set "FROM=2024-01-05"

set "TO=%~2"
if not defined TO (
  for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TO=%%i"
)

echo Running gap-aware historical sync from %FROM% to %TO%
echo The script will only fetch missing weekly dates inside that range.
python .\src\minagro_weekly_workflow.py historical --from %FROM% --to %TO%
echo.
pause
