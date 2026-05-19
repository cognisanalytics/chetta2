@echo off
cd /d "%~dp0"
python run_scheduled.py %*
exit /b %ERRORLEVEL%
