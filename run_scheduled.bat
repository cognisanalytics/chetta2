@echo off
cd /d "%~dp0"
uv run run_scheduled.py %*
exit /b %ERRORLEVEL%
