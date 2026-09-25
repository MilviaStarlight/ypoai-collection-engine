@echo off
rem YPOAI lane runner for Windows. Usage:  run_lane.cmd W3
rem Runs the lane worker for 8 hours at a time, forever, from a throwaway clone in %TEMP%.
rem Needs: git, python 3.11, and the user environment variable SUPABASE_SERVICE_ROLE_KEY (never put the key in this file).
setlocal
if "%~1"=="" ( echo Usage: run_lane.cmd W1 ^| W2 ^| W3 ^| W4 ^| W5 ^| W6 & exit /b 1 )
set LANE=%~1
if "%SUPABASE_SERVICE_ROLE_KEY%"=="" ( echo SUPABASE_SERVICE_ROLE_KEY is not set. Set it as a user environment variable and open a new window. & exit /b 1 )
set WORK=%TEMP%\ypoai-%LANE%
title YPOAI lane %LANE%
:loop
echo [%date% %time%] fetching latest code...
if exist "%WORK%" rmdir /s /q "%WORK%"
git clone -q --depth 1 https://github.com/MilviaStarlight/ypoai-collection-engine "%WORK%" || (echo clone failed, retrying in 5 minutes & timeout /t 300 /nobreak >nul & goto loop)
python -m pip install -q -r "%WORK%\requirements.txt"
echo [%date% %time%] starting lane %LANE% for 8 hours
python "%WORK%\scripts\worker.py" start --lane %LANE% --hours 8
echo [%date% %time%] lane %LANE% cycle ended; restarting in 60 seconds (close this window to stop)
timeout /t 60 /nobreak >nul
goto loop
