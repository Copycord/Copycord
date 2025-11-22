@echo off
setlocal enabledelayedexpansion

REM Directory where this script lives (repo root)
set "ROOT=%~dp0"
REM Remove trailing backslash if present
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "CODE_DIR=%ROOT%\code"
set "VENV_ROOT=%ROOT%\venvs"

set "ADMIN_PY=%VENV_ROOT%\admin\Scripts\python.exe"
set "SERVER_PY=%VENV_ROOT%\server\Scripts\python.exe"
set "CLIENT_PY=%VENV_ROOT%\client\Scripts\python.exe"

if not exist "%CODE_DIR%" (
    echo Error: code\ directory not found at "%CODE_DIR%"
    echo Make sure you ran: python install_standalone.py
    goto :EOF
)

if not exist "%ADMIN_PY%" (
    echo Error: admin venv python not found at "%ADMIN_PY%"
    echo Make sure you ran: python install_standalone.py
    goto :EOF
)

if not exist "%SERVER_PY%" (
    echo Error: server venv python not found at "%SERVER_PY%"
    echo Make sure you ran: python install_standalone.py
    goto :EOF
)

if not exist "%CLIENT_PY%" (
    echo Error: client venv python not found at "%CLIENT_PY%"
    echo Make sure you ran: python install_standalone.py
    goto :EOF
)

REM Default admin port; can be overridden by ADMIN_PORT in code\.env
set "ADMIN_PORT=8080"
if exist "%CODE_DIR%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%CODE_DIR%\.env") do (
        if /I "%%~A"=="ADMIN_PORT" (
            set "ADMIN_PORT=%%~B"
        )
    )
)

echo.
echo Starting Copycord...
echo   Root: %ROOT%
echo   Admin UI port: %ADMIN_PORT%
echo.

REM Start Admin UI in its own terminal window
echo Starting Copycord Admin UI window...
start "Copycord Admin" /D "%CODE_DIR%" "%ADMIN_PY%" -m uvicorn admin.app:app --host 0.0.0.0 --port %ADMIN_PORT%

REM Start Server control in its own terminal window
echo Starting Copycord Server control window...
set "ROLE=server"
set "CONTROL_PORT=9101"
start "Copycord Server" /D "%CODE_DIR%" "%SERVER_PY%" -m control.control

REM Start Client control in its own terminal window
echo Starting Copycord Client control window...
set "ROLE=client"
set "CONTROL_PORT=9102"
start "Copycord Client" /D "%CODE_DIR%" "%CLIENT_PY%" -m control.control

echo.
echo All Copycord components started in separate terminals.
echo   - Copycord Admin   (web UI)
echo   - Copycord Server  (server control service)
echo   - Copycord Client  (client control service)
echo Close those windows to stop the services.
echo.

endlocal
