@echo off

cd /d %~dp0

echo Building isp-parole-officer...
go build

if errorlevel 1 (
    echo Failed to build..
    pause
    exit /b 1
)

start "" "isp-parole-officer.exe"
exit
