@echo off

cd /d %~dp0

echo Building isp-parole-officer...
go build

if errorlevel 1 (
    echo Failed to build..
    exit /b 1
)

isp-parole-officer.exe config.json &