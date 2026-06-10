@echo off

cd /d %~dp0

echo Building paronline-officer...
go build

if errorlevel 1 (
    echo Failed to build..
    exit /b 1
)

paronline-officer.exe config.json &