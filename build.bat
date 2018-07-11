@echo off
rem Build pyadcp
setlocal ENABLEEXTENSIONS
set KEY_NAME="HKEY_LOCAL_MACHINE\SOFTWARE\Python\PythonCore\3.6\InstallPath"
set VALUE_NAME=ExecutablePath

FOR /F "usebackq skip=2 tokens=1,2,*" %%A IN (`REG QUERY %KEY_NAME% /v %VALUE_NAME% 2^>nul`) DO (
    set PYTHON36="%%C"
)

if defined PYTHON36 (
    %PYTHON36% -m venv env
    .\env\Scripts\python.exe -m pip install -r requirements.txt
    .\env\Scripts\python.exe -m pip install pyinstaller
    .\env\Scripts\pyinstaller.exe -F query.py
    .\env\Scripts\pyinstaller.exe -F convert.py
) else (
    @echo "Python3.6 not found"
    exit /B 1
)