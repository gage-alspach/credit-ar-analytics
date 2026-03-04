
@echo off
setlocal

REM Resolve this script'"'"'s folder and run main.py from there.
set \"SCRIPT_DIR=%~dp0\"
pushd \"%SCRIPT_DIR%\"

REM Use project's virtual environment Python.
set "VENV_PY=%SCRIPT_DIR%.venv\Scripts\python.exe"
"%VENV_PY%" main.py

popd
endlocal
