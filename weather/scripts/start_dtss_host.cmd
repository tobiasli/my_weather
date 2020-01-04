rem This script starts a DtssHost on the current machine. cwd must be the containing folder of this script.
rem start_dtss_host.py assumes that a series of environment variables are set. Errors are thrown if they are missing.
echo off
setlocal
cd ../..
echo %cd%
set PATH=%PATH%;%USERPROFILE%\Miniconda3\condabin;
call conda activate shyft
rem Add module path to pythonpath.
set PYTHONPATH=%PATH%;%cd%
call python weather\scripts\start_dtss_host.py
endlocal
pause