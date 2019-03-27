rem This script starts a DtssHost on the current machine. cwd must be the containing folder of this script.
rem start_dtss.py assumes that env.var. CONFIG_DIRECTORY contains a path containing configs for current machine.
setlocal
cd ../..
echo %cd%
set PATH=%PATH%;%USERPROFILE%\Miniconda3\condabin;
call conda activate shyft
rem Add module path to pythonpath.
set PATH=%PATH%;%cd%
call python -c "from weather.scripts import start_dtss"
endlocal
pause