@echo off
setlocal enabledelayedexpansion

chcp 65001 > nul

set "OUTPUT_FILE=combined_project_code.txt"

if exist "%OUTPUT_FILE%" del "%OUTPUT_FILE%"

set "count=0"

echo Початок сканування та об'єднання файлів .py...
echo --------------------------------------------------

set "ROOT_DIR=%CD%"

for /f "delims=" %%F in ('dir /b /s *.py 2^>nul') do (
    set "FILE_PATH=%%F"
    set "SKIP="

    rem Пропускаємо сам скрипт, якщо він випадково має .py розширення
    if /i not "%%F"=="%~f0" (

        rem Ігноруємо технічні папки
        if not "!FILE_PATH:\.venv312\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\.venv\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\venv\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\env\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\__pycache__\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\.git\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\.idea\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\.vscode\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\build\=!"=="!FILE_PATH!" set "SKIP=1"
        if not "!FILE_PATH:\dist\=!"=="!FILE_PATH!" set "SKIP=1"

        if not defined SKIP (
            set "REL_PATH=!FILE_PATH:%ROOT_DIR%\=!"

            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo START OF FILE: !REL_PATH! >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo. >> "%OUTPUT_FILE%"

            type "%%F" >> "%OUTPUT_FILE%"

            echo. >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo END OF FILE: !REL_PATH! >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo. >> "%OUTPUT_FILE%"

            echo [+] Додано: !REL_PATH!
            set /a count+=1
        )
    )
)

echo --------------------------------------------------
echo Успішно завершено! Об'єднано файлів: !count!.
echo Результат збережено у: %OUTPUT_FILE%
echo --------------------------------------------------

pause