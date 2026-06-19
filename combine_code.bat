@echo off
setlocal enabledelayedexpansion
:: Встановлюємо кодування UTF-8 в консолі для підтримки кирилиці та спецсимволів
chcp 65001 > nul

set "OUTPUT_FILE=combined_project_code.txt"

:: Якщо старий файл результату існує — видаляємо його
if exist "%OUTPUT_FILE%" del "%OUTPUT_FILE%"

set "count=0"
echo Початок сканування та об'єднання файлів .py...
echo --------------------------------------------------

:: Запам'ятовуємо корінь проекту, щоб зробити шляхи відносними
set "ROOT_DIR=%CD%"

:: Шукаємо всі файли .py в поточній папці та підпапках
for /f "delims=" %%F in ('dir /b /s *.py 2^>nul') do (
    set "FILE_PATH=%%F"
    
    :: Захист: пропускаємо сам файл скрипту, якщо його випадково назвали з розширенням .py
    if /i "%%F" neq "%~f0" (
        
        :: Перевірка на технічні папки (ігноруємо їх)
        set "SKIP="
        echo !FILE_PATH! | findstr /i /c:"\.venv\" /c:"\venv\" /c:"\env\" /c:"\__pycache__\" /c:"\.git\" /c:"\.idea\" /c:"\.vscode\" /c:"\build\" /c:"\dist\" >nul
        if !errorlevel! equ 0 set "SKIP=1"

        if not defined SKIP (
            :: Вирізаємо абсолютний шлях, залишаючи красивий відносний (наприклад: src/engine.py)
            set "REL_PATH=!FILE_PATH:%ROOT_DIR%\=!"
            
            :: Записуємо розділювач початку файла
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo  START OF FILE: !REL_PATH! >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo. >> "%OUTPUT_FILE%"
            
            :: Додаємо вміст самого файла
            type "%%F" >> "%OUTPUT_FILE%"
            
            :: Записуємо розділювач кінця файла
            echo. >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            echo  END OF FILE: !REL_PATH! >> "%OUTPUT_FILE%"
            echo ================================================================================ >> "%OUTPUT_FILE%"
            
            echo  [+] Додано: !REL_PATH!
            set /a count+=1
        )
    )
)

echo --------------------------------------------------
echo  Успішно завершено! Об'єднано файлів: %count%.
echo  Результат збережено у: %OUTPUT_FILE%
echo --------------------------------------------------
pause