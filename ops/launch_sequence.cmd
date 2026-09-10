@echo off
setlocal
rem Drive the funded-launch sequence on the VM from Windows.
rem
rem Everything real happens in ops/launch_sequence.sh on the box; this only gets
rem you there. It runs inside tmux on purpose: the phases take about two hours
rem each, and an SSH drop over that long is not unusual -- without tmux the drop
rem takes the run with it, which has already happened twice here. Closing this
rem window is then harmless, and re-running reattaches to the same session.
rem
rem   launch_sequence.cmd                       calibrate, then stop for the CI step
rem   launch_sequence.cmd --status              where the sequence stands
rem   launch_sequence.cmd --from verify         after the reserves are deployed
rem   launch_sequence.cmd --from arm --confirm-live-trading
rem                                             starts trading real money
rem
rem Detach from tmux with Ctrl-B then D; the run keeps going.

if "%LABYDA_HOST%"=="" set LABYDA_HOST=root@169.58.161.34
if "%LABYDA_DIR%"=="" set LABYDA_DIR=/opt/labyda_next

rem labyda-launch prefers the release's own copy once it has been pulled, and
rem falls back to the one installed outside the checkout. Keeping it out of the
rem checkout matters while a closeout is running: pulling would move HEAD, and
rem the run would abort on its own release-integrity check.
set REMOTE=labyda-launch

if "%~1"=="--status" (
    ssh %LABYDA_HOST% "%REMOTE% --status"
    goto :eof
)

echo Connecting to %LABYDA_HOST% ...
echo Detach with Ctrl-B then D. The run continues without you.
echo.

ssh -t %LABYDA_HOST% "mkdir -p %LABYDA_DIR%/.runtime/launch-sequence && tmux new-session -A -s launch '%REMOTE% %* 2>&1 | tee -a %LABYDA_DIR%/.runtime/launch-sequence/console.log; echo; echo \"--- finished, press enter to close ---\"; read _'"

endlocal
