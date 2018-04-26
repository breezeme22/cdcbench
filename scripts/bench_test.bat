@echo off
SETLOCAL enabledelayedexpansion

:_loop
cdcbench -i 750000 600

goto _loop