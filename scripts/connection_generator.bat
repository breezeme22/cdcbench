@echo off
SETLOCAL enabledelayedexpansion
SET INSERT_ROW=2000000
SET COMMIT_UNIT=20000
SET START_NUM=1
SET END_NUM=100
SET STEP=100

FOR /L %%i in (1, 1, 2) DO (
	START python source\cdcbench.py -u !START_NUM! !END_NUM!
	SET /a START_NUM+=%STEP%
	SET /a END_NUM+=%STEP%
)