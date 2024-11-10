rem start-readers.bat
rem
rem
set /a number=%random%

start "mint1" cmd /c %py% while_test1.py       ^> logs\T1_%number%.log
start "mint2" cmd /c %py% while_test2.py       ^> logs\T2_%number%.log
start "mint4" cmd /c %py% while_test4_share.py ^> logs\T4_%number%.log
start "mint5" cmd /c %py% while_test5_share.py ^> logs\T5_%number%.log
start "mint6" cmd /c %py% while_test6_share.py ^> logs\T6_%number%.log
