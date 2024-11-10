rem start-writers.bat
rem
start "mint1" cmd /c %py% while_test3.py ^> w3.log
start "mint2" cmd /c %py% while_test2.py ^> w4.log
start "mint4" cmd /c %py% while_test4_update.py ^> w4.log
start "mint5" cmd /c %py% while_test9_update.py ^> w9.log
