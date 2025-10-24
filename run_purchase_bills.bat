@echo off
cd /d "C:\Users\souri\OneDrive\Working Folder\V-CFO\Thermoflyde Industries Private Limited\python script"
python purchase_bills_etl.py >> purchase_etl_log.txt 2>&1
pause
