@echo off
set source_folder=D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\SourceFolderForDump
set destination_folder=D:\Sql-LinesSetup\sqlines-3.3.171\sqlines-3.3.171\DestinationFolderForDump

for %%f in ("%source_folder%\*.sql") do (
    echo Processing %%f...
    sqlines -s=mssql -t=postgresql -in="%%f" -out="%destination_folder%\%%~nf_converted.sql"
    if errorlevel 1 (
        echo Error processing %%f >> conversion_errors.log
    ) else (
        echo Successfully processed %%f >> conversion_success.log
    )
)

echo All files processed. Check logs for details.
pause
