@echo off
cd /D C:\TEMP

set KNMI_API_MODEL=
set KNMI_API_METING=


IF EXIST "C:\TEMP\.knmi_venv" (
    echo virtual environment bestaat al.
    CALL C:\TEMP\.knmi_venv\Scripts\activate 

) ELSE (
    echo maak virtual environment aan
    CALL python -m venv .knmi_venv
    CALL .knmi_venv\Scripts\activate
    Copy "knmi\requirements.txt" "C:\TEMP\."
    Copy "knmi\setup_knmi.py" "C:\TEMP\."
    Copy "knmi\modules\divers\vector.py" "C:\TEMP\.
    python setup_knmi.py install
    pip install -r requirements.txt
    pip install "C:\temp\knmi\GDAL-3.3.3-cp39-cp39-win_amd64.whl"
    DEL /Q "C:\temp\vector.py"
    DEL /Q "C:\temp\setup_knmi.py"
    DEL /Q "C:\temp\requirements.txt"
)

Copy "C:\temp\knmi\modules\knmi_opendata_mroapi.py" "C:\temp\."
python -m "knmi_opendata_mroapi" 

DEL /Q "C:\temp\knmi_opendata_mroapi.py"

deactivate
cd /D "C:\temp\knmi"

