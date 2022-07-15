::
:: Create Bay Area networks
:: See Readme.md for details
::

:: Step1: Extract SharedStreets
:step1
python step1_extract_shst.py
if errorlevel 1 goto error

:: Convert them to geofeather
:step1_geofeather
python ..\..\src\scripts\convert_geojson_to_geofeather.py "%OUTPUT_DATA_DIR%\step1_shst_extracts"
if errorlevel 1 goto error

:: Convert them to geopackage
:step1_geopackage
python ..\..\src\scripts\convert_geojson_to_geopackage.py "%OUTPUT_DATA_DIR%\step1_shst_extracts" "%OUTPUT_DATA_DIR%\step1_shst_extracts\mtc_all_out.gpkg"
if errorlevel 1 goto error

:: Step2: OSMnx Extraction
:step2
python step2_osmnx_extraction.py
if errorlevel 1 goto error

:: Step3: Join shst extraction with OSMnx extraction
:step3
python step3_join_shst_extraction_with_osm.py
if errorlevel 1 goto error

echo Successfully completed building networks
goto done

:error
echo An error occurred; errorlevel = %ERRORLEVEL%

:done