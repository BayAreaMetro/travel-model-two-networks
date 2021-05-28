:: The location of the RUNTPP executable from Citilabs
set TPP_PATH=C:\Program Files\Citilabs\CubeVoyager

:: The location of the Cube executable from Citilabs
set CUBE_PATH=C:\Program Files (x86)\Citilabs\Cube

:: Add cube to path
set PATH=C:\Windows\System32;%TPP_PATH%;%CUBE_PATH%

runtpp assign_scalar.s
REM runtpp add_transit.s
runtpp set_assignable.s
