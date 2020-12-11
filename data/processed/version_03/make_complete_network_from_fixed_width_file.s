RUN PGM = NETWORK MSG = "Read in network from fixed width file" 
FILEI LINKI[1] = %LINK_DATA_PATH%, VAR=A, BEG=1, LEN=7, VAR=B, BEG=9, LEN=7, VAR=assignable, BEG=17, LEN=1, VAR=bike_access, BEG=19, LEN=1, VAR=bus_only, BEG=21, LEN=1, VAR=county(C13), BEG=23, LEN=13, VAR=distance, BEG=37, LEN=22, VAR=drive_access, BEG=60, LEN=1, VAR=ft, BEG=62, LEN=2, VAR=managed, BEG=65, LEN=1, VAR=model_link_id, BEG=67, LEN=8, VAR=rail_only, BEG=76, LEN=1, VAR=segment_id, BEG=78, LEN=4, VAR=shstGeometryId(C32), BEG=83, LEN=32, VAR=tollbooth, BEG=116, LEN=1, VAR=tollseg, BEG=118, LEN=2, VAR=transit, BEG=121, LEN=1, VAR=walk_access, BEG=123, LEN=1, VAR=cntype(C5), BEG=125, LEN=5, VAR=lanes_EA, BEG=131, LEN=1, VAR=lanes_AM, BEG=133, LEN=1, VAR=lanes_MD, BEG=135, LEN=1, VAR=lanes_PM, BEG=137, LEN=1, VAR=lanes_EV, BEG=139, LEN=1, VAR=ML_lanes_EA, BEG=141, LEN=1, VAR=ML_lanes_AM, BEG=143, LEN=1, VAR=ML_lanes_MD, BEG=145, LEN=1, VAR=ML_lanes_PM, BEG=147, LEN=1, VAR=ML_lanes_EV, BEG=149, LEN=1, VAR=useclass_EA, BEG=151, LEN=1, VAR=useclass_AM, BEG=153, LEN=1, VAR=useclass_MD, BEG=155, LEN=1, VAR=useclass_PM, BEG=157, LEN=1, VAR=useclass_EV, BEG=159, LEN=1
FILEI NODEI[1] = %NODE_DATA_PATH%, VAR=osm_node_id(C12), BEG=1, LEN=12, VAR=county(C13), BEG=14, LEN=13, VAR=drive_access, BEG=28, LEN=1, VAR=walk_access, BEG=30, LEN=1, VAR=bike_access, BEG=32, LEN=1, VAR=N, BEG=34, LEN=7, VAR=rail_only, BEG=42, LEN=1, VAR=X, BEG=44, LEN=18, VAR=Y, BEG=63, LEN=18
FILEO NETO = "%SCENARIO_DIR%/complete_network.net" 

    ZONES = %zones% 

;ROADWAY = LTRIM(TRIM(ROADWAY)) 
;NAME = LTRIM(TRIM(NAME)) 

 
ENDRUN