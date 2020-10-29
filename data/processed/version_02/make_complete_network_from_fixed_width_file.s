RUN PGM = NETWORK MSG = "Read in network from fixed width file" 
FILEI LINKI[1] = %LINK_DATA_PATH%, VAR=bike_access, BEG=1, LEN=1, VAR=drive_access, BEG=3, LEN=1, VAR=shstGeometryId(C32), BEG=5, LEN=32, VAR=walk_access, BEG=38, LEN=1, VAR=county(C13), BEG=40, LEN=13, VAR=model_link_id, BEG=54, LEN=8, VAR=A, BEG=63, LEN=7, VAR=B, BEG=71, LEN=7, VAR=rail_only, BEG=79, LEN=1, VAR=ft, BEG=81, LEN=2, VAR=assignable, BEG=84, LEN=1, VAR=transit, BEG=86, LEN=1, VAR=managed, BEG=88, LEN=1, VAR=segment_id, BEG=90, LEN=4, VAR=tollbooth, BEG=95, LEN=1, VAR=tollseg, BEG=97, LEN=2, VAR=bus_only, BEG=100, LEN=1, VAR=cntype(C5), BEG=102, LEN=5, VAR=lanes_EA, BEG=108, LEN=1, VAR=lanes_AM, BEG=110, LEN=1, VAR=lanes_MD, BEG=112, LEN=1, VAR=lanes_PM, BEG=114, LEN=1, VAR=lanes_EV, BEG=116, LEN=1, VAR=ML_lanes_EA, BEG=118, LEN=1, VAR=ML_lanes_AM, BEG=120, LEN=1, VAR=ML_lanes_MD, BEG=122, LEN=1, VAR=ML_lanes_PM, BEG=124, LEN=1, VAR=ML_lanes_EV, BEG=126, LEN=1, VAR=useclass_EA, BEG=128, LEN=1, VAR=useclass_AM, BEG=130, LEN=1, VAR=useclass_MD, BEG=132, LEN=1, VAR=useclass_PM, BEG=134, LEN=1, VAR=useclass_EV, BEG=136, LEN=1
FILEI NODEI[1] = %NODE_DATA_PATH%, VAR=osm_node_id(C12), BEG=1, LEN=12, VAR=county(C13), BEG=14, LEN=13, VAR=drive_access, BEG=28, LEN=1, VAR=walk_access, BEG=30, LEN=1, VAR=bike_access, BEG=32, LEN=1, VAR=N, BEG=34, LEN=7, VAR=rail_only, BEG=42, LEN=1, VAR=X, BEG=44, LEN=18, VAR=Y, BEG=63, LEN=18
FILEO NETO = "%SCENARIO_DIR%/complete_network.net" 

    ZONES = %zones% 

;ROADWAY = LTRIM(TRIM(ROADWAY)) 
;NAME = LTRIM(TRIM(NAME)) 

 
ENDRUN