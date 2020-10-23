RUN PGM = NETWORK MSG = "Read in network from fixed width file" 
FILEI LINKI[1] = %LINK_DATA_PATH%, VAR=A, BEG=1, LEN=7, VAR=B, BEG=9, LEN=7, VAR=bike_access, BEG=17, LEN=1, VAR=county(C13), BEG=19, LEN=13, VAR=drive_access, BEG=33, LEN=1, VAR=managed, BEG=35, LEN=1, VAR=model_link_id, BEG=37, LEN=8, VAR=rail_only, BEG=46, LEN=1, VAR=segment_id, BEG=48, LEN=4, VAR=shstGeometryId(C32), BEG=53, LEN=32, VAR=walk_access, BEG=86, LEN=1, VAR=ft, BEG=88, LEN=2, VAR=assignable(C3), BEG=91, LEN=3, VAR=cntype(C5), BEG=95, LEN=5, VAR=transit, BEG=101, LEN=1, VAR=lanes_EA, BEG=103, LEN=2, VAR=lanes_AM, BEG=106, LEN=2, VAR=lanes_MD, BEG=109, LEN=2, VAR=lanes_PM, BEG=112, LEN=2, VAR=lanes_EV, BEG=115, LEN=2, VAR=ML_lanes_EA, BEG=118, LEN=1, VAR=ML_lanes_AM, BEG=120, LEN=1, VAR=ML_lanes_MD, BEG=122, LEN=1, VAR=ML_lanes_PM, BEG=124, LEN=1, VAR=ML_lanes_EV, BEG=126, LEN=1, VAR=useclass_EA, BEG=128, LEN=1, VAR=useclass_AM, BEG=130, LEN=1, VAR=useclass_MD, BEG=132, LEN=1, VAR=useclass_PM, BEG=134, LEN=1, VAR=useclass_EV, BEG=136, LEN=1
FILEI NODEI[1] = %NODE_DATA_PATH%, VAR=osm_node_id(C12), BEG=1, LEN=12, VAR=county(C13), BEG=14, LEN=13, VAR=drive_access, BEG=28, LEN=1, VAR=walk_access, BEG=30, LEN=1, VAR=bike_access, BEG=32, LEN=1, VAR=N, BEG=34, LEN=7, VAR=rail_only, BEG=42, LEN=1, VAR=X, BEG=44, LEN=18, VAR=Y, BEG=63, LEN=18
FILEO NETO = "%SCENARIO_DIR%/complete_network.net" 

    ZONES = %zones% 

;ROADWAY = LTRIM(TRIM(ROADWAY)) 
;NAME = LTRIM(TRIM(NAME)) 

 
ENDRUN