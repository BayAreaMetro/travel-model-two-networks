
<img src="https://github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/pipeline/TM2_network_rebuild_flow.png" width="800">

### [Step 0: Prepare for SharedStreets extraction](step0_prepare_for_shst_extraction.py)

Export county boundary polygons for SharedStreets Extraction.  Converts county shapefile to [WGS 84](https://spatialreference.org/ref/epsg/wgs-84/) and exports as geojson files.

#### Input:
* County/sub-county shapefile, based on [Census Cartographic Boundary File, cb_2018_us_county_5m.zip](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html), filtered to the Bay Area and with a few counties cut into smaller pieces, resulting in 14 rows: [`[INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp`](https://mtcdrive.box.com/s/mzxbqhysv1oqaomzvz5pd96g04q0mbs8)
#### Output:
* 14 county/sub-county boundaries, `[ROOT_OUTPUT_DATA_DIR]/external/step0_boundaries/boundary_[1-14].json`

### [Step 1: SharedStreets extraction](step1_shst_extraction.sh)

Use [Docker](https://www.docker.com/) to build an image as instructed by the [Dockerfile](Dockerfile).
See [sharedstreets-js docker documentation](https://github.com/sharedstreets/sharedstreets-js#docker), and extract SharedStreet networks data by the boundaries defined in step 0.

Installing Docker Desktop and getting Docker to run on an Mac machine is straightforward. Setting up Docker on a Windows machine requires BIOS configuration. Path referencing and line-ending format are also different in Mac versus Windows. See the inline comments for examples. 

#### Input:
* [Dockerfile](github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/pipeline/Dockerfile), used to build the shst image  
* 14 county/sub-county boundaries (Step 0 output), `[ROOT_OUTPUT_DATA_DIR]/external/step0_boundaries/boundary_[1-14].json`
#### Output: 
* Shared Street extract, `[ROOT_OUTPUT_DATA_DIR]/external/step1_shst_extracts/mtc_[1-14].out.geojson` with columns: 
   'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'roadClass', 'metadata', 'geometry'
* Shared Street extract logs: `[ROOT_OUTPUT_DATA_DIR]/external/step1_shst_extracts/mtc_[1-14].tiles.txt`

See [SharedStreets Geometries](https://github.com/sharedstreets/sharedstreets-ref-system#sharedstreets-geometries)

**Optional conversion to geopackage using [convert_geojson_to_geopackage.py](../../src/scripts/convert_geojson_to_geopackage.py)**:

`python "%ROOT_OUTPUT_DATA_DIR%\external\step1_shst_extracts" "%ROOT_OUTPUT_DATA_DIR\external\step1_shst_extracts\mtc_all_out.gpkg"`

### [Step 2: OSMnx extraction](step2_osmnx_extraction.py)

Use OMNx to extract OSM data for the Bay Area and save as geojson files.

#### Input:
* County shapefile, `[INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp`
* OpenStreetMap via [`osmnx.graph.graph_from_polygon()`](https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.graph.graph_from_polygon)
#### Output:
* OSM link extract, `[OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/link.geojson` with columns: 'osmid', 'oneway', 'lanes', 'ref', 'name', 'highway', 'maxspeed',
     'length', 'bridge', 'service', 'width', 'access', 'junction', 'tunnel', 'est_width', 'area', 'landuse', 'u', 'v', 'key', 'geometry'
* OSM node extract, `[OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/node.geojson` with columns: 'y', 'x', 'osmid', 'ref', 'highway', 'geometry'

### [Step 3: Process SharedStreets Extraction to Network Standard and Conflate with OSM](step3_join_shst_extraction_with_osm.py)

Add OSM attributes to extracted SharedStreets network and convert to Network Standard data formats. 

#### Input:
* OSM link extract (from step2), `[INPUT_DATA_DIR]/external/external/step2_osmnx_extraction/link.geojson`
* Shared Street extract (from step1), `[INPUT_DATA_DIR]/external/step1_shst_extraction/mtc_[1-14].out.geojson`
#### Output:
* Network Standard link shapes, `[OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_shape.geojson`, identified by these shst features: 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId'; with columns: 'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry'
* Network Standard link attributes, `[OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_link.json`, with columns: 'shstReferenceId', 'id', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'u', 'v', 'link', 'oneWay', 'roundabout', 'wayId', 'access', 'area', 'bridge', 'est_width', 'highway', 'junction', 'key', 'landuse', 'lanes', 'maxspeed', 'name', 'ref', 'service', 'tunnel', 'width', 'roadway', 'drive_access', 'walk_access', 'bike_access'
* Network Standard nodes, `[OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_node.geojson`, with columns: 'osm_node_id', 'shst_node_id', 'drive_access', 'walk_access', 'bike_access', 'geometry'

### [Step 4: Conflate Third Party Data with Base Networks from Step 3]
Four parts, run in sequence:

#### [step4a: Prepare third-party data for SharedStreet conflation.py](step4a_prepare_third_party_data_for_conflation.py)
Prepare third party data for SharedStreets matching, including: remove duplicates, remove no roadway links (e.g. centroid connectors), add missing links (e.g. add the other link for two-way links) partition regional network datasets by the 14 boundaries, set to the standard lat-lon EPSG 4326.
##### Input:
* TomTom network, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/TomTom networkFGDB/network2019/Network_region.gdb/mn_nw`
* TM2 non-Marion version, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/TM2_nonMarin/mtc_final_network_base.shp`
* TM2 Marin version, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/TM2_Marin/mtc_final_network_base.shp`
* SFCTA Stick network, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/sfcta/SanFrancisco_links.shp`
* ACTC network, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/actc_model/AlamedaCo_MASTER_20190410_no_cc.shp`
* CCTA network, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/ccta_model/ccta_2015_network/ccta_2015_network.shp`
* PEMS count, `[ROOT_INPUT_DATA_DIR]/external/step4a_third_party_data/raw/mtc/pems_period.csv`

##### Output:
Two sets of data for each third-party data source, one set only contains unique identifier for each link and link geometry (`[data_source]_[1-14].in.geojson`), which will be used in SharedStreet matching in step4b - using the entire dataset would substantially increase run time; the other set contains all link attributes (`[data_source]_raw.geojson`), which will be joined back to matched links in step4d.   
* TomTom network, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TomTom/tomtom[1-14].in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TomTom/tomtom_raw.geojson`
* TM2 non-Marion version, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TM2_nonMarin/tm2nonMarin_[1-14].in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TM2_nonMarin/tm2nonMarin_raw.geojson`
* TM2 Marin version, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TM2_Marin/tm2Marin_[1-14].in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/TM2_Marin/tm2Marin_raw.geojson`
* SFCTA Stick network, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/sfcta/sfcta_in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/sfcta/sfcta_raw.geojson`
* ACTC network, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/actc/actc_[1-14].in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/actc/actc_raw.geojson`
* CCTA network, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/ccta/ccta_[1-14].in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/ccta/ccta_raw.geojson`
* PEMS count, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/pems.in.geojson`, `[ROOT_OUTPUT_DATA_DIR]/external/step4a_third_party_data/modified/pems_raw.geojson`

#### [step4b_third_party_shst_match.sh](step4b_third_party_shst_match.sh)
Matche third party datasets to SharedStreets References using various rules.

##### Input: 
* Output of step4a

##### Output: 
Folder `[ROOT_OUTPUT_DATA_DIR]/interim/step4b_third_party_shst_match`, containing the following data and sub-folders. For each input geojson file, up to two files are created - one containing successfully matched links (`[]_matched.geojson`), the other containing links failed to match (`[]_unmatched.geojson`). 
* TOMTOM data:
  * `/TomTom/bike_rules/tomtom_[1-14].out.[matched,unmatched].geojson`
  * `/TomTom/car_rules/tomtom_[1-14].out.[matched,unmatched].geojson`
  * `/TomTom/ped_rules/tomtom_[1-14].out.[matched,unmatched].geojson`
* Legacy TM2_non-Marin data:
  * `/TM2_nonMarin/car_rules/tm2nonMarin_[1-14].out.[matched,unmatched].geojson`
  * `/TM2_nonMarin/ped_rules/tm2nonMarin_[1-14].out.[matched,unmatched].geojson`
  * `/TM2_nonMarin/reverse_dir/tm2nonMarin_[1-14].out.[matched,unmatched].geojson`
* Legacy TM2_Marin data:
  * `/TM2_Marin/car_rules/tm2Marin_[1-14].out.[matched,unmatched].geojson`
  * `/TM2_Marin/ped_rules/tm2Marin_[1-14].out.[matched,unmatched].geojson`
  * `/TM2_Marin/reverse_dir/tm2Marin_[1-14].out.[matched,unmatched].geojson`
* SFCTA stick network:
  * `/sfcta/car_rules/sfcta.out.[matched,unmatched].geojson`
  * `/sfcta/ped_rules/sfcta.out.[matched,unmatched].geojson`
  * `/sfcta/reverse_dir/sfcta.out.[matched,unmatched].geojson`
* ACTC model network:
  * `/actc/actc_[1-14].out.[matched,unmatched].geojson`
* CCTA model network:
  * `/ccta/ccta_[1-14].out.[matched,unmatched].geojson`
* PEMS:
  * `/pems/pems.out.matched.geojson`

#### [step4c_pems_conflation.ipynb](step4c_pems_conflation.ipynb)
Further conflate PEMS data based on shield number (e.g., I-80), direction (e.g., E), and FT.

* Input:
  * PEMs data, `../../data/external/mtc/pems_period.csv`
  * PEMs ShSt conflation result, `../../data/interim/mtc/pems.out.matched.geojson`
  * Base network links attributes and shapes (output of Step3), `../../data/interim/step3_join_shst_extraction_with_osm/shape.geojson` and `../../data/interim/step3_join_shst_extraction_with_osm/link.json` 
  * Tranit conflation result from step4d (only need 'tomtom_shieldnum' and 'tomtom_rtedir' info, so only tomtom conflation result), `../../data/interim/conflation_result.csv`
* Output:
  * PEMs conflation result, `../../data/interim/mtc/pems_conflation_result_new.geojson`. It is not added to the standard network; it is used in [validation](https://github.com/BayAreaMetro/travel-model-two-networks/blob/develop/src/scripts/make-roadway-assignment-viewer.Rmd)

#### [step4d_conflate_with_third_party.ipynb](step4d_conflate_with_third_party.ipynb)
Merge the SharedStreets match results in step4b with the base networks data created in Step 3.

* Input:
  * Base networks including links and shapes from step3
  * Third party data ShSt match results from step4b
  * PEMs conflation result from step4c 

* Output:
  * Network Standard link attributes, `../../data/interim/step4_conflate_with_tomtom/link.feather` and `../../data/interim/step4_conflate_with_tomtom/link.json`, with columns: 'shstReferenceId', 'id', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'u', 'v', 'link', 'oneWay', 'roundabout', 'wayId', 'access', 'area', 'bridge', 'est_width', 'highway', 'junction', 'key', 'landuse', 'lanes', 'maxspeed', 'name', 'ref', 'service', 'tunnel', 'width', 'roadway', 'drive_access', 'walk_access', 'bike_access'
  * `../../data/interim/conflation_result.csv`


### [Step 5: Tidy Roadway](step5_tidy_roadway.ipynb)
Add county tagging to network links, shapes, and nodes; remove out-of-the-region links and nodes, drop circular links and duplicate links between same node pairs; flag drive dead-end; number nodes, links, and link AB nodes.

* Input:
  * Network Standard link shapes from Step 3, `../../data/interim/step3_join_shst_extraction_with_osm/shape.geojson`
  * Network Standard nodes from Step 3, `../../data/interim/step3_join_shst_extraction_with_osm/node.geojson`
  * Network Standard link attributes from Step 4 (has attributes from conflation), `../../data/interim/step4_conflate_with_tomtom/link.feather`
  * County shapefile, `../../data/external/county_boundaries/cb_2018_us_county_500k/cb_2018_us_county_500k.shp` -- Get this from [`BOX_TM2NET_DATA > external > county_boundaries > cb_2018_us_county_500k > cb_2018_us_county_500k.shp`](https://mtcdrive.box.com/s/sm86z4zol33l73oeufll881eabqecpnz)

* Output:
  * Network Standard link shapes, `../../data/interim/step5_tidy_roadway/shape.geojson`, with columns: 'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry', 'NAME'
  * Network Standard link attributes, `../../data/interim/step5_tidy_roadway/link.feather` and `../../data/interim/step5_tidy_roadway/link.json`, with columns: 'shstReferenceId', 'id', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'u', 'v', 'link', 'oneWay', 'roundabout', 'wayId', 'access', 'area', 'bridge', 'est_width', 'highway', 'junction', 'key', 'landuse', 'lanes', 'maxspeed', 'name', 'ref', 'service', 'tunnel', 'width', 'roadway', 'drive_access', 'walk_access', 'bike_access', 'model_link_id', 'length',  'county', 'county_numbering_start', 'A', 'B'
  * Network Standard nodes, `../../data/interim/step5_tidy_roadway/node.geojson`, with columns: 'osm_node_id', 'shst_node_id', 'geometry', 'county', 'drive_access', 'walk_access', 'bike_access', 'model_node_id', 'county_numbering_start'


### Step 6: Conflate Transit GTFS Data with Roadway Network
Three parts, run in sequence:

#### [step6a_gtfs_shape_to_geojson_for_shst_js.ipynb](step6a_gtfs_shape_to_geojson_for_shst_js.ipynb)
Convert the 'shape' data from transit gtfs into geojson for SharedStreets conflation.

* Input: `../../data/external/gtfs/2015/[operator_name]/shapes.txt`

* Output: `../../data/external/gtfs/[operator_name].transit.geojson`, including the following operators: 'ACTransit_2015_8_14', 'Blue&Gold_gtfs_10_4_2017', 'Emeryville_2016_10_26', 'Fairfield_2015_10_14', 'GGTransit_2015_9_3', 'Marguerite_2016_10_10', 'MarinTransit_2015_8_31', 'MVGo_2016_10_26', 'petalumatransit-petaluma-ca-us__11_12_15', 'RioVista_2015_8_20', 'SamTrans_2015_8_20', 'SantaRosa_google_transit_08_28_15', 'SFMTA_2015_8_11', 'Soltrans_2016_5_20', 'SonomaCounty_2015_8_18', 'TriDelta-GTFS-2018-05-24_21-43-17', 'vacavillecitycoach-2020-ca-us', 'VTA_2015_8_27', 'westcat-ca-us_9_17_2015', 'Wheels_2016_7_13' 

#### [step6b_conflate_with_gtfs.sh](step6b_conflate_with_gtfs.sh)
Match transit gtfs shapes to SharedStreets network.

* Input: shapes in geojson format (output of step6a)
* Output: `../../data/interim/step6_gtfs/shst_match/[operator_name].out.matched.geojson`

#### [step6c_gtfs_transit_network_builder_v3](step6c_gtfs_transit_network_builder_v3)
Conflate transit gtfs data (including ShSt match results and other gtfs data) with roadway network. Also add rail walk access links.

* Input:
  * Network Standard link shapes, link attributes, and nodes from Step 5, `../../data/interim/step5_tidy_roadway/shape.geojson`, `../../data/interim/step5_tidy_roadway/link.feather`, `../../data/interim/step5_tidy_roadway/node.geojson`
  * GTFS raw data, in `../../data/external/gtfs/2015/`
  * ShSt match results (output of step6b), `../../data/interim/step6_gtfs/shst_match/[operator_name].out.matched.geojson`
  * GTFS to TM2 mode crosswalk, `../../data/interim/gtfs_to_tm2_mode_crosswalk.csv`
  * County shapefile, `../../data/external/county_boundaries/cb_2018_us_county_500k/cb_2018_us_county_500k.shp` [question mark]

* Output:
  * Transit standard files (*Final output for transit*), in `../../data/processed/version_12/`, including the following files: `routes.txt`, `shapes.txt`, `trips.txt`, `frequencies.txt`, `stops.txt`, `stop_times.txt`
  * Network Standard link shapes, link attributes, and nodes, `../../data/interim/step6_gtfs/version_12/shape.geojson`, `../../data/interim/step6_gtfs/version_12/link.feather`, `../../data/interim/step6_gtfs/version_12/node.geojson`
  * CUBE travel model transit network, `../../data/processed/version_12/transit.LIN`
  * consolidated gtfs input (mainly for QAQC), in `../../data/interim/step6_gtfs/consolidated_gtfs_input/`, including the following files: `routes.txt`, `trips.txt`, `stops.txt`, `shapes.txt`, `stop_times.txt`, `agency.txt`, `fare_attributes.txt`, `fare_rules.txt`
  * Tansit route true shape (for QAQC), `../../data/interim/step6_gtfs/transit_route.geojson`


### [Step7: Build Controid Connectors](step7_centroid_connector_builder)
Create TAZ/MAZ centroids and centroid connectors, which will be [added to the Standard networks](https://github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/10-all-projects.ipynb) along with TAPs and Project Cards to create model networks for Cube/Emme.

* Input:
  * Network Standard link shapes, link attributes, and nodes from Step 6, at `../../data/interim/step6_gtfs/version_12/`
  * TAZ and MAZ polygons, `../data/external/maz_taz/tazs_TM2_v2_2.shp` and `../data/external/maz_taz/mazs_TM2_v2_2.shp`
  * Assignable links lookup, when building the network for the first time, the legacy lookup was used, [`\GitHub\Lasso\mtc_data\lookups\legacy_tm2_attributes.csv`](https://github.com/BayAreaMetro/Lasso/blob/mtc_parameters/mtc_data/lookups/legacy_tm2_attributes.csv); as the network was updated and assignable calculations improved, newer versions of lookup would be used, e.g. `../../data/processed/version_05/assignable_analysis_links.dbf`
  * Centroid Connectors of legacy TM2 network (non-Marin version), `../../data/external/TM2_nonMarin/tm2_links.shp`, `../../data/external/TM2_nonMarin/tm2_nodes.shp`

* Output (at `../../data/interim/step7_centroid_connector/`; key files also saved at the [Lasso repo](https://github.com/BayAreaMetro/Lasso/tree/mtc_parameters/mtc_data/centroid), to be used to create model networks):
  * Shapes of all centroid connectors, including TAZ drive connector, MAZ drive, walk, bike connectors, in two formats, `cc_shape.geojson`, `cc_shape.pickle`
  * Link variables of all centroid connectors, in two formats, `cc_link.json`, `cc_link.pickle`
  * All centroid nodes, `centroid_node.geojson`, `centroid_node.pickle`

  * Output by centroids and connectors types, mainly for QAQC (at `../../data/interim/step7_centroid_connector/`): 
    * TAZ centroids, `taz_drive_centroid.pickle`
    * TAZ drive centroid connectors, `taz_drive_cc.pickle` and `../../data/interim/step7_centroid_connector/taz_drive.geojson`, note that it only contains one-direction (one link for each connector shape)
    * TAZ drive centroid connector shapes, `taz.geojson`
    * MAZ drive centroids, `maz_drive_centroid.pickle`
    * MAZ drive centroid connectors, `maz_drive_cc.pickle`, note that it only contains one-direction (one link for each connector shape)
    * MAZ drive centroid connector shapes, `maz_drive.geojson`
    * MAZ walk centroids, `maz_walk_centroid.pickle`
    * MAZ walk centroid connectors, `maz_walk_cc.pickle`, note that it only contains one-direction (one link for each connector shape)
    * MAZ walk centroid connector shapes, `maz_walk.geojson`
    * MAZ bikew centroids, `maz_bike_centroid.pickle`
    * MAZ bike centroid connectors, `maz_bike_cc.pickle`, note that it only contains one-direction (one link for each connector shape)
    * MAZ bike centroid connector shapes, `maz_bike.geojson`


### [Step8: Standard Format](step8_standard_format)
Add necessary fields to the base netowrk.

* Input: Network Standard link shapes, link attributes, and nodes from Step 6, at `../../data/interim/step6_gtfs/version_12/`, including: 
  * `link.feather`
  * `node.geojson` 
  * `shape.geojson`

* Output: Network Standard link shapes, link attributes, and nodes (*Final output for standard roadway networks plus transit nodes and links*), at `../../data/interim/step8_standard_format/`, including:
  * `shape.geojson`, with columns: 
  * `link.json` and `link.feather`, with columns: 'access', 'bike_access', 'drive_access', 'fromIntersectionId', 'lanes', 'maxspeed', 'name', 'oneWay', 'ref', 'roadway', 'shstGeometryId', 'shstReferenceId', 'toIntersectionId', 'u', 'v', 'walk_access', 'wayId', 'county', 'model_link_id', 'A', 'B', 'rail_traveltime', 'rail_only', 'locationReferences'
  * `node.geojson`, with columns: 'osm_node_id', 'shst_node_id', 'county', 'drive_access', 'walk_access', 'bike_access', 'model_node_id', 'rail_only', 'geometry', 'X', 'Y', 'point'


### [Step9: Create TAPS](step9_create_taps)
Create TAPs and TAP links,  which will be [added to the Standard networks](https://github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/10-all-projects.ipynb) to create model networks for Cube/Emme.

* Input:
  * Lasso scenario, same data as the standard network output from Step8, just faster to load in pickle format, `../../data/processed/version_03/working_scenario_01.pickle`, created by https://github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/01-attribute-and-make-pickles.ipynb
  * Lasso parameters, [`/GitHub/Lasso`, "mtc_parameters" branch](https://github.com/BayAreaMetro/Lasso/tree/mtc_parameters)
  * Legacy TM2 network (non-Marin version), `../../data/external/TM2_nonMarin/tm2_links.shp`, `../../data/external/TM2_nonMarin/tm2_nodes.shp`
  * County shapefile, `../../data/external/county_boundaries/cb_2018_us_county_500k/cb_2018_us_county_500k.shp`

* Output: (at `../../data/interim/step9_taps/`; key files also saved at the [Lasso repo](https://github.com/BayAreaMetro/Lasso/tree/mtc_parameters/mtc_data/tap), to be used to create model networks):
  * TAP link attributes, `tap_link.pickle`
  * TAP link shapes, `tap_shape.pickle`
  * TAP nodes, `tap_node.pickle`
  * Taps and tap links mainly for analyze the different options and QAQC, `stops_and_taps.csv`
  * `Tap_node.csv`, which includes available mode, county, etc. for TAPs. This file was only requested by Shimon to update TAP coding in the onboard survey.
