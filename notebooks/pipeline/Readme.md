
## Process

### [Step 0: Prepare for SharedStreets extraction](step0_prepare_for_shst_extraction.ipynb)

Export county boundary polygons for SharedStreets Extraction.  Converts county shapefile to [WGS 84](https://spatialreference.org/ref/epsg/wgs-84/) and exports as geojson files.

* Input: County shapefile, `../../data/external/county_boundaries/county_5m%20-%20Copy.shp` -- Get this from [`BOX_TM2NET_DATA > external > county_boundaries > county_5m - Copy.shp`](https://mtcdrive.box.com/s/jj5grp9eso5r1ljbztwjid6znzrzc6g7)
* Output: County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`

### [Step 1: SharedStreets extraction](step1_shst_extraction.sh)

Use [Docker](https://www.docker.com/) to build an image as instructed by the [Dockerfile](Dockerfile).
See [sharedstreets-js docker documentation](https://github.com/sharedstreets/sharedstreets-js#docker), and extract SharedStreet networks data by the boundaries defined in step 0.

Installing Docker Desktop and getting Docker to run on an Mac machine is straightforward. Setting up Docker on a Windows machine requires BIOS configuration. Path referencing and line-ending format are also different in Mac versus Windows. See the inline comments for examples. 

* Input: 
  * [Dockerfile](github.com/BayAreaMetro/travel-model-two-networks/blob/develop/notebooks/pipeline/Dockerfile), used to build the shst image  
  * County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`
* Output: Shared Street extract, `../../data/external/sharedstreets_extract/mtc_[1-14].out.geojson`, log files with columns: 
   'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'roadClass', 'metadata', 'geometry'

See [SharedStreets Geometries](https://github.com/sharedstreets/sharedstreets-ref-system#sharedstreets-geometries)

### [Step 2: OSMnx extraction](step2_osmnx_extraction.ipynb)

Use OMNx to extract OSM data for the Bay Area and save as geojson files.

* Input:
  * County shapefile, `../../data/external/county_boundaries/county_5m%20-%20Copy.shp`
  * OpenStreetMap via [`osmnx.graph.graph_from_polygon()`](https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.graph.graph_from_polygon)
* Output:
  * OSM link extract, `../../data/external/osmnx_extract/link.geojson` with columns: 'osmid', 'oneway', 'lanes', 'ref', 'name', 'highway', 'maxspeed',
       'length', 'bridge', 'service', 'width', 'access', 'junction', 'tunnel', 'est_width', 'area', 'landuse', 'u', 'v', 'key', 'geometry'
  * OSM node extract, `../../data/external/osmnx_extract/node.geojson` with columns: 'y', 'x', 'osmid', 'ref', 'highway', 'geometry'

### [Step 3: Process SharedStreets Extraction to Network Standard and Conflate with OSM](step3_join_shst_extraction_with_osm.ipynb)

Add OSM attributes to extracted SharedStreets network and convert to Network Standard data formats. 

* Input:
  * OSM link extract, `../../data/external/osmnx_extract/link.geojson`
  * OSM node extract, `../../data/external/osmnx_extract/node.geojson`
  * Shared Street extract, `../../data/external/sharedstreets_extract/mtc_[1-14].out.geojson`
* Output:
  * Link shapes, `../../data/interim/step3_join_shst_extraction_with_osm/shape.geojson`, identified by these shst features: 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId'
  * Link attributes, `../../data/interim/step3_join_shst_extraction_with_osm/link.json`, with columns: 
  * Nodes, `../../data/interim/step3_join_shst_extraction_with_osm/node.geojson`, with columns:

### [Step 4: Conflate Third Party Data with Base Networks from Step 3](step4_conflate_with_third_party.ipynb)

Contains two parts:
Part 1 prepares third party data (remove duplicates, remove unnecessary records, partition regional network datasets by the 14 boundaries) for SharedStreets matching.
* Input:
  * TomTom network for the Bay Area (pending)
  * TM2 non-Marion version, `../../data/external/TM2_nonMarin/mtc_final_network_base.shp`
  * TM2 Marin version, `../../data/external/TM2_Marin/mtc_final_network_base.shp`
  * SFCTA Stick network, `../../data/external/sfcta/SanFrancisco_links.shp`
  * PEMS
* Output:
  * `../../data/external/tomtom/tomtom[1-14].in.geojson`
  * `../../data/external/TM2_nonMarin/tm2nonMarin_[1-14].in.geojson`
  * `../../data/external/TM2_Marin/tm2Marin_[1-14].in.geojson`
  * `../../data/external/sfcta/sfcta_in.geojson`
  * `../../data/external/mtc/pems.in.geojson`

After running Part 1, run [step4_conflate_with_third_party.sh](step4_conflate_with_third_party.sh) with Part 1's output as its input. This bash script matches these third party datasets to SharedStreets References using various rules. The output of SharedStreets References matching:
  * `../../data/interim/tomtom/bike_rules/[1-14]_tomtom.out.[matched,unmatched].geojson`
  * `../../data/interim/tomtom/car_rules/[1-14]_tomtom.out.[matched,unmatched].geojson`
  * `../../data/interim/tomtom/ped_rules/[1-14]_tomtom.out.[matched,unmatched].geojson`

  * `../../data/interim/TM2_nonMarin/car_rules/[1-14]_tm2nonMarin.out.[matched,unmatched].geojson`
  * `../../data/interim/TM2_nonMarin/ped_rules/[1-14]_tm2nonMarin.out.[matched,unmatched].geojson`
  * `../../data/interim/TM2_nonMarin/reverse_dir/[1-14]_tm2nonMarin.out.[matched,unmatched].geojson`

  * `../../data/interim/TM2_Marin/car_rules/[1-14]_tm2Marin.out.[matched,unmatched].geojson`
  * `../../data/interim/TM2_Marin/ped_rules/[1-14]_tm2Marin.out.[matched,unmatched].geojson`
  * `../../data/interim/TM2_Marin/reverse_dir/[1-14]_tm2Marin.out.[matched,unmatched].geojson`

  * `../../data/interim/sfcta/car_rules/sfcta.out.[matched,unmatched].geojson`
  * `../../data/interim/sfcta/ped_rules/sfcta.out.[matched,unmatched].geojson`
  * `../../data/interim/sfcta/reverse_dir/sfcta.out.[matched,unmatched].geojson`

Part 2 takes the output of `step4_conflate_with_third_party.sh` - only the 'matched' geojson files - and merge them with the base networks data created in Step 3.
* Output:
  * Link attributes, `../../data/interim/step4_conflate_with_tomtom/link.feather` and `../../data/interim/step4_conflate_with_tomtom/link.json`, with columns: 'access', 'area', 'bike_access', 'bridge', 'drive_access', 'est_width', 'fromIntersectionId', 'highway', 'id', 'junction', 'key', 'landuse', 'lanes', 'link', 'maxspeed', 'name', 'oneWay', 'ref', 'roadway', 'roundabout', 'service', 'shstGeometryId', 'shstReferenceId', 'toIntersectionId', 'tunnel', 'u', 'v', 'walk_access', 'wayId', 'width'
  * `../../data/interim/conflation_result.csv`


### [Step 5: Tidy Roadway](step5_tidy_roadway.ipynb)
Add county tagging to network links, shapes, and nodes; remove out-of-the-region links and nodes, drop circular links and duplicate links between same node pairs; flag drive dead-end; number nodes, links, and link AB nodes.

* Input:
  * Link shapes from Step 3, `../../data/interim/step3_join_shst_extraction_with_osm/shape.geojson`
  * Nodes from Step 3, `../../data/interim/step3_join_shst_extraction_with_osm/node.geojson`
  * Link attributes from Step 4 (has attributes from conflation), `../../data/interim/step4_conflate_with_tomtom/link.feather`
  * County shapefile, `../../data/external/county_boundaries/cb_2018_us_county_500k/cb_2018_us_county_500k.shp` -- Get this from [`BOX_TM2NET_DATA > external > county_boundaries > cb_2018_us_county_500k > cb_2018_us_county_500k.shp`](https://mtcdrive.box.com/s/sm86z4zol33l73oeufll881eabqecpnz)

* Output:
  * Link shapes, `../../data/interim/step5_tidy_roadway/shape.geojson`, with columns: 'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry', 'NAME'
  * Link attributes, `../../data/interim/step5_tidy_roadway/link.feather` and `../../data/interim/step5_tidy_roadway/link.json`, with columns: 'access', 'area', 'bike_access', 'bridge', 'drive_access', 'est_width', 'fromIntersectionId', 'highway', 'id', 'junction', 'key', 'landuse', 'lanes', 'link', 'maxspeed', 'name', 'oneWay', 'ref', 'roadway', 'roundabout', 'service', 'shstGeometryId', 'shstReferenceId', 'toIntersectionId', 'tunnel', 'u', 'v', 'walk_access', 'wayId', 'width', 'county', 'length', 'model_link_id', 'county_numbering_start', 'A', 'B'
  * Nodes, `../../data/interim/step5_tidy_roadway/node.geojson`, with columns: 'osm_node_id', 'shst_node_id', 'geometry', 'county', 'drive_access', 'walk_access', 'bike_access', 'model_node_id', 'county_numbering_start'