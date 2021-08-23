
## Process

### [Step 0: Prepare for SharedStreets extraction](step0_prepare_for_shst_extraction.ipynb)

Export county boundary polygons for SharedStreets Extraction.  Converts county shapefile to [WGS 84](https://spatialreference.org/ref/epsg/wgs-84/) and exports as geojson files.

* Input: County shapefile, `../../data/external/county_boundaries/county_5m%20-%20Copy.shp` -- Get this from [`BOX_TM2NET_DATA > external > county_boundaries > county_5m - Copy.shp](https://mtcdrive.box.com/s/jj5grp9eso5r1ljbztwjid6znzrzc6g7)
* Output: County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`

### [Step 1: SharedStreets extraction](step1_shst_extraction.sh)

This step uses [Docker](https://www.docker.com/) to build an image as instructed by the [Dockerfile](Dockerfile).
See [sharedstreets-js docker documentation](https://github.com/sharedstreets/sharedstreets-js#docker).

I had trouble getting Docker to run on my Windows machine because of a BIOS update problem as well as the line-endings in the file (although I ultimately succeeded); it was easy on my Mac.

* Input: County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`
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

### [Step 3: Process SharedStreets Extraction to Network Standard and Conflate with OSM, TomTom](step3_join_shst_extraction_with_osm.ipynb)

* Input:
  * OSM link extract, `../../data/external/osmnx_extract/link.geojson`
  * OSM node extract, `../../data/external/osmnx_extract/node.geojson`
  * Shared Street extract, `../../data/external/sharedstreets_extract/mtc_[1-14].out.geojson`