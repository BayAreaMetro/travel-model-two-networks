
## Steps

* [Step 0: Prepare for SharedStreets extraction](step0_prepare_for_shst_extraction.ipynb)

Export county boundary polygons for SharedStreets Extraction.  Converts county shapefile to [WGS 84](https://spatialreference.org/ref/epsg/wgs-84/) and exports as geojson files.

Input: [County shapefile](../../data/external/county_boundaries/county_5m - Copy.shp)
Output: County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`

* [Step 1: SharedStreets extraction](step1_shst_extraction)

This step uses [Docker](https://www.docker.com/) to build an image as instructed by the [Dockerfile](Dockerfile).
See [sharedstreets-js docker documentation](https://github.com/sharedstreets/sharedstreets-js#docker).

I had trouble getting Docker to run on my Windows machine because of a BIOS update problem, but it was easy on my Mac.

Input: County boundaries, `../../data/external/county_boundaries/boundary_[1-14].json`
Output: Shared streets geojson files `[1-14].out.geojson`, log files

* [Step 2: OSMnx SharedStreets extraction](step2_osmnx_extraction.ipynb)

TBD

Input:
  * [County shapefile](../../data/external/county_boundaries/county_5m - Copy.shp)
  * OpenStreetMap via [`osmnx.graph.graph_from_polygon()`](https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.graph.graph_from_polygon)

Output:
