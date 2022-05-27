USAGE = """

Extracts complete OSM attributes using OSMNX and saves to disk

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable

Input: 
 - polygon boundary file for the region: 
   [INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp
Output: 
 - Node and link data in geofeather format for subsequent steps, as this format is fast to read/write: 
   [OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/[node,link].feather[.crs]: 
 - Node and link data in geopackage format for visualization; layers are link & node:
   [OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/osmnx_extraction.gpkg: 
  
   Data is fetched using osmnx.graph.graph_from_polygon() using simplify=False,
   so there are typically multiple links per OSM way.  (This is because if simplify is True, 
   OSMNx will aggregate some OSM ways into a single link, which we don't want; see a nice explanation 
   of this process in OSMnx: Python for Street Networks: https://geoffboeing.com/2016/11/osmnx-python-street-networks/)

   Link data includes columns:
     * osmid: the OpenStreetMap way ID (e.g. 619590730 for this link: https://www.openstreetmap.org/way/619590730)
     * geometry: the shape of the link
     * length: the length of the link, in meters; this is added by OSMNx
     * u, v: the OSM node ID of the start and end of the link.  However, OSMNx seems to still be doing some simplification 
       under the hood, so these are often 0 rather than corresponding the the OSM values. For example, for OSM way 619590730 referenced above
       (https://www.openstreetmap.org/way/619590730), the way is split into two links, and only one has the the u value, 
       1723738865 (https://www.openstreetmap.org/node/1723738865); the rest are zeros.
     * key: this is an OSMNx identifier for when multiple parallel links exist with the same u,v; key then distinguishes between them so
       that (u,v,key) is unique.  I believe this is only relevant if `simplify=True`, however, as there are many links with (u,v,key)=(0,0,0)
     * all the other columns are specified in methods.OSM_WAY_TAGS

  Node data include columns:
     * osmid: the OpenStreetMap node ID (e.g. 1723738865 for https://www.openstreetmap.org/node/1723738865).  However,
       this is often missing and set to 0 even when real node IDs exist; see the discussion on the u,v columns in the link dataset above.
       **Because of this, this dataset isn't used subsequently.**
     * y, x: latitude and longitude of the node
     * some of the tags in methods.OSM_WAY_TAGS also have corresponding data for nodes so they're included here.  For the
       set of tags we're using, this looks like it's only 'highway' and 'ref'

"""
import datetime, os, sys
import methods
import pandas as pd
import geopandas as gpd
import osmnx as ox
import geofeather
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging

#####################################
# EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

#####################################
# inputs and outputs

INPUT_DATA_DIR      = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR     = os.environ['OUTPUT_DATA_DIR']
INPUT_POLYGON       = os.path.join(INPUT_DATA_DIR,  'step0_boundaries', 'San_Francisco_Bay_Region_Counties.shp')
OUTPUT_DIR          = os.path.join(OUTPUT_DATA_DIR, 'step2_osmnx_extracts')
OUTPUT_FEATHER_LINK = os.path.join(OUTPUT_DIR, "link.feather")
OUTPUT_FEATHER_NODE = os.path.join(OUTPUT_DIR, "node.feather")
OUTPUT_GPKG         = os.path.join(OUTPUT_DIR, "osmnx_extracts.gpkg")




if __name__ == '__main__':
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    
    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        # need to print since logger isn't setup yet
        print('creating output folder {}'.format(OUTPUT_DIR))
        os.makedirs(OUTPUT_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        OUTPUT_DIR,
        "step2_osmnx_extraction_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME)

    # read polygon boundary
    county_polys_gdf = gpd.read_file(INPUT_POLYGON)
    WranglerLogger.info('Input county boundary file {} uses projection: {}'.format(INPUT_POLYGON, county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(county_polys_gdf.crs))

    # dissolve into one polygon
    boundary = county_polys_gdf.geometry.unary_union
    WranglerLogger.info('dissolved into one polygon')

    # Request specific way tags from OSM
    WranglerLogger.info("Requesting the following way tags from OSM: {}".format(methods.OSM_WAY_TAGS.keys()))
    ox.utils.config(useful_tags_way=methods.OSM_WAY_TAGS.keys())

    # OSM extraction - Note: this is memory intensive (~15GB) and time-consuming (~50 min)
    WranglerLogger.info('starting osmnx extraction')
    osmnx_graph = ox.graph_from_polygon(boundary, network_type='all', simplify=False)
    WranglerLogger.info('finished osmnx extraction')

    # these are very large datasets to do links first and then delete, then nodes
    WranglerLogger.info('getting links from osmnx data')
    link_gdf = ox.graph_to_gdfs(osmnx_graph, nodes=False, edges=True)
    WranglerLogger.info('link_gdf has {} records, with dtypes: \n{}'.format(
        link_gdf.shape[0], link_gdf.dtypes))
    WranglerLogger.info('link_gdf head: \n{}'.format(link_gdf.head()))
    link_gdf.reset_index(drop=False, inplace=True)  # geofeather requires reset index
  
    # warn if reversed column isn't present
    # added in osmnx-1.2.0 (https://github.com/gboeing/osmnx/blob/main/CHANGELOG.md)
    if 'reversed' not in list(link_gdf.columns):
        WranglerLogger.warn('Column reversed not found in link_gdf; this was added in osmnx-1.2.0')

    # report value_types for ordinal columns
    for column in link_gdf.select_dtypes([object]):
        WranglerLogger.info("column {} value_counts:\n{}".format(column, link_gdf[column].value_counts()))

    # write links to geopackage
    WranglerLogger.info('writing out OSM links to gpkg at {}'.format(OUTPUT_DIR))
    link_gdf.to_file(OUTPUT_GPKG, layer="link", driver="GPKG")
    # write links to geofeather
    WranglerLogger.info('writing out OSM links to geofeather at {}'.format(OUTPUT_FEATHER_LINK))
    geofeather.to_geofeather(link_gdf, OUTPUT_FEATHER_LINK)
    del link_gdf
    WranglerLogger.info('link objects deleted')

    # pull the osmnx nodes
    WranglerLogger.info('getting nodes from osmnx data')
    node_gdf = ox.graph_to_gdfs(osmnx_graph, nodes=True, edges=False)
    WranglerLogger.info('node_gdf has {} records, with dtypes: \n{}'.format(
        node_gdf.shape[0], node_gdf.dtypes))
    WranglerLogger.info('node_gdf head: \n{}'.format(node_gdf.head()))
    node_gdf.reset_index(drop=False, inplace=True) # geofeather requires reset index

    # report value_types for ordinal columns
    for column in node_gdf.select_dtypes([object]):
        WranglerLogger.info("column {} value_counts:\n{}".format(column, node_gdf[column].value_counts()))

    # writing out OSM node data to geopackage
    WranglerLogger.info('writing out OSM nodes to gpkg at {}'.format(OUTPUT_DIR))
    node_gdf.to_file(OUTPUT_GPKG, layer="node", driver="GPKG")
    # write nodes to geofeather
    WranglerLogger.info('writing out OSM nodes to geofeather at {}'.format(OUTPUT_FEATHER_NODE))
    geofeather.to_geofeather(node_gdf, OUTPUT_FEATHER_NODE)

    del node_gdf
    WranglerLogger.info('node objects deleted')

    WranglerLogger.info('finished writing out OSM links and nodes')
