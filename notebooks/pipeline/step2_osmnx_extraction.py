USAGE = """

Extracts complete OSM attributes using OSMNX and saves to disk

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable

Input: 
 - polygon boundary file for the region: 
   [INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp
Output: 
 - nodes and links data geofeather format for subsequent steps, as this format is fast to read/write: 
   [OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/[node,link].feather[.crs]: 
 - nodes and links data in geopackage format for visualization; layers are link & node:
   [OUTPUT_DATA_DIR]/external/step2_osmnx_extraction/osmnx_extraction.gpkg: 

"""
import datetime, os, sys
import methods
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
INPUT_POLYGON       = os.path.join(INPUT_DATA_DIR,  'external', 'step0_boundaries', 'cb_2018_us_county_5m_BayArea.shp')
OUTPUT_DIR          = os.path.join(OUTPUT_DATA_DIR, 'external', 'step2_osmnx_extracts')
OUTPUT_FEATHER_LINK = os.path.join(OUTPUT_DIR, "link.feather")
OUTPUT_FEATHER_NODE = os.path.join(OUTPUT_DIR, "node.feather")
OUTPUT_GPKG         = os.path.join(OUTPUT_DIR, "osmnx_extracts.gpkg")


# way (link) tags we want from OSM
# OSM Defaults are viewable here: https://osmnx.readthedocs.io/en/stable/osmnx.html?highlight=util.config#osmnx.utils.config
# as useful_tags_way
WAY_TAGS = [
    'highway',          # https://wiki.openstreetmap.org/wiki/Key:highway
    'tunnel',           # https://wiki.openstreetmap.org/wiki/Key:tunnel
    'bridge',           # https://wiki.openstreetmap.org/wiki/Key:bridge
    'junction',         # https://wiki.openstreetmap.org/wiki/Key:junction
    'oneway',           # https://wiki.openstreetmap.org/wiki/Key:oneway
    'name',             # https://wiki.openstreetmap.org/wiki/Key:name
    'ref',              # https://wiki.openstreetmap.org/wiki/Key:ref
    'width',            # https://wiki.openstreetmap.org/wiki/Key:width
    'est_width',        # https://wiki.openstreetmap.org/wiki/Key:est_width
    'access',           # https://wiki.openstreetmap.org/wiki/Key:access
    'area',             # https://wiki.openstreetmap.org/wiki/Key:area
    'service',          # https://wiki.openstreetmap.org/wiki/Key:service
    'maxspeed',         # https://wiki.openstreetmap.org/wiki/Key:maxspeed
    # lanes accounting
    'lanes',            # https://wiki.openstreetmap.org/wiki/Key:lanes
    'bus',              # https://wiki.openstreetmap.org/wiki/Key:bus
    'hov',              # https://wiki.openstreetmap.org/wiki/Key:hov
    'taxi',             # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:taxi',       # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:hov',        # https://wiki.openstreetmap.org/wiki/Key:hov
    'shoulder',         # https://wiki.openstreetmap.org/wiki/Key:shoulder
    'lanes:bus',        # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:backward',   # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:forward',    # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:both_ways',  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'turn',             # https://wiki.openstreetmap.org/wiki/Key:turn
    'turn:lanes',       # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    # active modes
    'sidewalk',         # https://wiki.openstreetmap.org/wiki/Key:sidewalk
    'cycleway',         # https://wiki.openstreetmap.org/wiki/Key:cycleway
]

if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        # need to print since logger isn't setup yet
        print('creating output folder {}'.format(OUTPUT_DIR))
        os.makedirs(OUTPUT_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        OUTPUT_DIR,
        "step2_osmnx_extraction_{}.info.log".format(datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
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
    WranglerLogger.info("Requesting the following way tags from OSM: {}".format(WAY_TAGS))
    ox.utils.config(useful_tags_way=WAY_TAGS)

    # OSM extraction - Note: this is memory intensive (~15GB) and time-consuming (~50 min)
    WranglerLogger.info('starting osmnx extraction')
    osmnx_graph = ox.graph_from_polygon(boundary, network_type='all', simplify=False)
    WranglerLogger.info('finished osmnx extraction')

    # these are very large datasets to do links first and then delete, then nodes
    WranglerLogger.info('getting links from osmnx data')
    link_gdf = ox.graph_to_gdfs(osmnx_graph, nodes=False, edges=True)
    WranglerLogger.info('link_gdf has {} records, with columns: \n{}'.format(
        link_gdf.shape[0], list(link_gdf)))
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

    # writing out OSM node data to geojson
    WranglerLogger.info('getting nodes from osmnx data')
    node_gdf = ox.graph_to_gdfs(osmnx_graph, nodes=True, edges=False)
    node_gdf.reset_index(inplace=True) # geofeather requires reset index
    WranglerLogger.info('node_gdf has {} records, with columns: \n{}'.format(
        node_gdf.shape[0], list(node_gdf)))
    # report value_types for ordinal columns
    for column in node_gdf.select_dtypes([object]):
        WranglerLogger.info("column {} value_counts:\n{}".format(column, node_gdf[column].value_counts()))

    # writing out OSM node data to geopackage
    WranglerLogger.info('writing out OSM links to gpkg at {}'.format(OUTPUT_DIR))
    node_gdf.to_file(OUTPUT_GPKG, layer="node", driver="GPKG")
    # write nodes to geofeather
    WranglerLogger.info('writing out OSM nodes to geofeather at {}'.format(OUTPUT_FEATHER_NODE))
    geofeather.to_geofeather(node_gdf, OUTPUT_FEATHER_NODE)

    del node_gdf
    WranglerLogger.info('node objects deleted')

    WranglerLogger.info('finished writing out OSM links and nodes')
