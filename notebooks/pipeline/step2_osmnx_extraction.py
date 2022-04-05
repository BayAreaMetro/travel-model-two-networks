USAGE = """

Extracts complete OSM attributes using OSMNX.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Input: polygon boundary file for the region, [INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp
Output: nodes and links data from OSMNX in geojson format,
    [OUTPUT_DATA_DIR]/external/external/step2_osmnx_extraction/link.geojson,
    [OUTPUT_DATA_DIR]/external/external/step2_osmnx_extraction/node.geojson
"""
from methods import *
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging
from datetime import datetime

#####################################
# EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

#####################################
# inputs and outputs

INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
INPUT_POLYGON   = os.path.join(INPUT_DATA_DIR, 'external', 'step0_boundaries', 'cb_2018_us_county_5m_BayArea.shp')
OUTPUT_DIR      = os.path.join(OUTPUT_DATA_DIR, 'external', 'step2_osmnx_extraction')


if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        WranglerLogger.info('create output folder')
        os.makedirs(OUTPUT_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        OUTPUT_DIR,
        "step2_osmnx_extraction_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(LOG_FILENAME)

    # read polygon boundary
    county_polys_gdf = gpd.read_file(INPUT_POLYGON)
    WranglerLogger.info('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(county_polys_gdf.crs))

    # dissolve into one polygon
    WranglerLogger.info('dissolve into one polygon')
    boundary = county_polys_gdf.geometry.unary_union

    # OSM extraction
    WranglerLogger.info('starting osmnx extraction')
    G_drive = ox.graph_from_polygon(boundary, network_type='all', simplify=False)
    WranglerLogger.info('finished osmnx extraction')

    WranglerLogger.info('getting links and nodes from osmnx data')
    link_gdf = ox.graph_to_gdfs(G_drive, nodes=False, edges=True)
    WranglerLogger.info('link_gdf has {} records, with columns: \n{}'.format(
        link_gdf.shape[0], list(link_gdf)))
    node_gdf = ox.graph_to_gdfs(G_drive, nodes=True, edges=False)
    WranglerLogger.info('node_gdf has {} records, with columns: \n{}'.format(
        node_gdf.shape[0], list(node_gdf)))

    # writing out OSM link data to geojson
    WranglerLogger.info('writing out OSM links and nodes to geojson at {}'.format(OUTPUT_DIR))
    link_prop = link_gdf.drop("geometry", axis=1).columns.tolist()
    link_geojson = link_df_to_geojson(link_gdf, link_prop)
    with open(os.path.join(OUTPUT_DIR, 'link.geojson'), "w") as f:
        json.dump(link_geojson, f)

    # writing out OSM node data to geojson
    node_prop = node_gdf.drop("geometry", axis=1).columns.tolist()
    node_geojson = point_df_to_geojson(node_gdf, node_prop)
    with open(os.path.join(OUTPUT_DIR, 'node.geojson'), "w") as f:
        json.dump(node_geojson, f)

    WranglerLogger.info('finished writing out OSM links and nodes')
