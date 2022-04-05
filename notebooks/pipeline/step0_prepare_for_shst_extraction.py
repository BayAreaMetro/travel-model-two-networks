USAGE = """

Partitions the region geographically into 14 sub-regions. 
SharedStreets extraction requires a polygon boundary as input. 
Running extraction with the entire Bay Area as the boundary will run out of space.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Input:  [INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp: polygons of Bay Area counties
Output: [OUTPUT_DATA_DIR]/external/step0_boundaries/modified/boundary_[1-14].json
"""

from methods import *
import geopandas as gpd
import os
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging
from datetime import datetime

# input/output directory and files
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
INPUT_POLYGON   = os.path.join(INPUT_DATA_DIR, 'external', 'step0_boundaries', 'cb_2018_us_county_5m_BayArea.shp')
OUTPUT_DIR      = os.path.join(OUTPUT_DATA_DIR, 'external', 'step0_boundaries')

# EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        WranglerLogger.info('create output folder: {}'.format(OUTPUT_DIR))
        os.makedirs(OUTPUT_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        OUTPUT_DIR,
        "step0_prepare_for_shst_extraction_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(info_log_filename=LOG_FILENAME)

    # read polygon boundary
    county_polys_gdf = gpd.read_file(INPUT_POLYGON)

    WranglerLogger.info('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(county_polys_gdf.crs))
    WranglerLogger.debug('county_polys_gdf: {}'.format(county_polys_gdf))

    # export polygon to geojson for shst extraction
    for row_index, row in county_polys_gdf.iterrows():
        WranglerLogger.info('Exporting boundary file number: {}'.format(row_index+1))

        boundary_gdf = gpd.GeoDataFrame({"geometry": gpd.GeoSeries(row['geometry'])})

        boundary_gdf.to_file(os.path.join(OUTPUT_DIR, 'boundary_{}.geojson'.format(row_index+1)), driver="GeoJSON")
