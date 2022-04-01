USAGE = """

Partitions the region geographically into 14 sub-regions. 
SharedStreets extraction requires a polygon boundary as input. 
Running extraction with the entire Bay Area as the boundary will run out of space.

Input: county_5m - Copy.shp: polygons of Bay Area counties
Output: 14 geojson files
"""

from methods import *
import geopandas as gpd
import os
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging
from datetime import datetime

# input/output directory and files
ROOT_DATA_DIR = 'C:\\Users\\{}\\Box\\Modeling and Surveys\\Development\\Travel Model Two Development\\Build_TM2_networkV13_pipeline'.format(os.getenv('USERNAME'))
INPUT_DIR = os.path.join(ROOT_DATA_DIR, 'external', 'step0_boundaries')
COUNTY_POLYGON = 'county_5m - Copy.shp'
OUTPUT_DIR = os.path.join(ROOT_DATA_DIR, 'external', 'step0_boundaries', 'modified')

# # EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(LAT_LONG_EPSG))

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
    setupLogging(LOG_FILENAME)

    # read polygon boundary
    county_polys_gdf = gpd.read_file(os.path.join(INPUT_DIR, COUNTY_POLYGON))

    WranglerLogger.info('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(county_polys_gdf.crs))

    # export polygon to geojson for shst extraction
    i = 1
    for g in county_polys_gdf.geometry:
        WranglerLogger.info('exporting boundary file number: ' + str(i))

        boundary_gdf = gpd.GeoDataFrame({"geometry": gpd.GeoSeries(g)})

        boundary_gdf.to_file(os.path.join(OUTPUT_DIR, 'boundary_' + str(i) + '.geojson'),
                             driver="GeoJSON")
        i += 1
