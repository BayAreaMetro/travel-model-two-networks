USAGE = """
Conflate GTFS data to SharedStreet roadway links.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: 
Outputs:   

"""
# TODO: step6a can potentially be integrated into step6b instead of being a separate script.
# It also reduces the number of shapes needed to run shst match, because step6b only uses 
# "representative trips" in GTFS data. But fortunately the number of transit shapes needed
# for shst match is not that huge, so performance is ok with the entire dataset.

import geopandas as gpd
import geofeather
import geojson
from geojson import Feature, FeatureCollection
import json
from shapely.geometry import Point, LineString
import pandas as pd
import os, datetime
import methods
from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# 2015 GTFS data input
GTFS_INPUT_DIR = os.path.join(INPUT_DATA_DIR, 'step6_gtfs', '2015_input')

# sub-region boundary polygons to split third-party data
BOUNDARY_DIR = os.path.join(INPUT_DATA_DIR, 'step0_boundaries')

# output
GTFS_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, 'step6_gtfs')
CONFLATION_SHST = os.path.join(GTFS_OUTPUT_DIR, 'conflation_shst')

def conflate_gfts_shape(gtfs_shape_file, gtfs_raw_name):
    """
    Use the 'shapes.txt' file in GTFS data to match non-rail and non-ferry transit lines to sharedstreest network.

    """

    # first, convert "shapes.txt" in the GTFS data to transit line geodataframe
    WranglerLogger.info('try converting shapes.txt to transit line geodataframe')  
    line_gdf = methods.gtfs_point_shapes_to_link_gdf(gtfs_shape_file)

    if line_gdf is not None:
        # second, conflate
        # note: sharedstreet conflation cannot work with space in input/output file names, therefore, use
        # the raw gtfs feed name, which is the same as 'agency_raw_name' in the GTFS data, instead of the
        # shorter 'agency_name' which has spaces
        (matched_gdf, unmatched_gdf) = methods.conflate(
            gtfs_raw_name, line_gdf, ['shape_id'], 'transit',
            GTFS_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)


if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(CONFLATION_SHST):
        print('creating output folde {}'.format(CONFLATION_SHST))
        os.makedirs(CONFLATION_SHST)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        CONFLATION_SHST,
        "step6a_gtfs_shape_shst_match_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    gtfs_raw_name_ls = os.listdir(GTFS_INPUT_DIR)
    WranglerLogger.info('Available GTFS data sets: {}'.format(gtfs_raw_name_ls))

    for gtfs_raw_name in gtfs_raw_name_ls:
        # check if all GTFS data is in 'gtfs_name_dict'; there might be gtfs data in the folder that is not
        # needed for conflation, so just 'warning', not 'error'
        if gtfs_raw_name not in methods.gtfs_name_dict:
            WranglerLogger.debug('{} not in gtfs_name_dict, skip!'.format(gtfs_raw_name))
        
        else:
            # recode the long GTFS feed name into a shorter name
            gtfs_name = methods.gtfs_name_dict[gtfs_raw_name]

            # rails and ferries do not use roadway network, skip them for sharedstreets match
            if gtfs_name in methods.rail_gtfs:
                WranglerLogger.info('skiping rail: {}'.format(gtfs_name))
            elif gtfs_name in methods.ferry_gtfs:
                WranglerLogger.info('skiping ferry: {}'.format(gtfs_name))
            
            # run sharedstreets match for non-rail and non-ferry transit lines
            else:
                WranglerLogger.info('conflating {} with GTFS data: {}'.format(gtfs_name, gtfs_raw_name))

                # first, create a line geodataframe from 'shapes.txt' in GTFS data, which is 
                #  to match non-rail and non-ferry transit lines to sharedstreest network
                gtfs_shape_file = os.path.join(GTFS_INPUT_DIR, gtfs_raw_name, 'shapes.txt')

                if not os.path.exists(gtfs_shape_file):
                    WranglerLogger.warning('shapes.txt does not exist!')
                
                else:
                    try:
                        gdf = conflate_gfts_shape(gtfs_shape_file, gtfs_raw_name)
                    except:
                        WranglerLogger.warning('cannot conflate {} !'.format(gtfs_raw_name))
