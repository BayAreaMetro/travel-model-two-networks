USAGE = """
Conflate GTFS data ares third-party data for SharedStreet conflation.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: 
Outputs:   

"""


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
GTFS_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, 'step6_gtfs', 'output')
CONFLATION_SHST = os.path.join(GTFS_OUTPUT_DIR, 'conflation_shst')

# some settings on GTFS data sources
gtfs_name_dict = {
    'ACE_2017_3_20'                                   : 'ACT',
    'ACTransit_2015_8_14'                             : 'AC_Transit', 
    'BART_2015_8_3'                                   : 'BART', 
    'Blue&Gold_gtfs_10_4_2017'                        : 'Blue_Gold', 
    'Caltrain_2015_5_13'                              : 'Caltrain', 
    'Capitol_2017_3_20'                               : 'Capitol_Corridor', 
    'CCTA_2015_8_11'                                  : 'CCTA', 
    'commuteDOTorg_GTFSImportExport_20160127_final_mj': 'Caltrain_shuttle', #? 
    'Emeryville_2016_10_26'                           : 'EmeryGo', 
    # 'Fairfield_2015_10_14'                            : 'Fairfield Suisun', 
    'Fairfield_2015_10_14_updates'                    : 'Fairfield_Suisun',
    'GGFerries_2017_3_18': 'GG_Ferries', 
    'GGTransit_2015_9_3': 'GG_Transit', 
    'Marguerite_2016_10_10': 'Marguerite', 
    'MarinTransit_2015_8_31': 'Marin_Transit', 
    'MVGo_2016_10_26': 'MVGo', 
    'petalumatransit-petaluma-ca-us__11_12_15': 'Petaluma_Transit', 
    # 'Petaluma_2016_5_22': 'Petaluma_Transit',
    'RioVista_2015_8_20': 'Rio_Vista', 
    'SamTrans_2015_8_20': 'SamTrans', 
    'SantaRosa_google_transit_08_28_15': 'Santa_Rosa_CityBus', 
    'SFMTA_2015_8_11': 'SFMTA', 
    'SF_Bay_Ferry2016_07_01': 'SF_Bay_Ferry', 
    'Soltrans_2016_5_20': 'SolTrans', 
    'SonomaCounty_2015_8_18': 'Sonoma_County_Transit', 
    'TriDelta-GTFS-2018-05-24_21-43-17': 'Tri_Delta_Transit',
    'Union_City_Transit_Aug-01-2015 to Jun-30-2017': 'Union_City_Transit', 
    'vacavillecitycoach-2020-ca-us': 'Vacaville_City_Coach', 
    'Vine_GTFS_PLUS_2015': 'Vine', 
    'VTA_2015_8_27': 'VTA', 
    'westcat-ca-us_9_17_2015': 'WestCat', 
    # 'WestCAT_2016_5_26': 'WestCat',
    'Wheels_2016_7_13': 'Wheels'
}

rail_gtfs = ['BART', 'Caltrain', 'Capitol_Corridor']
ferry_gtfs = ['GG_Ferries', 'SF_Bay_Ferry']


def conflate_gfts_shape(gtfs_shape_file, gtfs_name):
    """
    Use the 'shapes.txt' file in GTFS data to match non-rail and non-ferry transit lines to sharedstreest network.

    """

    # first, convert "shapes.txt" in the GTFS data to transit line geodataframe
    WranglerLogger.info('try converting shapes.txt to transit line geodataframe')  
    line_gdf = methods.gtfs_point_shapes_to_link_gdf(gtfs_shape_file, gtfs_name)

    if line_gdf is not None:

        # # export the line geodataframe for shst match QA/QC
        # OUTPUT_FILE= os.path.join(CONFLATION_SHST, '{}_line.feather'.format(gtfs_name))
        # WranglerLogger.info('export {:,} rows of line_gdf to {}'.format(len(line_gdf), OUTPUT_FILE))
        # geofeather.to_geofeather(line_gdf, OUTPUT_FILE)

        # second, conflate
        (matched_gdf, unmatched_gdf) = methods.conflate(
            gtfs_name, line_gdf, ['shape_id'], 'transit',
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
        if gtfs_raw_name not in gtfs_name_dict:
            WranglerLogger.warning('{} not in gtfs_name_dict, please check!'.format(gtfs_raw_name))
        
        else:
            # recode the long GTFS feed name into a shorter name
            gtfs_name = gtfs_name_dict[gtfs_raw_name]

            # rails and ferries do not use roadway network, skip them for sharedstreets match
            if gtfs_name in rail_gtfs:
                WranglerLogger.info('skiping rail: {}'.format(gtfs_name))
            elif gtfs_name in ferry_gtfs:
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
                    gdf = conflate_gfts_shape(gtfs_shape_file, gtfs_name)
