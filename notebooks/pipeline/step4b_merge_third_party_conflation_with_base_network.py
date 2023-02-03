USAGE = """
Merge third-party data SharedStreets match results with the base network links (ShSt+OSMNX) and run heuristics.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: 
    - standard network data from step3
    - third-party network data conflation results from step4b

Outputs:  

"""
import methods
import pandas as pd
import geopandas as gpd
import geofeather
from pyproj import CRS
import os, datetime
import numpy as np
# import requests
# from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
# import fiona
from shapely.geometry import Point
from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs

INPUT_DATA_DIR = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
# base standard network data
SHST_OSMNX_LINK_FILE = os.path.join(INPUT_DATA_DIR, 'step3_join_shst_with_osm', 'step3_link.feather')
# third-party data matching results
THIRD_PARTY_MATCHED_DIR = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data')
TOMTOM_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TomTom', 'conflation_shst', 'matched.feather')
TM2_nonMarin_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_nonMarin', 'conflation_shst', 'matched.feather')
TM2_Marin_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_Marin', 'conflation_shst', 'matched.feather')
SFCTA_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'SFCTA', 'conflation_shst', 'matched.feather')
CCTA_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'CCTA', 'conflation_shst', 'matched.feather')
ACTC_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'ACTC', 'conflation_shst', 'matched.feather')

# output
CONFLATION_RESULT_DIR = os.path.join(OUTPUT_DATA_DIR, 'step4_third_party_data', 'output_with_all_third_party_data')
CONFLATED_LINK_GEOFEATHER_FILE = os.path.join(CONFLATION_RESULT_DIR, 'step4_link.feather')
ROAD_SHIELD_LONG_NAME_CROSSWALK_FILE = os.path.join(CONFLATION_RESULT_DIR, 'road_shield_long_name_crosswalk.csv')

if __name__ == '__main__':
    # create output directories if not exist
    if not os.path.exists(CONFLATION_RESULT_DIR):
        WranglerLogger.info('create output folder: {}'.format(CONFLATION_RESULT_DIR))
        os.makedirs(CONFLATION_RESULT_DIR)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        CONFLATION_RESULT_DIR,
        "step4d_conflate_with_third_party_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    #####################################
    # EPSG requirement
    # TARGET_EPSG = 4326
    lat_lon_epsg_str = 'EPSG:{}'.format(methods.LAT_LONG_EPSG)
    WranglerLogger.info('standard ESPG: {}'.format(lat_lon_epsg_str))
    nearest_match_epsg_str = 'epsg:{}'.format(methods.NEAREST_MATCH_EPSG)
    WranglerLogger.info('nearest match ESPG: {}'.format(nearest_match_epsg_str))

    ####################################
    # Read base network from step 3
    WranglerLogger.info('Reading shst_oxmnx links from {}'.format(SHST_OSMNX_LINK_FILE))
    link_gdf = gpd.read_feather(SHST_OSMNX_LINK_FILE)

    # verify link_gdf contains no duplicated ['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
    # which is the join key bwteen third-party data and shst+osmnx base network. Note that this is achieved in step3: 
    # https://github.com/BayAreaMetro/travel-model-two-networks/blob/76c99020af6b023fa232d655610304826ff70b8d/notebooks/pipeline/step3_join_shst_extraction_with_osm.py#L323,
    # with ['fromIntersectionId', 'toIntersectionId', 'u', 'v', 'shstReferenceId'] having the same de-dup effect.        
    # NOTE on duplicated shstReferenceId at this point: there are cases when for a two-way street, shst created two records with different
    # 'fromIntersectionId'/'toIntersectionId', each containing two-way osm ways, instead of creating one shst record with 'fromIntersectionId'
    # and 'toIntersectionId' simply flipped, but the two shst records still just have 'forwardReferenceId' and 'backReferenceId' flipped,
    # therefore when creating reverse links for two-way streets, a reverse link was created for each of the shst records. This is essentially
    # a data issue on the ShSt side. Example: shstReferenceId '431c41541438629028f30f5770836ae1', or in shst extract, forwardReferenceId or
    # backReferenceId is '431c41541438629028f30f5770836ae1'.
    unique_shst_link_IDs = ['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
    WranglerLogger.info('loaded {:,} shst_osmnx links: {:,} unique shstReferenceId,\
        {:,} unique combinations of shstReferenceId + shstGeometryId + fromIntersectionId + toIntersectionId,\
        {:,} unique shapes'.format(
        link_gdf.shape[0], link_gdf.shstReferenceId.nunique(),
        link_gdf[unique_shst_link_IDs].drop_duplicates().shape[0],
        link_gdf.shstGeometryId.nunique())
    )
    WranglerLogger.info('shst_osmnx links have columns: \n{}'.format(list(link_gdf)))

    ####################################
    # Load ShSt match results of third-party data sources, deduplicates, and update field names

    # NOTE on de-duplication method: 
    # In the shst matching result, there are cases where multiple third-party links were matched to one shst link.
    #
    # WSP's v12 pipeline attributed this to links crossing the subregion boundaries, and simply dropped duplicates and kept one third-party link for each shst link.
    # In their work for the bi-county model where CCTA and ACTC network datasets were processed, they kept all third-party links matched to the same shst link, 
    # and generated a 'max' and a 'min' value for the shst link.
    #
    # Upon visual inspections, I identified additional reasons for the "duplicated" match. ShSt matching is essentially a many-to-many match, especially when
    # shst links and third-party links split a same roadway segment at different places. Consider the following three cases:
    #   1) one third-party link is split into multiple pieces and matched to different shst links. The fields 'gisTotalSegments' amd 'gisSegmentIndex' 
    #      in the match output show the breakdown. This case alone doesn't create duplicated shst links in the matching result. 
    #   2) multiple third-party links are matched to one (usually quite long) shst link, with each representing a portion of the shst link. This creates
    #      duplicated shst links in the matching output, and the 'geometry' of each row now represents the partial shst geometry, in other words, 
    #      the 'matched' portion, instead of the entire shst link's geometry.
    #   3) multiple third-party links are matched to one shst link without breaking up the shst link. This also creates duplicated shst links in the matching output,
    #      but each row still represents the entire shst link's geometry; however, the 'score' field usually has different values.
    #  
    # Apply the following de-duplication approach:
    #   To address case 2), calculate the length of the matched partial shst links, and keep the longest one to represent the shst link.
    #   To address case 3) where all matched links have the same length, keep the matched links with the smallest 'score'. I didn't find any documentation on
    #       how "score" is calculated except for this part of the code (https://github.com/sharedstreets/sharedstreets-js/blob/98f8b78d0107046ed2ac1f681cff11eb5a356474/src/commands/match.ts#L696).
    #      I created an issue (https://github.com/sharedstreets/sharedstreets-js/issues/107). But from maps, it appears that a lower score represents a higher
    #      similarity between the raw third-party link's shape and the matched link's shape.

    ### TomTom
    WranglerLogger.info('loading TomTom ShSt matching result: {}'.format(TOMTOM_MATCHED_FILE))
    # tomtom_match_gdf = methods.read_shst_matched(TOMTOM_MATCHED_DIR, 'tomtom_*.out.matched.geojson')
    tomtom_matched_gdf = gpd.read_feather(TOMTOM_MATCHED_FILE)
    WranglerLogger.debug('{:,} TomTom matched records'.format(tomtom_matched_gdf.shape[0]))

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates by keeping largest length and smallest score
    tomtom_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tomtom_matched_gdf['matched_segment_length'] = tomtom_matched_gdf.length
    tomtom_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                   ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                   inplace=True)

    unique_tomtom_matched_gdf = tomtom_matched_gdf.drop_duplicates(subset=unique_shst_link_IDs)
    WranglerLogger.debug('keep {:,} TomTom matched records based on unique shst link'.format(
        unique_tomtom_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_tomtom_matched_gdf = unique_tomtom_matched_gdf[
        unique_shst_link_IDs + ['ID', 'reversed', 'RAMP', 'FREEWAY', 'LANES', 'FRC', 'FRC_def', 'NAME', 'SHIELDNUM', 'RTEDIR', 'TOLLRD']]
    
    # add data source prefix to column names
    unique_tomtom_matched_gdf.rename(columns={"ID"       : "tomtom_ID",
                                              "reversed" : 'tomtom_reversed',
                                              'RAMP'     : 'tomtom_RAMP',
                                              'FREEWAY'  : 'tomtom_FREEWAY',
                                              "LANES"    : "tomtom_lanes",
                                              "FRC"      : "tomtom_FRC",
                                              'FRC_def'  : 'tomtom_FRC_def',
                                              "NAME"     : "tomtom_name",
                                              "SHIELDNUM": "tomtom_shieldnum",
                                              "RTEDIR"   : "tomtom_rtedir",
                                              'TOLLRD'   : 'tomtom_TOLLRD'},
                                     inplace=True)

    # print out unique values for each key attribute to help fix typos
    for attribute in ['tomtom_lanes', 'tomtom_FRC', 'tomtom_shieldnum', 'tomtom_rtedir']:
        WranglerLogger.debug('{} unique values:\n{}'.format(attribute,
                                                            unique_tomtom_matched_gdf[attribute].unique()))
    # fix 'tomtom_shielfnum' = ' ' and tomtom_rtedir = ' '
    unique_tomtom_matched_gdf.loc[unique_tomtom_matched_gdf.tomtom_shieldnum == ' ', 'tomtom_shieldnum'] = ''
    unique_tomtom_matched_gdf.loc[unique_tomtom_matched_gdf.tomtom_rtedir == ' ', 'tomtom_rtedir'] = ''

    ### TM2 nonMarin data
    # read shst match result
    WranglerLogger.info('read TM2 nonMarin conflation result from {}'.format(TM2_nonMarin_MATCHED_FILE))
    tm2nonMarin_matched_gdf = gpd.read_feather(TM2_nonMarin_MATCHED_FILE)
    WranglerLogger.debug('{:,} tm2_nonMarin matched records'.format(tm2nonMarin_matched_gdf.shape[0]))

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates by keeping largest length and smallest score
    tm2nonMarin_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tm2nonMarin_matched_gdf['matched_segment_length'] = tm2nonMarin_matched_gdf.length
    tm2nonMarin_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                        ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                        inplace=True)

    unique_tm2nonMarin_matched_gdf = tm2nonMarin_matched_gdf.drop_duplicates(subset=unique_shst_link_IDs)
    WranglerLogger.info('keep {:,} TM2_nonMarin matched records based on unique shst link'.format(unique_tm2nonMarin_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_tm2nonMarin_matched_gdf = unique_tm2nonMarin_matched_gdf[
        unique_shst_link_IDs + ['A', 'B', 'NUMLANES_gp', 'NUMLANES_hov', 'NUMLANES_nontruck', 'FT', 'FT_def', 'ASSIGNABLE']]

    # add data source prefix to column names
    unique_tm2nonMarin_matched_gdf.rename(columns={'A'                  : 'TM2nonMarin_A',
                                                   'B'                  : 'TM2nonMarin_B',
                                                   'NUMLANES_gp'        : 'TM2nonMarin_LANES_GP',
                                                   'NUMLANES_hov'       : 'TM2nonMarin_LANES_HOV',
                                                   'NUMLANES_nontruck'  : 'TM2nonMarin_LANES_nonTruck',
                                                   'FT'                 : 'TM2nonMarin_FT',
                                                   'FT_def'             : 'TM2nonMarin_FT_def',
                                                   'ASSIGNABLE'         : 'TM2nonMarin_ASSIGNABLE'},
                                           inplace=True)

    ### TM2 Marin data
    # read shst match result
    WranglerLogger.info('read TM2 Marin conflation result from {}'.format(TM2_Marin_MATCHED_FILE))
    tm2marin_matched_gdf = gpd.read_feather(TM2_Marin_MATCHED_FILE)
    WranglerLogger.debug('{:,} tm2_Marin matched records'.format(tm2marin_matched_gdf.shape[0]))

    # convert to meter-based crs and calculate the length of each segment for de-duplication downstream
    tm2marin_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tm2marin_matched_gdf['matched_segment_length'] = tm2marin_matched_gdf.length
    tm2marin_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                     ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                     inplace=True)

    unique_tm2Marin_matched_gdf = tm2marin_matched_gdf.drop_duplicates(subset=unique_shst_link_IDs)
    WranglerLogger.info('keep {:,} TM2_Marin matched records based on unique shst link'.format(unique_tm2Marin_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_tm2Marin_matched_gdf = unique_tm2Marin_matched_gdf[
        unique_shst_link_IDs + ['A', 'B', 'MARIN', 'NUMLANES_gp', 'NUMLANES_hov', 'NUMLANES_nontruck', 'FT', 'FT_def', 'ASSIGNABLE']]

    # add data source prefix to column names
    unique_tm2Marin_matched_gdf.rename(columns={'A'                 : 'TM2Marin_A',
                                                'B'                 : 'TM2Marin_B',
                                                'MARIN'             : 'TM2Marin_tag',
                                                'NUMLANES_gp'       : 'TM2Marin_LANES_GP',
                                                'NUMLANES_hov'      : 'TM2Marin_LANES_HOV',
                                                'NUMLANES_nontruck' : 'TM2Marin_LANES_nonTruck',
                                                'FT'                : 'TM2Marin_FT',
                                                'FT_def'            : 'TM2Marin_FT_def',
                                                'ASSIGNABLE'        : 'TM2Marin_ASSIGNABLE'},
                                        inplace=True)

    ### SFCTA data
    # read shst match result
    WranglerLogger.info('read sfcta stick network conflation result from {}'.format(SFCTA_MATCHED_FILE))
    sfcta_matched_gdf = gpd.read_feather(SFCTA_MATCHED_FILE)
    WranglerLogger.debug('{:,} SFCTA matched records'.format(sfcta_matched_gdf.shape[0]))

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates by keeping largest length and smallest score
    sfcta_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    sfcta_matched_gdf['matched_segment_length'] = sfcta_matched_gdf.length
    sfcta_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                  ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                  inplace=True)
    unique_sfcta_matched_gdf = sfcta_matched_gdf.drop_duplicates(subset=unique_shst_link_IDs)
    WranglerLogger.info('keep {:,} SFCTA matched records based on unique shst link'.format(unique_sfcta_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_sfcta_matched_gdf = unique_sfcta_matched_gdf[
        unique_shst_link_IDs + ['A', 'B', 'FT', 'FT_def', 'STREETNAME', 'TYPE', 
                                'LANE_AM', 'LANE_OP', 'LANE_PM', 'BUSLANE_AM', 'BUSLANE_PM', 'BUSLANE_OP', 'BIKE_CLASS']]

    # add data source prefix to column names
    unique_sfcta_matched_gdf.rename(columns={'A'         : 'sfcta_A',
                                             'B'         : 'sfcta_B',
                                             'FT'        : 'sfcta_FT',
                                             'FT_def'    : 'sfcta_FT_def',
                                             'STREETNAME': 'sfcta_STREETNAME',
                                             'TYPE'      : 'sfcta_NAMETYPE', 
                                             'LANE_AM'   : 'sfcta_LANE_AM_GP',
                                             'LANE_OP'   : 'sfcta_LANE_OP_GP',
                                             'LANE_PM'   : 'sfcta_LANE_PM_GP',
                                             'BUSLANE_AM': 'sfcta_BUSLANE_AM',
                                             'BUSLANE_PM': 'sfcta_BUSLANE_PM',
                                             'BUSLANE_OP': 'sfcta_BUSLANE_OP',
                                             'BIKE_CLASS': 'sfcta_BIKE_CLASS'},
                                    inplace=True)

    ### CCTA data
    # read shst match result
    WranglerLogger.info('read CCTA conflation result from {}'.format(CCTA_MATCHED_FILE))
    ccta_matched_gdf = gpd.read_feather(CCTA_MATCHED_FILE)
    WranglerLogger.debug('{:,} CCTA matched records'.format(ccta_matched_gdf.shape[0]))

    # drop duplicates
    # NOTE: use the V13 method instead of WSP's by-county network rebuild method
    ccta_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    ccta_matched_gdf['matched_segment_length'] = ccta_matched_gdf.length
    ccta_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                 ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                 inplace=True)

    unique_ccta_matched_gdf = ccta_matched_gdf.drop_duplicates(subset=unique_shst_link_IDs)
    WranglerLogger.info('keep {:,} CCTA matched records based on unique shst link'.format(unique_ccta_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_ccta_matched_gdf = unique_ccta_matched_gdf[unique_shst_link_IDs + ['ID', 'AB_FT', 'AB_FT_def', 'AB_LANES_GP']]

    # add data source prefix to column names
    unique_ccta_matched_gdf.rename(columns={'ID'         : 'CCTA_ID',
                                            'AB_FT'      : 'CCTA_FT',
                                            'AB_FT_def'  : 'CCTA_FT_def',
                                            'AB_LANES_GP': 'CCTA_LANES_GP'},
                                   inplace=True)

    ### ACTC data
    # read shst match result
    WranglerLogger.info('read ACTC conflation result from {}'.format(ACTC_MATCHED_FILE))
    actc_matched_gdf = gpd.read_feather(ACTC_MATCHED_FILE)
    WranglerLogger.debug('{:,} ACTC matched records'.format(actc_matched_gdf.shape[0]))

    # NOTE: use the V13 method instead of WSP's by-county network rebuild method
    actc_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    actc_matched_gdf['matched_segment_length'] = actc_matched_gdf.length
    actc_matched_gdf.sort_values(unique_shst_link_IDs + ['matched_segment_length', 'score'], 
                                 ascending=[True] * len(unique_shst_link_IDs) + [False, True],
                                 inplace=True)

    unique_actc_matched_gdf = actc_matched_gdf.drop_duplicates(
        subset=unique_shst_link_IDs)
    WranglerLogger.info('keep {:,} ACTC matched records based on unique shst link'.format(unique_actc_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_actc_matched_gdf = unique_actc_matched_gdf[
        unique_shst_link_IDs + ['A', 'B', '2015_FT', '2015_FT_def', '2015_LN_GP', 'NMT2010', 'NMT2020']]

    # add data source prefix to column names
    unique_actc_matched_gdf.rename(columns={'A'          : 'ACTC_A',
                                            'B'          : 'ACTC_B',
                                            '2015_FT'    : 'ACTC_FT', 
                                            '2015_FT_def': 'ACTC_FT_def', 
                                            '2015_LN_GP' : 'ACTC_LN_GP_2015',
                                            'NMT2010'    : 'ACTC_nmt2010',
                                            'NMT2020'    : 'ACTC_nmt2020'},
                                   inplace=True)
 
    ####################################
    # merge third-party conflation results to the base network links from step3
    WranglerLogger.info('Mergeing third-party data conflation results with base network link_gdf')

    # TomTom: 'tomtom_ID', 'tomtom_reversed', 'tomtom_RAMP', 'tomtom_FREEWAY', 'tomtom_lanes', 'tomtom_FRC', 'tomtom_FRC_def',
    #         'tomtom_name', 'tomtom_shieldnum', 'tomtom_rtedir', 'tomtom_TOLLRD'
    tomtom_new_fields = list(set(list(unique_tomtom_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add TomTom attributes: {}'.format(tomtom_new_fields))
    link_with_third_party_gdf = pd.merge(link_gdf,
                                         unique_tomtom_matched_gdf,
                                         on=unique_shst_link_IDs,
                                         how='left')

    WranglerLogger.debug('{:,} base network links have TomTom attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.tomtom_ID.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.tomtom_ID.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TomTom attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.tomtom_ID.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.tomtom_ID.isnull()]['roadway'].value_counts(dropna=False)))
    
    # TM2_nonMarin: 'TM2nonMarin_A', 'TM2nonMarin_B', 'TM2nonMarin_LANES_GP', 'TM2nonMarin_LANES_HOV', 'TM2nonMarin_LANES_nonTruck',
    # 'TM2nonMarin_FT', 'TM2nonMarin_FT_def', 'TM2nonMarin_ASSIGNABLE'
    tm2nonmarin_new_fields = list(set(list(unique_tm2nonMarin_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add TM2_nonMarin attributes: {}'.format(tm2nonmarin_new_fields))
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_tm2nonMarin_matched_gdf,
                                         how='left',
                                         on=unique_shst_link_IDs)

    WranglerLogger.debug('{:,} base network links have TM2_nonMarin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2nonMarin_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2nonMarin_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TM2_nonMarin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2nonMarin_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2nonMarin_A.isnull()]['roadway'].value_counts(dropna=False)))

    # TM2_Marin: 'TM2Marin_A', 'TM2Marin_B', 'TM2Marin_tag', 'TM2Marin_LANES_GP', 'TM2Marin_LANES_HOV', 'TM2Marin_LANES_nonTruck',
    # 'TM2Marin_FT', 'TM2Marin_FT_def', 'TM2Marin_ASSIGNABLE'
    tm2marin_new_fields = list(set(list(unique_tm2Marin_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add TM2_Marin attributes: {}'.format(tm2marin_new_fields))
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_tm2Marin_matched_gdf,
                                         how='left',
                                         on=unique_shst_link_IDs)

    WranglerLogger.debug('{:,} base network links have TM2_Marin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2Marin_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2Marin_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TM2_Marin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2Marin_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2Marin_A.isnull()]['roadway'].value_counts(dropna=False)))                                         

    # SFCTA: 'sfcta_A', 'sfcta_B', 'sfcta_FT', 'sfcta_FT_def', 'sfcta_STREETNAME', 'sfcta_NAMETYPE',
    # 'sfcta_LANE_AM_GP', 'sfcta_LANE_OP_GP', 'sfcta_LANE_PM_GP', 'sfcta_BUSLANE_AM', 'sfcta_BUSLANE_PM', 'sfcta_BUSLANE_OP',
    # 'sfcta_BIKE_CLASS'
    sfcta_new_fields = list(set(list(unique_sfcta_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add SFCTA attributes: {}'.format(sfcta_new_fields))
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_sfcta_matched_gdf,
                                         how='left',
                                         on=unique_shst_link_IDs)

    WranglerLogger.debug('{:,} base network links have SFCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.sfcta_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.sfcta_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have SFCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.sfcta_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.sfcta_A.isnull()]['roadway'].value_counts(dropna=False)))

    # CCTA: 'CCTA_ID', 'CCTA_FT', 'CCTA_FT_def', 'CCTA_LANES_GP'
    ccta_new_fields = list(set(list(unique_ccta_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add CCTA attributes: {}'.format(ccta_new_fields))
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_ccta_matched_gdf,
                                         how='left',
                                         on=unique_shst_link_IDs)

    WranglerLogger.debug('{:,} base network links have CCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.CCTA_ID.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.CCTA_ID.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have CCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.CCTA_ID.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.CCTA_ID.isnull()]['roadway'].value_counts(dropna=False)))

    # ACTC: 'ACTC_A', 'ACTC_B', 'ACTC_FT',  'ACTC_FT_def', 'ACTC_LN_GP_2015', 'ACTC_nmt2010', 'ACTC_nmt2020'
    actc_new_fields = list(set(list(unique_actc_matched_gdf)) - set(unique_shst_link_IDs))
    WranglerLogger.info('add ACTC attributes: {}'.format(actc_new_fields))
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_actc_matched_gdf,
                                         how='left',
                                         on=unique_shst_link_IDs)

    WranglerLogger.debug('{:,} base network links have ACTC attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.ACTC_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.ACTC_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have ACTC attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.ACTC_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.ACTC_A.isnull()]['roadway'].value_counts(dropna=False)))

    ####################################
    WranglerLogger.info('Write out third-party conflation result data base')
    # write out conflation data base
    conflation_result_fields = [
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'roadway', 'hierarchy', 'drive_access', 'bike_access', 'walk_access',
        # lane accounting
        'osm_dir_tag', 'lane_count_type', 'reverse', 'lanes_tot', 'lanes_gp', 'lanes_hov', 'lanes_busonly', 
        'lanes_gp_through', 'lanes_gp_turn', 'lanes_gp_aux', 'lanes_gp_mix', 'lanes_gp_bothways', 'osm_agg',
        # other
        'county', 'length_meter'] + tomtom_new_fields + tm2nonmarin_new_fields + tm2marin_new_fields + sfcta_new_fields + ccta_new_fields + actc_new_fields
    
    link_conflation_fields_df = link_with_third_party_gdf[conflation_result_fields]
    LINK_WITH_THIRD_PARTY_CSV_FILE = os.path.join(CONFLATION_RESULT_DIR, 'link_with_all_third_party_attrs.csv')
    WranglerLogger.info('export conflation fields to {}'.format(LINK_WITH_THIRD_PARTY_CSV_FILE))
    link_conflation_fields_df.to_csv(LINK_WITH_THIRD_PARTY_CSV_FILE, index=False)

    # export the spatial to inspect on a map to decide lane heuristics rule
    LINK_WITH_THIRD_PARTY_FILE = os.path.join(CONFLATION_RESULT_DIR, 'link_with_all_third_party_attrs.feather')
    link_with_third_party_gdf.to_feather(LINK_WITH_THIRD_PARTY_FILE)

    ####################################
    # Apply road name heuristics to finalize road names, adding two new field 'road_name' (str), 'road_name_heuristic' (int8),
    # also writing out 'road_shield_long_name_crosswalk' mainly for QAQC 
    methods.determine_road_name(link_with_third_party_gdf, ROAD_SHIELD_LONG_NAME_CROSSWALK_FILE)

    ####################################
    # Apply lane heuristics to finalize lane accounting

    # tm2_Marin 'USECLASS' represents link user class: 0: NA link open to everyone, 2: HOV 2+, 3: HOV 3+, 4: No combination trucks.
    # for USECLASS==0, lanes = general purpose lanes; for USECLASS==2, 3, lanes = HOV lanes, general purpose lanes = 0.

    # SFCTA:
    # 'BUSLANE' !=0 represents bus-only lane in addition to general purpose lane ('LANE')
    # 'USE' == 2 or 3 represents HOV lane in addition to general purpose lane ('LANE')

    # TODO
    methods.determine_number_of_gp_lanes(link_conflation_fields_gdf)

    # TODO
    methods.determine_number_of_bus_lanes(link_conflation_fields_gdf)

    # TODO
    methods.determine_number_of_hov_lanes(link_conflation_fields_gdf)

    # TODO
    methods.finalize_lane_accounting(link_conflation_fields_gdf)

    ####################################
    # Write out standard links
    # TODO: decide which columns to include.
    # Note: the initial pipeline method only writes out columns included in the input shst_osmnx_gdf; all columns
    # from third-party are exported as a .csv file.
    WranglerLogger.info('Saving links to {}'.format(CONFLATED_LINK_GEOFEATHER_FILE))
    geofeather.to_geofeather(link_with_third_party_gdf, CONFLATED_LINK_GEOFEATHER_FILE)

    WranglerLogger.info('Done')
