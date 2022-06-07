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

# from methods import read_shst_extract, LAT_LONG_EPSG
# from methods import link_df_to_geojson
from methods import point_df_to_geojson
from network_wrangler import WranglerLogger, setupLogging

#####################################
# EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'EPSG:{}'.format(methods.LAT_LONG_EPSG)
WranglerLogger.info('standard ESPG: {}'.format(lat_lon_epsg_str))
nearest_match_epsg_str = 'epsg:{}'.format(methods.NEAREST_MATCH_EPSG)
WranglerLogger.info('nearest match ESPG: {}'.format(nearest_match_epsg_str))

#####################################
# inputs and outputs

INPUT_DATA_DIR = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']
# base standard network data
# SHST_OSMNX_LINK_FILE = os.path.join(INPUT_DATA_DIR, 'step3_join_shst_with_osm', 'step3_link.feather')
SHST_OSMNX_LINK_FILE = 'C:\\Users\\ywang\\Documents\\GitHub\\travel-model-two-networks\\step3_join_shst_with_osm\\step3_link.feather'
# third-party data matching results
THIRD_PARTY_MATCHED_DIR = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data')
TOMTOM_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TomTom', 'conflation_shst', 'matched.feather')
TM2_nonMarin_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_nonMarin', 'conflation_shst', 'matched.feather')
TM2_Marin_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_Marin', 'conflation_shst', 'matched.feather')
SFCTA_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'SFCTA', 'conflation_shst', 'matched.feather')
CCTA_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'CCTA', 'conflation_shst', 'matched.feather')
ACTC_MATCHED_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'ACTC', 'conflation_shst', 'matched.feather')
# TODO: PEMS_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'pems')

# PEMS
INPUT_PEMS_FILE = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data', 'PeMS', 'input', 'pems_period.csv')

# output
CONFLATION_RESULT_DIR = os.path.join(OUTPUT_DATA_DIR, 'step4_third_party_data', 'output_with_all_third_party_data')
CONFLATED_LINK_GEOFEATHER_FILE = os.path.join(CONFLATION_RESULT_DIR, 'step4_link.feather')
# conflation summary table
CONFLATION_SUMMARY_FILE = os.path.join(CONFLATION_RESULT_DIR, 'conflation_result.csv')

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

    ####################################
    # Read base network from step 3
    WranglerLogger.info('Reading shst_oxmnx links from {}'.format(SHST_OSMNX_LINK_FILE))
    link_gdf = geofeather.from_geofeather(SHST_OSMNX_LINK_FILE)
    link_gdf = gpd.GeoDataFrame(link_gdf, crs=lat_lon_epsg_str)

    WranglerLogger.info('loaded {:,} shst_osmnx links, with {:,} unique shstReferenceId and {:,} unique shapes'.format(
        link_gdf.shape[0], link_gdf.shstReferenceId.nunique(), link_gdf.shstGeometryId.nunique())
    )
    WranglerLogger.info('shst_osmnx links have columns: \n{}'.format(list(link_gdf)))

    ####################################
    # Load ShSt match results of third-party data sources, deduplicates, and update field names

    ### TomTom
    WranglerLogger.info('loading TomTom ShSt matching result: {}'.format(TOMTOM_MATCHED_FILE))
    # tomtom_match_gdf = methods.read_shst_matched(TOMTOM_MATCHED_DIR, 'tomtom_*.out.matched.geojson')
    tomtom_matched_gdf = geofeather.from_geofeather(TOMTOM_MATCHED_FILE)

    # TODO: decide on a de-duplicate method:
    # Duplicates may come from links crossing the subregion boundaries, or from multiple third-party links being matched 
    # to the same sharedstreets link, often because a shst link is long (already aggregated, similar to in the situation of shst-osm relationship).
    # In the latter case, in the matching output, each row has the shst-related attributes (e.g. shstReferecenId) of the entire shst link, and 
    # the third-party attributes (e.g. FT) of the third-party link, and the "matched" geometry - a segment of the shst link's geometry (which often
    # has a different shape from the raw third-party link).  
    # In WSP's code, for data sources other than CCTA and ACTC, it simply dropped duplicates and kept one third-party link for each shst link, and use
    # its value (FT, LANES) to represent the shst link; for CCTA and ACTC datasets, it kept all third-party links matched to the same shst link, 
    # and generated a 'max' and a 'min' value for the shst link. 
    #
    # Upon a spot check with visual inspection, it appears that two approaches worth exploring, both call "drop_duplicates(
    # subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])" to keep only one row, but use different condition 
    # to select which row to keep: 
    #   1) keep the row with the highest 'score' in shst match output. I didn't find any documentation on how "score" is calculated except for 
    #      this part of the code (https://github.com/sharedstreets/sharedstreets-js/blob/98f8b78d0107046ed2ac1f681cff11eb5a356474/src/commands/match.ts#L696).
    #      I raised an issue (https://github.com/sharedstreets/sharedstreets-js/issues/107). But from maps, it appears that a higher score represents a higher
    #      similarity between the raw third-party link's shape and the matched link's shape. 
    #   2) calculate the length of the matched links, and keep the longest one. This assumes that all the third-party links got matched to the correct shst link,
    #      so we use the longest segment to reprenet the entire shst link. 
    # I tend to think approach 2) makes more sense.

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates
    tomtom_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tomtom_matched_gdf['matched_segment_length'] = tomtom_matched_gdf.length
    tomtom_matched_gdf.sort_values(['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'matched_segment_length'], 
                                   ascending=False,
                                   inplace=True)

    unique_tomtom_matched_gdf = tomtom_matched_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])
    WranglerLogger.info('after dropping duplicates, {} TomTom links remain'.format(
        unique_tomtom_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_tomtom_matched_gdf = unique_tomtom_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'tomtom_link_id', 'ID', 'F_JNCTID', 'T_JNCTID', 'LANES', 'FRC', 'NAME', 'SHIELDNUM', 'RTEDIR']]
    
    # add data source prefix to column names
    unique_tomtom_matched_gdf.rename(columns={"ID": "tomtom_ID",
                                              "LANES": "tomtom_lanes",
                                              "FRC": "tomtom_FRC",
                                              "NAME": "tomtom_name",
                                              "SHIELDNUM": "tomtom_shieldnum",
                                              "RTEDIR": "tomtom_rtedir"},
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
    tm2nonMarin_matched_gdf = geofeather.from_geofeather(TM2_nonMarin_MATCHED_FILE)

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates
    tm2nonMarin_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tm2nonMarin_matched_gdf['matched_segment_length'] = tm2nonMarin_matched_gdf.length
    tm2nonMarin_matched_gdf.sort_values(['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'matched_segment_length'], 
                                        ascending=False,
                                        inplace=True)

    unique_tm2nonMarin_matched_gdf = tm2nonMarin_matched_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    # only keep fields needed for link attributes heuristics
    unique_tm2nonMarin_matched_gdf = unique_tm2nonMarin_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'A', 'B', 'NUMLANES', 'FT', 'ASSIGNABLE']]

    # add data source prefix to column names
    unique_tm2nonMarin_matched_gdf.rename(columns={'A': 'TM2nonMarin_A',
                                                   'B': 'TM2nonMarin_B',
                                                   'NUMLANES': 'TM2nonMarin_LANES',
                                                   'FT': 'TM2nonMarin_FT',
                                                   'ASSIGNABLE': 'TM2nonMarin_ASSIGNABLE'},
                                          inplace=True)

    ### TM2 Marin data
    # read shst match result
    WranglerLogger.info('read TM2 Marin conflation result from {}'.format(TM2_Marin_MATCHED_FILE))
    tm2marin_matched_gdf = geofeather.from_geofeather(TM2_Marin_MATCHED_FILE)

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates
    tm2marin_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    tm2marin_matched_gdf['matched_segment_length'] = tm2marin_matched_gdf.length
    tm2marin_matched_gdf.sort_values(['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'matched_segment_length'], 
                                     ascending=False,
                                     inplace=True)

    unique_tm2marin_matched_gdf = tm2marin_matched_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])
    WranglerLogger.info('{:,} rows after dropping duplicates'.format(unique_tm2marin_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_tm2marin_matched_gdf = unique_tm2marin_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'A', 'B', 'NUMLANES', 'FT', 'ASSIGNABLE']]

    # add data source prefix to column names
    unique_tm2marin_matched_gdf.rename(columns={'A': 'TM2Marin_A',
                                                'B': 'TM2Marin_B',
                                                'NUMLANES': 'TM2Marin_LANES',
                                                'FT': 'TM2Marin_FT',
                                                'ASSIGNABLE': "TM2Marin_ASSIGNABLE"},
                                       inplace=True)

    ### SFCTA data
    # read shst match result
    WranglerLogger.info('read sfcta stick network conflation result from {}'.format(SFCTA_MATCHED_FILE))
    sfcta_stick_matched_gdf = geofeather.from_geofeather(SFCTA_MATCHED_FILE)

    # convert to meter-based crs and calculate the length of each segment, then drop duplicates
    sfcta_stick_matched_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    sfcta_stick_matched_gdf['matched_segment_length'] = sfcta_stick_matched_gdf.length
    sfcta_stick_matched_gdf.sort_values(['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId', 'matched_segment_length'], 
                                        ascending=False,
                                        inplace=True)

    unique_sfcta_matched_gdf = sfcta_stick_matched_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])
    WranglerLogger.info('{:,} rows after dropping duplicates'.format(unique_sfcta_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_sfcta_matched_gdf = unique_sfcta_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'A', 'B', 'FT', 'STREETNAME', 'LANE_AM', 'LANE_OP', 'LANE_PM']]

    # add data source prefix to column names
    unique_sfcta_matched_gdf.rename(columns={"A"         : "sfcta_A",
                                             "B"         : "sfcta_B",
                                             "FT"        : "sfcta_FT",
                                             "STREETNAME": "sfcta_STREETNAME",
                                             "LANE_AM"   : "sfcta_LANE_AM",
                                             "LANE_OP"   : "sfcta_LANE_OP",
                                             "LANE_PM"   : "sfcta_LANE_PM"},
                                    inplace=True)

    ### CCTA data
    # read shst match result
    WranglerLogger.info('read CCTA conflation result from {}'.format(CCTA_MATCHED_FILE))
    ccta_matched_gdf = geofeather.from_geofeather(CCTA_MATCHED_FILE)

    # drop duplicates
    # TODO: for now keep the du-dup method used by WSP for CCTA and ACTC. May revise to be consistent with others.
    unique_ccta_matched_gdf = ccta_matched_gdf.drop_duplicates()
    WranglerLogger.info('{:,} rows after dropping duplicates'.format(unique_ccta_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_ccta_matched_gdf = unique_ccta_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'ID', 'AB_LANES']]

    # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId, including
    # when multiple ccta links have been joined to the same shstReferenceId
    ccta_lanes_conflation_df = unique_ccta_matched_gdf.loc[
        unique_ccta_matched_gdf['AB_LANES'] > 0
        ].groupby(
        ['shstReferenceId']
    )['AB_LANES'].apply(list).to_frame().reset_index()

    ccta_lanes_conflation_df['base_lanes_min'] = ccta_lanes_conflation_df['AB_LANES'].apply(lambda x: min(set(x)))
    ccta_lanes_conflation_df['base_lanes_max'] = ccta_lanes_conflation_df['AB_LANES'].apply(lambda x: max(set(x)))
    
    # TODO: decide if export or merge into the base network
    ccta_lanes_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'cctamodel_legacy_lanes.csv'), index=False)

    # add data source prefix to column names
    unique_ccta_matched_gdf.rename(columns={'ID': 'CCTA_ID',
                                            'base_lanes_min': 'CCTA_base_lanes_min',
                                            'base_lanes_max': 'CCTA_base_lanes_max'},
                                   inplace=True)

    ### ACTC data
    # read shst match result
    WranglerLogger.info('read ACTC conflation result from {}'.format(ACTC_MATCHED_FILE))
    actc_matched_gdf = geofeather.from_geofeather(ACTC_MATCHED_FILE)
    
    # TODO: reconcile different methodologies for dropping duplicates
    unique_actc_matched_gdf = actc_matched_gdf.drop_duplicates()
    WranglerLogger.info('{:,} rows after dropping duplicates'.format(unique_actc_matched_gdf.shape[0]))

    # only keep fields needed for link attributes heuristics
    unique_actc_matched_gdf = unique_actc_matched_gdf[[
        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
        'A', 'B', 'BASE_LN', 'NMT2010', 'NMT2020']]

    # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId
    actc_lanes_conflation_df = unique_actc_matched_gdf.loc[unique_actc_matched_gdf['BASE_LN'] > 0].groupby(
        ['shstReferenceId']
    )['BASE_LN'].apply(list).to_frame().reset_index()

    actc_lanes_conflation_df['base_lanes_min'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: min(set(x)))
    actc_lanes_conflation_df['base_lanes_max'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_lanes_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'actcmodel_legacy_lanes.csv'), index=False)

    # same for bike lane
    actc_bike_conflation_df = unique_actc_matched_gdf.groupby(
        ['shstReferenceId']
    )[['NMT2010', 'NMT2020']].agg(lambda x: list(x)).reset_index()

    actc_bike_conflation_df['nmt2010_min'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2010_max'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: max(set(x)))
    actc_bike_conflation_df['nmt2020_min'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2020_max'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_bike_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'actcmodel_legacy_bike.csv'), index=False)

    # add data source prefix to column names
    unique_actc_matched_gdf.rename(columns={'A': 'ACTC_A',
                                            'B': 'ACTC_B',
                                            'base_lanes_min': 'ACTC_base_lanes_min',
                                            'base_lanes_max': 'ACTC_base_lanes_max',
                                            'nmt2010_min': 'ACTC_nmt2010_min',
                                            'nmt2010_max': 'ACTC_nmt2010_max',
                                            'nmt2020_min': 'ACTC_nmt2020_min',
                                            'nmt2020_max': 'ACTC_nmt2020_max'},
                                   inplace=True)
 
    ####################################
    # merge third-party conflation results to the base network links from step3
    WranglerLogger.info('Mergeing third-party data conflation results with base network link_gdf')

    # TomTom, bring in 'tomtom_link_id', "tomtom_ID", 'F_JNCTID', 'T_JNCTID', "tomtom_lanes", "tomtom_FRC", "tomtom_name", "tomtom_shieldnum", "tomtom_rtedir"
    WranglerLogger.info('add TomTom attributes to base network from step3')
    link_with_third_party_gdf = pd.merge(link_gdf,
                                         unique_tomtom_matched_gdf,
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'],
                                         how='left')

    WranglerLogger.debug('{:,} base network links have TomTom attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.tomtom_link_id.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.tomtom_link_id.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TomTom attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.tomtom_link_id.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.tomtom_link_id.isnull()]['roadway'].value_counts(dropna=False)))
    
    # TM2_nonMarin, bring in 'TM2nonMarin_A', 'TM2nonMarin_B', 'TM2nonMarin_LANES', 'TM2nonMarin_FT', 'TM2nonMarin_ASSIGNABLE'
    WranglerLogger.info('add TM2_nonMarin attributes')
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_tm2nonMarin_matched_gdf,
                                         how='left',
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    WranglerLogger.debug('{:,} base network links have TM2_nonMarin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2nonMarin_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2nonMarin_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TM2_nonMarin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2nonMarin_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2nonMarin_A.isnull()]['roadway'].value_counts(dropna=False)))

    # TM2_Marin, bring in 'TM2Marin_A', 'TM2Marin_B', 'TM2Marin_LANES', 'TM2Marin_FT', 'TM2Marin_ASSIGNABLE'
    WranglerLogger.info('add TM2_Marin attributes')
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_tm2marin_matched_gdf,
                                         how='left',
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    WranglerLogger.debug('{:,} base network links have TM2_Marin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2Marin_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2Marin_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have TM2_Marin attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.TM2Marin_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.TM2Marin_A.isnull()]['roadway'].value_counts(dropna=False)))                                         

    # SFCTA, bring in 'sfcta_A', 'sfcta_B', 'sfcta_FT', 'sfcta_STREETNAME', 'sfcta_LANE_AM', 'sfcta_LANE_OP', 'sfcta_LANE_PM'
    WranglerLogger.info('add SFCTA attributes')
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_sfcta_matched_gdf,
                                         how='left',
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    WranglerLogger.debug('{:,} base network links have SFCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.sfcta_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.sfcta_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have SFCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.sfcta_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.sfcta_A.isnull()]['roadway'].value_counts(dropna=False)))

    # CCTA, bring in 'CCTA_ID', 'CCTA_base_lanes_min', 'CCTA_base_lanes_max'
    WranglerLogger.info('add CCTA attributes')
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_ccta_matched_gdf,
                                         how='left',
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    WranglerLogger.debug('{:,} base network links have CCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.CCTA_ID.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.CCTA_ID.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have CCTA attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.CCTA_ID.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.CCTA_ID.isnull()]['roadway'].value_counts(dropna=False)))

    # ACTC, bring in 'ACTC_A', 'ACTC_B', 'ACTC_base_lanes_min', 'ACTC_base_lanes_max', 'ACTC_nmt2010_min', 'ACTC_nmt2010_max', 'ACTC_nmt2020_min', 'ACTC_nmt2020_max'
    WranglerLogger.info('add ACTC attributes')
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         unique_actc_matched_gdf,
                                         how='left',
                                         on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    WranglerLogger.debug('{:,} base network links have ACTC attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.ACTC_A.notnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.ACTC_A.notnull()]['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('{:,} base network links do not have ACTC attributes, stats by roadway type:\n{}'.format(
        (link_with_third_party_gdf.ACTC_A.isnull()).sum(),
        link_with_third_party_gdf.loc[link_with_third_party_gdf.ACTC_A.isnull()]['roadway'].value_counts(dropna=False)))

    ####################################
    # Conflate PEMS data by matching to TomTom route, direction, and facility type

    # load PEMS raw data
    WranglerLogger.info('Loading PEMS raw data from {}'.format(INPUT_PEMS_FILE))
    pems_raw_df = pd.read_csv(INPUT_PEMS_FILE)

    WranglerLogger.info('drop points without complete longitude and latitude info')
    pems_df = pems_raw_df[~((pems_raw_df.longitude.isnull()) | (pems_raw_df.latitude.isnull()))]
    WranglerLogger.debug('after dropping, PEMS data went from {} rows to {} rows'.format(pems_raw_df.shape[0],
                                                                                         pems_df.shape[0]))
    # PEMS data contains multiple years
    for yr in pems_df.year.unique():
        WranglerLogger.debug('for year {}: PEMS data contains {} stations, {} unique route+direction comb'.format(
            yr,
            pems_df.loc[pems_df.year == yr]['station'].nunique(),
            pems_df[['route', 'direction']].drop_duplicates().shape[0]))

    # generate 'geometry' and convert to geodataframe
    WranglerLogger.info('create "geometry" from longitude and latitude')
    pems_df['geometry'] = [Point(xy) for xy in zip(pems_df.longitude, pems_df.latitude)]
    pems_gdf = gpd.GeoDataFrame(pems_df,
                                geometry=pems_df['geometry'],
                                crs={'init': lat_lon_epsg_str})

    # convert crs to meter-based for nearest match algorithm
    WranglerLogger.info('convert to meter-based epsg:26915')
    pems_gdf = pems_gdf.to_crs(CRS(nearest_match_epsg_str))

    # get TomTom 'tomtom_shieldnum', 'tomtom_rtedir' in the correct crs
    WranglerLogger.info('Preparing TomTom Shieldnum and rtedir for conflation with PEMS')
    # subset link_with_third_party_gdf to only keep needed fields
    tomtom_for_pems_conflation_gdf = link_with_third_party_gdf[list(link_gdf) + ['tomtom_shieldnum', 'tomtom_rtedir']]
    # convert crs to meter-based for nearest match algorithm
    tomtom_for_pems_conflation_gdf = tomtom_for_pems_conflation_gdf.to_crs(CRS(nearest_match_epsg_str))

    # To match PEMS to the nearest link with the same shieldnum and direction, examine these values first
    WranglerLogger.info('Matching PEMS to the nearest link with the same shieldnum and direction')
    WranglerLogger.debug('PEMS road "type" value counts:\n{}'.format(pems_gdf['type'].value_counts(dropna=False)))
    WranglerLogger.debug('base network "roadway" value counts:\n{}'.format(
        tomtom_for_pems_conflation_gdf['roadway'].value_counts(dropna=False)))
    WranglerLogger.debug('PEMS route value counts:\n{}'.format(pems_gdf['route'].value_counts(dropna=False)))
    WranglerLogger.debug('TomTom shieldnum value counts:\n{}'.format(
        tomtom_for_pems_conflation_gdf['tomtom_shieldnum'].value_counts(dropna=False)))

    # only for QAQC: write out links whose shieldnum is include in PEMS routes - these are candidates for nearest match
    candidate_links_for_PEMS_match_QAQC_gdf = tomtom_for_pems_conflation_gdf.loc[
        tomtom_for_pems_conflation_gdf['tomtom_shieldnum'].isin(pems_gdf['route'].unique().astype(str))]
    candidate_links_for_PEMS_match_QAQC_gdf.reset_index(drop=True, inplace=True)

    WranglerLogger.debug('TomTom unique shieldnum+tomtom_rtedir comb:\n{}'.format(
        tomtom_for_pems_conflation_gdf.groupby(['tomtom_shieldnum', 'tomtom_rtedir'])['shstReferenceId'].count().reset_index().rename(columns={'station': 'row_count'})))
    WranglerLogger.debug('PEMS unique route+direction comb:\n{}'.format(
        pems_gdf.groupby(['route', 'direction'])['station'].count().reset_index().rename(columns={'station': 'row_count'})))

    OUTPUT_FILE = os.path.join(CONFLATION_RESULT_DIR, 'candidate_links_for_PEMS_match_QAQC.feather')
    geofeather.to_geofeather(candidate_links_for_PEMS_match_QAQC_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {} rows to {}".format(candidate_links_for_PEMS_match_QAQC_gdf.shape[0], OUTPUT_FILE))

    # 1. create a PEMS road "type" to base network "roadway" dictionary
    WranglerLogger.debug('PEMS "type" field value counts:\n{}'.format(pems_gdf.type.value_counts(dropna=False)))
    WranglerLogger.debug('base network "roadway" field value counts:\n{}'.format(
        tomtom_for_pems_conflation_gdf.roadway.value_counts(dropna=False)))

    roadway_types = tomtom_for_pems_conflation_gdf.loc[tomtom_for_pems_conflation_gdf['roadway'].notnull()]['roadway'].unique()
    pems_type_roadway_crosswalk = {'ML': ['tertiary', 'primary', 'secondary', 'motorway', 'trunk'],
                                   'HV': [c for c in roadway_types if c.endswith("_link")],
                                   'FF': [c for c in roadway_types if c.endswith("_link")],
                                   'OR': [c for c in roadway_types if c.endswith("_link")],
                                   'FR': [c for c in roadway_types if c.endswith("_link")]}

    # 2. match PEMS stations to base network links based on PEMS route + direction, and TomTom shieldnum + rtedir
    pems_nearest_match = methods.pems_station_sheild_dir_nearest_match(pems_gdf,
                                                            tomtom_for_pems_conflation_gdf,
                                                            pems_type_roadway_crosswalk)

    # 3. merge it back to pems_gdf
    WranglerLogger.info('Merging PEMS nearest sheildnum+direction matching result back to pems_gdf')
    pems_nearest_gdf = pd.merge(pems_gdf,
                                pems_nearest_match.drop(['point', 'geometry'], axis=1),
                                how='left',
                                on=['station', 'longitude', 'latitude', 'route', 'direction', 'type'])
    WranglerLogger.info('Finished PEMS nearest sheildnum+direction matching')
    WranglerLogger.debug('{} out of {} PEMS records found a matching link, {} stations, with "type" value counts:\n{}'.format(
        (pems_nearest_gdf.shstReferenceId.notnull()).sum(),
        pems_nearest_gdf.shape[0],
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.notnull()].station.nunique(),
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.notnull()]['type'].value_counts()
    ))
    WranglerLogger.debug('{} PEMS records failed to find a matching link, representing {} stations, with "type" value counts:\n{}'.format(
        (pems_nearest_gdf.shstReferenceId.isnull()).sum(),
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.isnull()].station.nunique(),
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.isnull()]['type'].value_counts()
    ))

    # QAQC: write out PEMS data not able to find nearest sheildnum+direction match for debugging
    pems_nearest_debug_gdf = pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.isnull()]
    pems_nearest_debug_gdf.reset_index(drop=True, inplace=True)
    OUTPUT_FILE = os.path.join(CONFLATION_RESULT_DIR, 'PEMS_no_nearest_match_QAQC.feather')
    geofeather.to_geofeather(pems_nearest_debug_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {} rows to {}".format(pems_nearest_debug_gdf.shape[0], OUTPUT_FILE))

    # 4. (TODO: decide if this step is useful) For those that failed in nearest sheildnum+direction match, use nearest match only based on facility type
    # get PEMS station+type+location that failed to find nearest match
    pems_sheild_dir_unmatched_gdf = pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.isnull()]
    pems_sheild_dir_unmatched_station_df = pems_sheild_dir_unmatched_gdf[['station', 'type', 'latitude', 'longitude']].drop_duplicates()
    WranglerLogger.debug('PEMS records not find a nearest sheildnum+direction match represent {} '
                         'unique PEMS station/type/location'.format(pems_sheild_dir_unmatched_station_df.shape[0]))
    # convert all links to lat/lon epsg (epsg:4326)
    link_for_pems_ft_match_gdf = tomtom_for_pems_conflation_gdf.to_crs(CRS(lat_lon_epsg_str))

    # call the function to do facility type based shortest distance match
    pems_stations_ft_matched_gdf = methods.pems_match_ft(pems_sheild_dir_unmatched_station_df,
                                                         link_for_pems_ft_match_gdf,
                                                         pems_type_roadway_crosswalk)

    # join the result back to pems_sheild_dir_unmatched_gdf
    pems_ft_matched_df = pd.merge(pems_sheild_dir_unmatched_gdf.drop('shstReferenceId', axis=1),
                                  pems_stations_ft_matched_gdf[['shstReferenceId', 'station', 'longitude', 'latitude', 'type']],
                                  how='left',
                                  on=['station', 'type', 'longitude', 'latitude'])
    WranglerLogger.info('facility type based matching matched {} additional stations'.format(
        pems_ft_matched_df.station.nunique()))
    WranglerLogger.info('facility types of still unmatched PEMS records:\n{}'.format(
        pems_ft_matched_df.loc[pems_ft_matched_df.shstReferenceId.isnull()]['type'].value_counts()))

    # TODO: if also do ShSt match for PEMS points, use that result to fill in unmatched

    # 5. concatenate matching results from the two methods
    pems_sheild_dir_matched_gdf = pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.notnull()]
    pems_conflation_result = pd.concat([pems_sheild_dir_matched_gdf, pems_ft_matched_df],
                                       sort=False,
                                       ignore_index=True)

    # post-process PEMS matching results
    # link can have multiple pems station on it, so trying to get the mode of #lanes by station type
    # TODO: the initial code uses three years' PEMS data 2014, 2015, 2016 to represent 2015 lane count. Decide if want to apply the year filter before running matching, which would have reduce run time.
    # first, get lane count for each unique comb of shstReferenceId and PEMS type
    pems_lanes_by_shstRef_type_df = pems_conflation_result.loc[
        (pems_conflation_result['year'].isin([2014, 2015, 2016]))].groupby(
        ['shstReferenceId', 'type', 'lanes'])['station'].count().sort_values(ascending=False).reset_index()[[
            'shstReferenceId', 'type', 'lanes']].drop_duplicates(subset=['shstReferenceId', 'type'])
    WranglerLogger.debug('pems_lanes_by_shstRef_type_df has {} rows, with header:\n{}'.format(
        pems_lanes_by_shstRef_type_df.shape[0],
        pems_lanes_by_shstRef_type_df.head(10)
    ))
    # then, expand the dataframe so 'type' is in columns
    pems_lanes_by_shstRef_type_df = pems_lanes_by_shstRef_type_df.pivot_table(
        index=['shstReferenceId'], values='lanes', columns='type').fillna(0).reset_index()

    # get a dataframe of all PEMS stations matched to each shstReferenceId
    link_pems_station_df = pems_conflation_result.loc[
        (pems_conflation_result['year'].isin([2014, 2015, 2016]))].drop_duplicates(
            subset=['shstReferenceId', 'station']).groupby(
                ['shstReferenceId'])['station'].apply(list).reset_index().rename(columns={'station': 'PEMS_station_ID'})
    WranglerLogger.debug('link_pems_station_df has {} rows, with header:\n{}'.format(
        link_pems_station_df.shape[0],
        link_pems_station_df.head(10)
    ))
    # merge the two
    pems_lanes_df = pd.merge(pems_lanes_by_shstRef_type_df,
                             link_pems_station_df,
                             how='left',
                             on='shstReferenceId')
    # rename
    pems_lanes_df.rename(columns={'FF': 'pems_lanes_FF',
                                  'FR': 'pems_lanes_FR',
                                  'HV': 'pems_lanes_HV',
                                  'ML': 'pems_lanes_ML',
                                  'OR': 'pems_lanes_OR'},
                         inplace=True)
    WranglerLogger.info('Finished conflating PEMS lane counts, got PEMS lane counts for {} unique shst links'.format(
        pems_lanes_df.shstReferenceId.nunique()
    ))

    # merge PEMS conflation result into link_with_third_party_gdf
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         pems_lanes_df,
                                         how='left',
                                         on='shstReferenceId')

    WranglerLogger.info('after conflation, {:,} links with the follow columns: \n{}'.format(
        link_with_third_party_gdf.shape[0],
        list(link_with_third_party_gdf)))

    ####################################
    # TODO: resolve duplicated shstReferenceId; do lane/name heuristics here instead of exporting
    # Write out conflation result data base - this will be used to create a number of 'lookup' tables to be used in
    # lane heuristics and QAQC
    WranglerLogger.info('Write out third-party conflation result data base')

    # convert tomtom FRC to standard road type
    tomtom_FRC_dict = {
        0: "0-Motorway, Freeway, or Other Major Road",
        1: "1-Major Road Less Important than a Motorway",
        2: "2-Other Major Road",
        3: "3-Secondary Road",
        4: "4-Local Connecting Road",
        5: "5-Local Road of High Importance",
        6: "6-Local Road",
        7: "7-Local Road of Minor Importance",
        8: "8-Other Road"
    }
    link_with_third_party_gdf['tomtom_FRC_def'] = link_with_third_party_gdf['tomtom_FRC'].map(tomtom_FRC_dict)
    WranglerLogger.debug('TomTom FRC standardized value counts:\n{}'.format(
        link_with_third_party_gdf.tomtom_FRC_def.value_counts(dropna=False)))

    # convert legacy TM2 data FT to standard road type
    TM2_FT_dict = {
        0: "0-Connector",
        1: "1-Freeway to Freeway",
        2: "2-Freeway",
        3: "3-Expressway",
        4: "4-Collector",
        5: "5-Ramp",
        6: "6-Special Facility",
        7: "7-Major Arterial",
    }
    link_with_third_party_gdf['TM2nonMarin_FT_def'] = link_with_third_party_gdf['TM2nonMarin_FT'].map(TM2_FT_dict)
    WranglerLogger.debug('TM2nonMarin FT standardized value counts:\n{}'.format(
        link_with_third_party_gdf.TM2nonMarin_FT_def.value_counts(dropna=False)))

    # write out conflation data base
    conflation_result_fields = [
        'shstReferenceId', 'roadway', 'lanes_tot', 'drive_access', 'bike_access', 'walk_access',
        'tomtom_FRC', 'tomtom_FRC_def', 'tomtom_lanes', 'tomtom_link_id', 'F_JNCTID', 'T_JNCTID',
        'tomtom_name', 'tomtom_shieldnum', 'tomtom_rtedir',
        'TM2Marin_A', 'TM2Marin_B', 'TM2Marin_FT', 'TM2Marin_LANES', 'TM2Marin_ASSIGNABLE',
        'TM2nonMarin_A', 'TM2nonMarin_B', 'TM2nonMarin_FT', 'TM2nonMarin_FT_def', 'TM2nonMarin_LANES',
        'TM2nonMarin_ASSIGNABLE',
        'sfcta_A', 'sfcta_B', "sfcta_STREETNAME", 'sfcta_FT', 'sfcta_LANE_AM', 'sfcta_LANE_OP', 'sfcta_LANE_PM',
        'ACTC_A', 'ACTC_B', 'ACTC_base_lanes_min', 'ACTC_base_lanes_max', 'ACTC_nmt2010_min', 'ACTC_nmt2010_max',
        'ACTC_nmt2020_min', 'ACTC_nmt2020_max',
        'CCTA_ID', 'CCTA_base_lanes_min', 'CCTA_base_lanes_max',
        'pems_lanes_FF', 'pems_lanes_FR', 'pems_lanes_HV', 'pems_lanes_ML',
        'pems_lanes_OR', 'PEMS_station_ID',
    ]

    link_conflation_fields_gdf = link_with_third_party_gdf[conflation_result_fields].rename(
        columns={'lanes_tot': 'lanes_tot_osm',
                 'tomtom_link_id': 'tomtom_unique_id'})

    WranglerLogger.info('export conflation fields to {}'.format(CONFLATION_SUMMARY_FILE))
    link_conflation_fields_gdf.to_csv(CONFLATION_SUMMARY_FILE, index=False)

    ####################################
    # Write out standard links
    # TODO: decide which columns to include.
    # Note: the initial pipeline method only writes out columns included in the input shst_osmnx_gdf; all columns
    # from third-party are exported as a .csv file.
    WranglerLogger.info('Saving links to {}'.format(CONFLATED_LINK_GEOFEATHER_FILE))
    geofeather.to_geofeather(link_with_third_party_gdf, CONFLATED_LINK_GEOFEATHER_FILE)

    WranglerLogger.info('Done')
