USAGE = """
Merge the SharedStreets match results in step4b with the base networks data (ShSt+OSM) created in Step 3.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: 
    - standard network data from step3
    - third-party network data conflation results from step4b
    - 

Outputs:  

"""
import methods
import pandas as pd
import geopandas as gpd
import geofeather
from pyproj import CRS
import os, datetime
import numpy as np
import requests
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import fiona
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
SHST_OSMNX_LINK_FILE = os.path.join(INPUT_DATA_DIR, 'interim', 'step3_join_shst_with_osm', 'step3_link.feather')
# third-party data matching results
THIRD_PARTY_MATCHED_DIR = os.path.join(INPUT_DATA_DIR, 'interim', 'step4b_third_party_shst_match')
TOMTOM_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TomTom')
TM2_nonMarin_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_nonMarin')
TM2_Marin_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_Marin')
SFCTA_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'sfcta')
CCTA_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'ccta')
ACTC_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'actc')
# PEMS_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'pems')
# third-party raw data with all attributes
THIRD_PARTY_ALL_ATTRS = os.path.join(
    INPUT_DATA_DIR, 'external', 'step4a_third_party_data', 'modified', 'modified_all_attrs.gpkg')
TOMTOM_LAYER = 'tomtom_allAttrs'
TM2_nonMarin_LAYER = 'tm2nonMarin_allAttrs'
TM2_Marin_LAYER = 'tm2Marin_allAttrs'
SFCTA_LAYER = 'sfcta_allAttrs'
CCTA_LAYER = 'ccta_allAttrs'
ACTC_LAYER = 'actc_allAttrs'
# PEMS
INPUT_PEMS_FILE = os.path.join('external', 'step4a_third_party_data', 'raw', 'mtc', 'pems_period.csv')

# TOMTOM_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'TomTom', 'tomtom_raw.geojson')
# TM2_nonMarin_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'TM2_nonMarin', 'tm2nonMarin_raw.geojson')
# TM2_Marin_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'TM2_Marin', 'tm2Marin_raw.geojson')
# SFCTA_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'sfcta', 'sfcta_raw.geojson')
# CCTA_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'ccta', 'ccta_raw.geojson')
# ACTC_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'actc', 'actc_raw.geojson')
# PEMS_RAW_FILE = os.path.join(THIRD_PARTY_RAW_DIR, 'pems', 'pems_raw.geojson')


# output
CONFLATION_RESULT_DIR = os.path.join(OUTPUT_DATA_DIR, 'interim', 'step4d_conflate_with_third_party')
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
    # TomTom conflation

    # Read tomtom ShSt match result
    WranglerLogger.info('loading TomTom conflation result from {}'.format(TOMTOM_MATCHED_DIR))
    tomtom_match_gdf = methods.read_shst_matched(TOMTOM_MATCHED_DIR, 'tomtom_*.out.matched.geojson')

    tomtom_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                     'shstToIntersectionId'  : 'toIntersectionId',
                                     'pp_tomtom_link_id'     : 'tomtom_link_id'},
                            inplace=True)
    WranglerLogger.info('TomTom conflation result has {} rows with columns: \n{}'.format(
        tomtom_match_gdf.shape[0], list(tomtom_match_gdf)))

    # read TomTom raw data with all link attributes
    WranglerLogger.info('loading TomTom all-attribute link data')
    tomtom_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=TOMTOM_LAYER)
    WranglerLogger.debug('TomTom raw data has {} rows with columns: \n{}'.format(
        tomtom_attr_gdf.shape[0], list(tomtom_attr_gdf)))

    WranglerLogger.info('Sharedstreets matched {} out of {} total TomTom Links.'.format(
        tomtom_match_gdf.tomtom_link_id.nunique(), tomtom_attr_gdf.shape[0]))

    # merge
    WranglerLogger.info('join TomTom all link attributes back to the matched links')
    tomtom_match_with_att_gdf = pd.merge(tomtom_match_gdf,
                                         tomtom_attr_gdf[['tomtom_link_id', 'ID',
                                                          'F_JNCTID', 'T_JNCTID',
                                                          'LANES', 'FRC', 'NAME', 'SHIELDNUM', 'RTEDIR']],
                                         how='left',
                                         on='tomtom_link_id')

    # drop duplicates - duplicates may come from links crossing the subregion boundaries; also, when more than one
    # TomTom links are matched to the same sharedstreets link.
    # TODO: there are cases when two TomTom links were matched to the same shst link, it is possible to use the 'score' field to chose one?
    unique_tomtom_match_gdf = tomtom_match_with_att_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])
    WranglerLogger.info('after dropping duplicates, {} TomTom links remain'.format(
        unique_tomtom_match_gdf.shape[0]))

    # add data source prefix to column names
    unique_tomtom_match_gdf.rename(columns={"ID": "tomtom_ID",
                                            "LANES": "tomtom_lanes",
                                            "FRC": "tomtom_FRC",
                                            "NAME": "tomtom_name",
                                            "SHIELDNUM": "tomtom_shieldnum",
                                            "RTEDIR": "tomtom_rtedir"},
                                   inplace=True)

    # print out unique values for each key attribute to help fix typos
    for attribute in ['tomtom_lanes', 'tomtom_FRC', 'tomtom_shieldnum', 'tomtom_rtedir']:
        WranglerLogger.debug('{} unique values:\n{}'.format(attribute,
                                                            unique_tomtom_match_gdf.unique()))
    # fix 'tomtom_shielfnum' = ' ' and tomtom_rtedir = ' '
    unique_tomtom_match_gdf.loc[unique_tomtom_match_gdf.tomtom_shieldnum == ' ', 'tomtom_shieldnum'] = ''
    unique_tomtom_match_gdf.loc[unique_tomtom_match_gdf.tomtom_rtedir == ' ', 'tomtom_rtedir'] = ''

    # join tomtom data with base network
    WranglerLogger.info('add TomTom attributes to base network from step3')
    link_with_tomtom_gdf = pd.merge(link_gdf,
                                    unique_tomtom_match_gdf[[
                                        'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId',
                                        'tomtom_link_id', "tomtom_ID", 'F_JNCTID', 'T_JNCTID',
                                        "tomtom_lanes", "tomtom_FRC", "tomtom_name", "tomtom_shieldnum",
                                        "tomtom_rtedir"]],
                                    how="left",
                                    on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
                                    )

    WranglerLogger.debug('{:,} base network links have TomTom attributes'.format(
        (link_with_tomtom_gdf.tomtom_link_id.notnull()).sum()))

    ####################################
    # Load and process other third-party data conflation results
    # For each data source, do three things:
    #  1) read shst match result and consolidate into one dataframe if there are multiple []_matched.geojson files
    #  2) read raw data with all link attributes
    #  3) join link attributes back to shst match result

    # TM2 nonMarin data
    # read shst match result
    WranglerLogger.info('read TM2 nonMarin conflation result from folder {}'.format(TM2_nonMarin_MATCHED_DIR))
    tm2nonMarin_match_gdf = methods.read_shst_matched(TM2_nonMarin_MATCHED_DIR, "tm2nonMarin_*.out.matched.geojson")
    # rename columns
    tm2nonMarin_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                          'shstToIntersectionId'  : 'toIntersectionId',
                                          'pp_a'                  : 'A',
                                          'pp_b'                  : 'B'},
                                 inplace=True)
    # drop duplicates
    unique_tm2nonMarin_match_gdf = tm2nonMarin_match_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    # read raw data with all link attributes
    WranglerLogger.info('read TM2 nonMarin all-attribute link data')
    tm2_nonMarin_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=TM2_nonMarin_LAYER)

    # join link attributes back to shst match result
    WranglerLogger.info('join link attributes back to match result')
    unique_tm2nonMarin_match_gdf = pd.merge(unique_tm2nonMarin_match_gdf,
                                            tm2_nonMarin_attr_gdf[['A', 'B', 'NUMLANES', 'FT', 'ASSIGNABLE']],
                                            how='left',
                                            on=['A', 'B'])
    # add data source prefix to column names
    unique_tm2nonMarin_match_gdf.rename(columns={'A': 'TM2nonMarin_A',
                                                 'B': 'TM2nonMarin_B',
                                                 'NUMLANES': 'TM2nonMarin_LANES',
                                                 'FT': 'TM2nonMarin_FT',
                                                 'ASSIGNABLE': 'TM2nonMarin_ASSIGNABLE'},
                                        inplace=True)

    # TM2 Marin data
    # read shst match result
    WranglerLogger.info('read TM2 Marin conflation result from folder {}'.format(TM2_Marin_MATCHED_DIR))
    tm2marin_match_gdf = methods.read_shst_matched(TM2_Marin_MATCHED_DIR, "tm2Marin_*.out.matched.geojson")
    # rename columns
    tm2marin_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                       'shstToIntersectionId'  : 'toIntersectionId',
                                       'pp_a'                  : 'A',
                                       'pp_b'                  : 'B'},
                              inplace=True)
    # drop duplicates
    unique_tm2marin_match_gdf = tm2marin_match_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    # read raw data with all link attributes
    WranglerLogger.info('read TM2_Marin all-attribute link data')
    tm2_marin_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=TM2_Marin_LAYER)

    # join link attributes back to shst match result
    WranglerLogger.info('join link attributes back to match result')
    unique_tm2marin_match_gdf = pd.merge(unique_tm2marin_match_gdf,
                                         tm2_marin_attr_gdf[['A', 'B', 'NUMLANES', 'FT', 'ASSIGNABLE']],
                                         how='left',
                                         on=['A', 'B'])
    # add data source prefix to column names
    unique_tm2marin_match_gdf.rename(columns={'A': 'TM2Marin_A',
                                              'B': 'TM2Marin_B',
                                              'NUMLANES': 'TM2Marin_LANES',
                                              'FT': 'TM2Marin_FT',
                                              'ASSIGNABLE': "TM2Marin_ASSIGNABLE"},
                                     inplace=True)

    # SFCTA data
    # read shst match result
    WranglerLogger.info('read sfcta stick network conflation result from folder {}'.format(SFCTA_MATCHED_DIR))
    sfcta_stick_match_gdf = methods.read_shst_matched(SFCTA_MATCHED_DIR, "*sfcta.out.matched.geojson")
    # rename columns
    sfcta_stick_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                          'shstToIntersectionId'  : 'toIntersectionId',
                                          'pp_a'                  : 'A',
                                          'pp_b'                  : 'B'},
                                 inplace=True)
    # drop duplicates
    unique_sfcta_match_gdf = sfcta_stick_match_gdf.drop_duplicates(
        subset=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId'])

    # read raw data with all link attributes
    WranglerLogger.info('read SFCTA stick network all-attribute link data')
    sfcta_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=SFCTA_LAYER)

    # join link attributes back to shst match result
    WranglerLogger.info('join link attributes back to match result')
    unique_sfcta_match_gdf = pd.merge(unique_sfcta_match_gdf,
                                      sfcta_attr_gdf[['A', 'B', 'FT', 'STREETNAME', 'LANE_AM', 'LANE_OP', 'LANE_PM']],
                                      how='left',
                                      on=['A', 'B'])
    # add data source prefix to column names
    unique_sfcta_match_gdf.rename(columns={"A"         : "sfcta_A",
                                           "B"         : "sfcta_B",
                                           "FT"        : "sfcta_FT",
                                           "STREETNAME": "sfcta_STREETNAME",
                                           "LANE_AM"   : "sfcta_LANE_AM",
                                           "LANE_OP"   : "sfcta_LANE_OP",
                                           "LANE_PM"   : "sfcta_LANE_PM"},
                                  inplace=True)

    # CCTA data
    # read shst match result
    WranglerLogger.info('read CCTA conflation result from folder {}'.format(CCTA_MATCHED_DIR))
    ccta_match_gdf = methods.read_shst_matched(CCTA_MATCHED_DIR, 'ccta_*.out.matched.geojson')
    # rename columns
    ccta_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                   'shstToIntersectionId'  : 'toIntersectionId',
                                   'pp_id'                 : 'ID'},
                          inplace=True)
    # drop duplicates
    # TODO: for CCTA and ACTC, WSP appeared to have changed the conflation post-processing methodology. For other data
    # sources, when multiple third-party links were matched to one shst link, only one is kept; for CCTA/ACTC, all kept
    unique_ccta_match_gdf = ccta_match_gdf.drop_duplicates()

    # read raw data with all link attributes
    WranglerLogger.info('read CCTA all-attribute link data')
    ccta_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=CCTA_LAYER)

    # join link attributes back to shst match result
    WranglerLogger.info('join link attributes back to match result')
    unique_ccta_match_gdf = pd.merge(unique_ccta_match_gdf,
                                     ccta_attr_gdf[['ID', 'AB_LANES']],
                                     on='ID',
                                     how='left')

    # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId, including
    # when multiple ccta links have been joined to the same shstReferenceId
    ccta_lanes_conflation_df = unique_ccta_match_gdf[
        unique_ccta_match_gdf['AB_LANES'] > 0
        ].groupby(
        ['shstReferenceId']
    )['AB_LANES'].apply(list).to_frame().reset_index()

    ccta_lanes_conflation_df['base_lanes_min'] = ccta_lanes_conflation_df['AB_LANES'].apply(lambda x: min(set(x)))
    ccta_lanes_conflation_df['base_lanes_max'] = ccta_lanes_conflation_df['AB_LANES'].apply(lambda x: max(set(x)))
    WranglerLogger.debug()
    # TODO: decide if export or merge into the base network
    ccta_lanes_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'cctamodel_legacy_lanes.csv'), index=False)

    # add data source prefix to column names
    unique_ccta_match_gdf.rename(columns={'A': 'CCTA_A',
                                          'B': 'CCTA_B',
                                          'base_lanes_min': 'CCTA_base_lanes_min',
                                          'base_lanes_max': 'CCTA_base_lanes_max'},
                                 inplace=True)

    # ACTC data
    # read shst match result
    WranglerLogger.info('read ACTC all-attribute link data')
    actc_match_gdf = methods.read_shst_matched(ACTC_MATCHED_DIR, 'actc_*.out.matched.geojson')
    # rename columns
    actc_match_gdf.rename(columns={'shstFromIntersectionId': 'fromIntersectionId',
                                   'shstToIntersectionId': 'toIntersectionId',
                                   'pp_a': 'A',
                                   'pp_b': 'B'},
                          inplace=True)
    # TODO: reconcile different methodologies for dropping duplicates
    unique_actc_match_gdf = actc_match_gdf.drop_duplicates()

    # read raw data with all link attributes
    WranglerLogger.info('read ACTC all-attribute link data')
    actc_attr_gdf = gpd.read_file(THIRD_PARTY_ALL_ATTRS, layer=ACTC_LAYER)

    # join link attributes back to shst match result
    WranglerLogger.info('join link attributes back to match result')
    unique_actc_match_gdf = pd.merge(unique_actc_match_gdf,
                                     actc_attr_gdf[['A', 'B', 'BASE_LN', 'NMT2010', 'NMT2020']],
                                     how='left',
                                     on=['A', 'B']
                                     )

    # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId
    actc_lanes_conflation_df = unique_actc_match_gdf.loc[unique_actc_match_gdf['BASE_LN'] > 0].groupby(
        ['shstReferenceId']
    )['BASE_LN'].apply(list).to_frame().reset_index()

    actc_lanes_conflation_df['base_lanes_min'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: min(set(x)))
    actc_lanes_conflation_df['base_lanes_max'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_lanes_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'actcmodel_legacy_lanes.csv'), index=False)

    # same for bike lane
    actc_bike_conflation_df = unique_actc_match_gdf.groupby(
        ['shstReferenceId']
    )[['NMT2010', 'NMT2020']].agg(lambda x: list(x)).reset_index()

    actc_bike_conflation_df['nmt2010_min'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2010_max'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: max(set(x)))
    actc_bike_conflation_df['nmt2020_min'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2020_max'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_bike_conflation_df.to_csv(os.path.join(CONFLATION_RESULT_DIR, 'actcmodel_legacy_bike.csv'), index=False)

    # add data source prefix to column names
    unique_actc_match_gdf.rename(columns={'A': 'ACTC_A',
                                          'B': 'ACTC_B',
                                          'base_lanes_min': 'ACTC_base_lanes_min',
                                          'base_lanes_max': 'ACTC_base_lanes_max',
                                          'nmt2010_min': 'ACTC_nmt2010_min',
                                          'nmt2010_max': 'ACTC_nmt2010_max',
                                          'nmt2020_min': 'ACTC_nmt2020_min',
                                          'nmt2020_max': 'ACTC_nmt2020_max'},
                                 inplace=True)

    ####################################
    # TODO: Conflate PEMS data

    # load PEMS raw data
    WranglerLogger.info('Loading PEMS raw data from {}'.format(INPUT_PEMS_FILE))
    pems_raw_df = pd.read_csv(INPUT_PEMS_FILE)

    WranglerLogger.info('drop points without complete longitude and latitude info')
    pems_df = pems_raw_df[~((pems_raw_df.longitude.isnull()) | (pems_raw_df.latitude.isnull()))]
    WranglerLogger.debug('after dropping, PEMS data went from {} rows to {} rows'.format(pems_raw_df.shape[0],
                                                                                         pems_df.shape[0]))

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
    # subset link_with_tomtom_gdf
    tomtom_for_pems_conflation_gdf = link_with_tomtom_gdf[list(link_gdf) + ['tomtom_shieldnum', 'tomtom_rtedir']]
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
        tomtom_for_pems_conflation_gdf.groupby(['tomtom_shieldnum', 'tomtom_rtedir'])['shstReferenceId'].count()))
    WranglerLogger.debug('PEMS unique route+direction comb:\n{}'.format(
        pems_gdf.groupby(['route', 'direction'])['station'].count()))

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
    pems_nearest_match = methods.pems_station_nearest_match(pems_gdf,
                                                            tomtom_for_pems_conflation_gdf,
                                                            pems_type_roadway_crosswalk)

    # 3. merge it back to pems_gdf
    WranglerLogger.info('Merging PEMS nearest matching result back to pems_gdf')
    pems_nearest_gdf = pd.merge(pems_gdf,
                                pems_nearest_match.drop(['point', 'geometry'], axis=1),
                                how='left',
                                on=['station', 'longitude', 'latitude', 'route', 'direction', 'type'])
    WranglerLogger.info('Finished PEMS nearest matching')
    WranglerLogger.debug('{} out of {} rows of PEMS data found a matching link, with "type" value counts:\n{}'.format(
        (pems_nearest_gdf.shstReferenceId.notnull()).sum(),
        pems_nearest_gdf.shape[0],
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.notnull()]['type'].value_counts()
    ))
    WranglerLogger.debug('{} rows of PEMS data failed to find a matching link, with "type" value counts:\n{}'.format(
        (pems_nearest_gdf.shstReferenceId.isnull()).sum(),
        pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.isnull()]['type'].value_counts()
    ))

    # TODO (?) for these that failed in nearest match, use sharedstreets conflation result


    ####################################
    # Join link attributes from other third-party data to base+TomTom network

    link_all_conflated_gdf = pd.merge(
        link_with_tomtom_gdf,
        unique_tm2nonMarin_match_gdf[['shstReferenceId', 'shstGeometryId', 'fromIntersectionId',
                                      'toIntersectionId', 'TM2nonMarin_A', 'TM2nonMarin_B',
                                      'TM2nonMarin_LANES', 'TM2nonMarin_FT', 'TM2nonMarin_ASSIGNABLE']],
        how='left',
        on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
    )

    link_all_conflated_gdf = pd.merge(
        link_all_conflated_gdf,
        unique_tm2marin_match_gdf[['shstReferenceId', 'shstGeometryId', 'fromIntersectionId',
                                   'toIntersectionId', 'TM2Marin_A', 'TM2Marin_B', 'TM2Marin_LANES', 'TM2Marin_FT',
                                   'TM2Marin_ASSIGNABLE']],
        how='left',
        on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
    )

    link_all_conflated_gdf = pd.merge(
        link_all_conflated_gdf,
        unique_sfcta_match_gdf[['shstReferenceId', 'shstGeometryId', 'fromIntersectionId',
                                'toIntersectionId', 'sfcta_A', 'sfcta_B', 'sfcta_FT', 'sfcta_STREETNAME',
                                'sfcta_LANE_AM',
                                'sfcta_LANE_OP',
                                'sfcta_LANE_PM']],
        how='left',
        on=['shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId']
    )

    WranglerLogger.info('after conflation, {:,} links with the follow columns: \n{}'.format(
        link_all_conflated_gdf.shape[0],
        list(link_all_conflated_gdf)))

    ####################################
    # TODO: resolve duplicated shstReferenceId

    ####################################
    # Write out standard links
    # TODO: decide which columns to include.
    # Note: the initial pipeline method only writes out columns included in the input shst_osmnx_gdf; all columns
    # from third-party are exported as a .csv file.
    WranglerLogger.info('Saving links to {}'.format(CONFLATED_LINK_GEOFEATHER_FILE))
    geofeather.to_geofeather(link_all_conflated_gdf, CONFLATED_LINK_GEOFEATHER_FILE)


    ####################################
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
    link_all_conflated_gdf['tomtom_FRC_def'] = link_all_conflated_gdf['tomtom_FRC'].map(tomtom_FRC_dict)
    WranglerLogger.debug('TomTom FRC standardized value counts:\n{}'.format(
        link_all_conflated_gdf.tomtom_FRC_def.value_counts(dropna=False)))

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
    link_all_conflated_gdf['TM2nonMarin_FT_def'] = link_all_conflated_gdf['TM2nonMarin_FT'].map(TM2_FT_dict)
    WranglerLogger.debug('TM2nonMarin FT standardized value counts:\n{}'.format(
        link_all_conflated_gdf.TM2nonMarin_FT_def.value_counts(dropna=False)))

    # write out conflation data base
    conflation_result_fields = [
        'shstReferenceId', 'roadway', 'lanes_tot', 'drive_access', 'bike_access', 'walk_access',
        'tomtom_FRC', 'tomtom_FRC_def', 'tomtom_lanes', 'tomtom_link_id', 'F_JNCTID', 'T_JNCTID',
        'tomtom_name', 'tomtom_shieldnum', 'tomtom_rtedir',
        'TM2Marin_A', 'TM2Marin_B', 'TM2Marin_FT', 'TM2Marin_LANES', 'TM2Marin_ASSIGNABLE',
        'TM2nonMarin_A', 'TM2nonMarin_B', 'TM2nonMarin_FT', 'TM2nonMarin_FT_def', 'TM2nonMarin_LANES', 'TM2nonMarin_ASSIGNABLE',
        'sfcta_A', 'sfcta_B', "sfcta_STREETNAME", 'sfcta_FT', 'sfcta_LANE_AM', 'sfcta_LANE_OP', 'sfcta_LANE_PM',
        'ACTC_A', 'ACTC_B', 'ACTC_base_lanes_min', 'ACTC_base_lanes_max', 'ACTC_nmt2010_min', 'ACTC_nmt2010_max',
        'ACTC_nmt2020_min', 'ACTC_nmt2020_max',
        'CCTA_A', 'CCTA_B', 'CCTA_base_lanes_min', 'CCTA_base_lanes_max'
    ]
    
    link_conflation_fields_gdf = link_all_conflated_gdf[conflation_result_fields].rename(
        columns={'lanes_tot': 'lanes_tot_osm',
                 'tomtom_link_id': 'tomtom_unique_id'})
    
    WranglerLogger.info('export conflation fields to {}'.format(CONFLATION_SUMMARY_FILE))
    link_conflation_fields_gdf.to_csv(CONFLATION_SUMMARY_FILE, index=False)
    
    WranglerLogger.info('Done')
