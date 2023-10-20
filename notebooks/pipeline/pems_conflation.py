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

# third-party data matching results
THIRD_PARTY_MATCHED_DIR = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data')

# TODO: move PEMS conflation out of Pipeline to validation
INPUT_PEMS_FILE = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data', 'PeMS', 'input', 'pems_period.csv')
PEMS_MATCHED_DIR = os.path.join(THIRD_PARTY_MATCHED_DIR, 'pems')


if __name__ == '__main__':
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

    # 4. for those that failed in nearest sheildnum+direction match, most likely due to falling out of the 100-meter offset boundary,
    # use nearest match only based on facility type
    # get PEMS station+type+location that failed to find nearest match
    pems_ft_matched_df = pd.DataFrame()
    
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

    # TODO: decide if 5. is needed
    # 5. finally, use ShSt match to fill in unmatched
    pems_shst_matched_df = pd.DataFrame()

    # 6. concatenate matching results from the two methods
    pems_sheild_dir_matched_gdf = pems_nearest_gdf.loc[pems_nearest_gdf.shstReferenceId.notnull()]
    pems_conflation_result = pd.concat([pems_sheild_dir_matched_gdf, pems_ft_matched_df, pems_shst_matched_df],
                                       sort=False,
                                       ignore_index=True)

    # 7. post-process PEMS matching results
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

    # recode 'lanes_[]' fields into 'lanes' and 'ft' fields
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_FR'].isnull(), 'pems_ft'] = 'Ramp'
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_OR'].isnull(), 'pems_ft'] = 'Ramp'
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_FF'].isnull(), 'pems_ft'] = 'Freeway to Freeway'
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_ML'].isnull(), 'pems_ft'] = 'Freeway'
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_HV'].isnull(), 'pems_ft'] = 'Freeway'

    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_FR'].isnull(), 'pems_lanes'] = pems_lanes_df['pems_lanes_FR']
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_OR'].isnull(), 'pems_lanes'] = pems_lanes_df['pems_lanes_OR']
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_FF'].isnull(), 'pems_lanes'] = pems_lanes_df['pems_lanes_FF']
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_ML'].isnull(), 'pems_lanes'] = pems_lanes_df['pems_lanes_ML']
    pems_lanes_df.loc[~pems_lanes_df['pems_lanes_HV'].isnull(), 'pems_lanes'] = pems_lanes_df['pems_lanes_HV'] + pems_lanes_df['pems_lanes']

    # merge PEMS conflation result into link_with_third_party_gdf
    link_with_third_party_gdf = pd.merge(link_with_third_party_gdf,
                                         pems_lanes_df,
                                         how='left',
                                         on='shstReferenceId')

    WranglerLogger.info('after conflation, {:,} links with the follow columns: \n{}'.format(
        link_with_third_party_gdf.shape[0],
        list(link_with_third_party_gdf)))
