USAGE = """
Prepares third-party data for SharedStreet conflation.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: third-party data sources, including -
    - TomTom Bay Area network
    - TM2 non-Marion version network
    - TM2 Marin version network
    - SFCTA Stick network
    - CCTA 2015 network
    - ACTC network
    - PEMS count
Outputs: two sets of data for each of the above sources:
    - data only with 'geometry' and identification, to be used in SharedStreet matching;
      also partitioned to match ShSt sub-region boundaries
    - modified_all_attrs.gpkg: data with all link attributes, to be joined back to the SharedStreet matching result  

"""
import argparse, datetime, os, sys
import methods
import docker
import pandas as pd
import geopandas as gpd
import fiona
import geofeather
from shapely.geometry import Point, shape, LineString
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging

# third-party network data types
TOMTOM        = 'TomTom'
TM2_NON_MARIN = 'TM2_nonMarin'
TM2_MARIN     = 'TM2_Marin'
SFCTA         = 'SFCTA'
CCTA          = 'CCTA'
ACTC          = 'ACTC'
PEMS          = 'PeMS'

#####################################
# EPSG requirement
# LAT_LONG_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)
# NEAREST_MATCH_EPSG = 26915
nearest_match_epsg_str = 'epsg:{}'.format(methods.NEAREST_MATCH_EPSG)
WranglerLogger.info('nearest match ESPG: {}'.format(nearest_match_epsg_str))

#####################################
# inputs and outputs
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# sub-region boundary polygons to split third-party data
BOUNDARY_DIR = os.path.join(INPUT_DATA_DIR, 'step0_boundaries')

# third-party network data inputs
THIRD_PARTY_INPUT_DIR = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data')
THIRD_PARTY_INPUT_FILES = {
    TOMTOM          : os.path.join(THIRD_PARTY_INPUT_DIR, TOMTOM,        'input', 'network2019', 'Network_region.gdb'),
    TM2_NON_MARIN   : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_NON_MARIN, 'input', 'mtc_final_network_base.shp'),
    TM2_MARIN       : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_MARIN,     'input', 'mtc_final_network_base.shp'),
    SFCTA           : os.path.join(THIRD_PARTY_INPUT_DIR, SFCTA,         'input', 'SanFrancisco_links.shp'),
    CCTA            : os.path.join(THIRD_PARTY_INPUT_DIR, CCTA,          'input', 'ccta_2015_network.shp'),
    ACTC            : os.path.join(THIRD_PARTY_INPUT_DIR, ACTC,          'input', 'AlamedaCo_MASTER_20190410_no_cc.shp'),
    PEMS            : os.path.join(THIRD_PARTY_INPUT_DIR, PEMS,          'input', 'pems_period.csv')
}
THIRD_PARTY_OUTPUT_DIR  = os.path.join(OUTPUT_DATA_DIR, 'step4_third_party_data')
# conflation will be done in [THIRD_PARTY_OUTPUT_DIR]/[third_party]/conflation_shst except for PEMS which uses nearest point method
CONFLATION_SHST = 'conflation_shst'
CONFLATION_PEMS_= os.path.join(THIRD_PARTY_OUTPUT_DIR, PEMS, 'nearest_match')



def conflate_TOMTOM():
    """
    Conflate TomTom data with sharedstreets
    TODO: What files are written?  Where is documentation on TomTom columns?
    """
    # Prepare tomtom for conflation
    WranglerLogger.info('Reading TomTom data from {}'.format(THIRD_PARTY_INPUT_FILES[TOMTOM]))

    # print out all the layers from the .gdb file
    layers = fiona.listlayers(THIRD_PARTY_INPUT_FILES[TOMTOM])
    WranglerLogger.info('TomTom gdb has the following layers: {}'.format(layers))
    # load tomtom data, use the street link layer
    WranglerLogger.info('loading TomTom raw data from layer mn_nw')
    tomtom_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TOMTOM], layer='mn_nw')

    # convert to ESPG lat-lon
    tomtom_raw_gdf = tomtom_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tomtom_raw_gdf.crs))

    WranglerLogger.info('total {:,} tomtom links'.format(tomtom_raw_gdf.shape[0]))

    # There is no existing unique tomtom handle for Bay Area, thus we need to create unique handle
    WranglerLogger.info('ID + F_JNCTID + T_JNCTID: {}'.format(
        len(tomtom_raw_gdf.groupby(["ID", "F_JNCTID", "T_JNCTID"]).count())))

    # generating unique handle for tomtom
    tomtom_raw_gdf['tomtom_link_id'] = range(1, len(tomtom_raw_gdf) + 1)

    (tomtom_matched_gdf, tomtom_unmatched_gdf) = methods.conflate(
        TOMTOM, tomtom_raw_gdf, ['tomtom_link_id'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.debug('TomTom has the following dtypes:\n{}'.format(tomtom_raw_gdf.dtypes))
    WranglerLogger.info('finished conflating TomTom data')

def conflate_TM2_NON_MARIN():
    """
    Conflate TM2 (NonMarin) data with sharedstreets
    TODO: What files are written?
    """
    # Prepare TM2 non-Marin for conflation
    WranglerLogger.info('loading TM2_nonMarin data from {}'.format(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN]))
    tm2_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN])
    WranglerLogger.debug('TM2_Marin data info: \n{}'.format(tm2_link_gdf.info()))

    # define initial ESPG
    tm2_link_gdf.crs = "esri:102646"

    # convert to ESPG lat-lon
    tm2_link_gdf = tm2_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_nonMarin link data CNTYPE stats: \n{}'.format(
        tm2_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_link_roadway_gdf = tm2_link_gdf.loc[tm2_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]
    WranglerLogger.info('\n out of {:,} links in TM2_non-Marin network, {:,} are roadway links'.format(
        tm2_link_gdf.shape[0],
        tm2_link_roadway_gdf.shape[0]))

    # double check with AB node pairs
    WranglerLogger.info('# of unique AB node pairs: {:,}'.format(
        tm2_link_roadway_gdf.groupby(["A", "B"]).count().shape[0]))

    # create conflation directory
    conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, TM2_NON_MARIN, CONFLATION_SHST)
    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
    # create docker container to do the shst matchting
    (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # Partition TM2 Non Marin for shst Match
    WranglerLogger.info('exporting TM2_nonMarin partitioned data to {}'.format(conflation_dir))
    for boundary_num in range(1,methods.NUM_SHST_BOUNDARIES+1):
        boundary_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_{:02d}.geojson'.format(boundary_num)))
        sub_gdf = tm2_link_roadway_gdf[tm2_link_roadway_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["A", "B", "geometry"]].to_file(
            os.path.join(conflation_dir, 'tm2nonMarin_{}.in.geojson'.format(boundary_num)),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    # before exporting, fix NAME that would cause encoding error
    tm2_link_roadway_gdf.loc[
        tm2_link_roadway_gdf.NAME.notnull() & \
        (tm2_link_roadway_gdf.NAME.str.contains('Vista Monta')) & \
        (tm2_link_roadway_gdf.NAME != 'Vista Montara C'), 'NAME'] = 'Vista Montana'

    # export modified raw data for merging the conflation results back
    output_file = os.path.join(conflation_dir, "tm2nonMarin.feather")
    WranglerLogger.info('exporting TM2_nonMarin with all attributes to {}'.format(output_file))
    geofeather.to_geofeather(tm2_link_roadway_gdf, output_file)

    WranglerLogger.debug('TM2_nonMarin data has the following dtypes:\n{}'.format(tm2_link_roadway_gdf.dtypes))
    WranglerLogger.info('finished conflating TM2_nonMarin data')

def conflate_TM2_MARIN():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare TM2 Marin for conflation
    WranglerLogger.info('loading TM2_Marin data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[TM2_MARIN])))
    tm2_marin_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_MARIN])
    WranglerLogger.debug('TM2_Marin data info: \n{}'.format(tm2_marin_link_gdf.info()))

    # define initial ESPG
    tm2_marin_link_gdf.crs = CRS("esri:102646")

    # convert to ESPG lat-lon
    tm2_marin_link_gdf = tm2_marin_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_marin_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_Marin link data CNTYPE stats: \n{}'.format(
        tm2_marin_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_marin_link_roadway_gdf = tm2_marin_link_gdf.loc[
        tm2_marin_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]
    WranglerLogger.info('\n out of {:,} links in TM2_Marin network, {:,} are roadway links'.format(
        tm2_marin_link_gdf.shape[0],
        tm2_marin_link_roadway_gdf.shape[0]))

    # double check with AB node pairs
    WranglerLogger.info('# of unique AB node pairs: {:,}'.format(
        tm2_marin_link_roadway_gdf.groupby(["A", "B"]).count().shape[0]))

    # create conflation directory
    conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, TM2_MARIN, CONFLATION_SHST)
    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
    # create docker container to do the shst matchting
    (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # Partition TM2 Marin for shst Match
    WranglerLogger.info('exporting TM2_Marin partitioned data to {}'.format(conflation_dir))
    for boundary_num in range(1,methods.NUM_SHST_BOUNDARIES+1):
        boundary_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_{:02d}.geojson'.format(boundary_num)))
        sub_gdf = tm2_marin_link_roadway_gdf[
            tm2_marin_link_roadway_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["A", "B", "geometry"]].to_file(
            os.path.join(conflation_dir, 'tm2Marin_{}.in.geojson'.format(boundary_num)),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    # before exporting, fix NAME that would cause encoding error
    tm2_marin_link_roadway_gdf.loc[
        tm2_marin_link_roadway_gdf.NAME.notnull() & \
        (tm2_marin_link_roadway_gdf.NAME.str.contains('Vista Monta')) & \
        (tm2_marin_link_roadway_gdf.NAME != 'Vista Montara C'), 'NAME'] = 'Vista Montana'

    # export modified raw data for merging the conflation results back
    output_file = os.path.join(conflation_dir, "tm2Marin.feather")
    WranglerLogger.info('exporting TM2_Marin with all attributes to {}'.format(output_file))
    geofeather.to_geofeather(tm2_marin_link_roadway_gdf, output_file)

    WranglerLogger.debug('TM2_Marin data has the following dtypes:\n{}'.format(tm2_marin_link_roadway_gdf.dtypes))
    WranglerLogger.info('finished conflating TM2_Marin data')

def conflcate_SFCTA():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare SFCTA for conflation
    WranglerLogger.info('loading SFCTA data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[SFCTA])))
    sfcta_stick_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[SFCTA])
    WranglerLogger.debug('SFCTA data info: \n{}'.format(sfcta_stick_gdf.info()))

    # set initial ESPG
    sfcta_stick_gdf.crs = CRS("EPSG:2227")
    # convert to ESPG lat-lon
    sfcta_stick_gdf = sfcta_stick_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(sfcta_stick_gdf.crs))

    # only conflate SF part of the network
    boundary_4_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_4.geojson'))
    sfcta_SF_gdf = sfcta_stick_gdf[
        sfcta_stick_gdf.intersects(boundary_4_gdf.geometry.unary_union)]

    # remove "special facility" (FT 6)
    sfcta_SF_roadway_gdf = sfcta_SF_gdf[~(sfcta_SF_gdf.FT == 6)]

    WranglerLogger.info('after removing links outside bounary_4 and FT=6, SF network has {:,} links, {:,} unique A-B combination'.format(
        sfcta_SF_roadway_gdf.shape[0], len(sfcta_SF_roadway_gdf.groupby(['A', 'B']).count())))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        SFCTA, sfcta_SF_roadway_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    # # create conflation directory
    # conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, SFCTA, CONFLATION_SHST)
    # if not os.path.exists(conflation_dir):
    #     WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
    #     os.makedirs(conflation_dir)

    # # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    # docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
    # # create docker container to do the shst matchting
    # (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # # Write out SFCTA stick network for conflation
    # sfcta_SF_roadway_gdf[['A', 'B', "geometry"]].to_file(
    #     os.path.join(conflation_dir, 'sfcta_in.geojson'), driver="GeoJSON")

    # # export modified raw data for merging the conflation results back
    # output_file = os.path.join(conflation_dir, "sfcta.feather")
    # WranglerLogger.info('exporting SFCTA with all attributes to {}'.format(output_file))
    # geofeather.to_geofeather(sfcta_SF_roadway_gdf, output_file)

    # WranglerLogger.debug('SFTCA data has the following dtypes: {}'.format(sfcta_SF_roadway_gdf.dtypes))
    WranglerLogger.info('Finished conflating SFCTA data')

def conflate_CCTA():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare CCTA for conflation
    WranglerLogger.info('loading CCTA data from {}'.format(THIRD_PARTY_INPUT_FILES[CCTA]))
    ccta_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[CCTA])
    WranglerLogger.debug('CCTA data info: \n{}'.format(ccta_raw_gdf.info()))

    WranglerLogger.info('CCTA data projection: ' + str(ccta_raw_gdf.crs))

    # filter out connectors
    ccta_gdf = ccta_raw_gdf.loc[(ccta_raw_gdf.AB_FT != 6) & (ccta_raw_gdf.BA_FT != 6)]
    WranglerLogger.info('CCTA data has {:,} rows, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # this network is from transcad, for one way streets, dir=1;
    # for two-way streets, there is only one links with dir=0, need to create other direction
    # from shapely.geometry import LineString
    two_way_links_gdf = ccta_gdf.loc[ccta_gdf.DIR == 0].copy()
    two_way_links_gdf["geometry"] = two_way_links_gdf.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]),
        axis=1)
    two_way_links_gdf.rename(columns={'AB_LANES': 'BA_LANES', 'BA_LANES': 'AB_LANES'}, inplace=True)
    two_way_links_gdf['ID'] = two_way_links_gdf['ID'] + 9000000

    ccta_gdf = pd.concat([ccta_gdf, two_way_links_gdf], sort=False, ignore_index=True)
    # double check
    WranglerLogger.info('after creating other direction for two-way roads, ccta data has {:,} links, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        CCTA, ccta_gdf, ['ID'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    #TODO: whatever we do next

    # WranglerLogger.debug('CCTA data has the following dtypes: {}'.format(ccta_gdf.dtypes))
    WranglerLogger.info('finished conflating CCTA data')

def conflate_ACTC():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?  Is there documentation on the ACTC input file fields?
    """
    WranglerLogger.info('loading ACTC data from {}'.format(THIRD_PARTY_INPUT_FILES[ACTC]))
    actc_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[ACTC])
    WranglerLogger.info('ACTC raw data has {:,} links, {:,} unique A-B combination'.format(
        actc_raw_gdf.shape[0], len(actc_raw_gdf.groupby(['A', 'B']).count())
    ))
    WranglerLogger.debug('ACTC raw data dtypes:\n{}'.format(actc_raw_gdf.dtypes))
    WranglerLogger.debug('ACTC crs:\n{}'.format(actc_raw_gdf.crs))

    # convert to ESPG lat-lon
    actc_raw_gdf = actc_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(actc_raw_gdf.crs))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 2.5-3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        ACTC, actc_raw_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    # # TODO: reconcile different methodologies for dropping duplicates
    # unique_actc_match_gdf = actc_matched_gdf.drop_duplicates()

    # # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId
    # actc_lanes_conflation_df = unique_actc_match_gdf.loc[unique_actc_match_gdf['BASE_LN'] > 0].groupby(
    #     ['shstReferenceId'])['BASE_LN'].apply(list).to_frame().reset_index()

    # actc_lanes_conflation_df['base_lanes_min'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: min(set(x)))
    # actc_lanes_conflation_df['base_lanes_max'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: max(set(x)))

    # # TODO: decide if export or merge into the base network
    # actc_lanes_conflation_df.to_csv(os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'actcmodel_legacy_lanes.csv'), index=False)

    # # same for bike lane
    # actc_bike_conflation_df = unique_actc_match_gdf.groupby(['shstReferenceId'])[['NMT2010', 'NMT2020']].agg(lambda x: list(x)).reset_index()

    # actc_bike_conflation_df['nmt2010_min'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: min(set(x)))
    # actc_bike_conflation_df['nmt2010_max'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: max(set(x)))
    # actc_bike_conflation_df['nmt2020_min'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: min(set(x)))
    # actc_bike_conflation_df['nmt2020_max'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: max(set(x)))

    # # TODO: decide if export or merge into the base network
    # actc_bike_conflation_df.to_csv(os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'actcmodel_legacy_bike.csv'), index=False)

    # # add data source prefix to column names
    # unique_actc_match_gdf.rename(columns={'A': 'ACTC_A',
    #                                       'B': 'ACTC_B',
    #                                       'base_lanes_min': 'ACTC_base_lanes_min',
    #                                       'base_lanes_max': 'ACTC_base_lanes_max',
    #                                       'nmt2010_min': 'ACTC_nmt2010_min',
    #                                       'nmt2010_max': 'ACTC_nmt2010_max',
    #                                       'nmt2020_min': 'ACTC_nmt2020_min',
    #                                       'nmt2020_max': 'ACTC_nmt2020_max'},
    #                              inplace=True)

    WranglerLogger.info('Finished conflating ACTC data')

# TODO: def conflate_pums():

if __name__ == '__main__':
    # do one dataset at a time
    # We could split this up into multiple scripts but I think there's enough in common that it's helpful to see in one script
    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('third_party', choices=[TOMTOM,TM2_NON_MARIN,TM2_MARIN,SFCTA,CCTA,ACTC,PEMS], help='Third party data to conflate')
    args = parser.parse_args()

    if not os.path.exists(os.path.join(THIRD_PARTY_OUTPUT_DIR, args.third_party)):
        os.makedirs(os.path.join(THIRD_PARTY_OUTPUT_DIR, args.third_party))

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        THIRD_PARTY_OUTPUT_DIR, args.third_party,
        "step4_conflate_third_party_{}_{}.info.log".format(
            args.third_party, datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    # do this first to error fast
    # set these based on what create_docker_container() will do
    if OUTPUT_DATA_DIR.startswith('E:'):
        output_mount_target = '/usr/e_volume'
    elif OUTPUT_DATA_DIR.startswith('C:/Users/{}'.format(os.environ['USERNAME'])):
        output_mount_target = '/usr/home'
    else:
        WranglerLogger.error('Only USERPROFILE dir and E: are currently supported for OUTPUT_DATA_DIR')
        sys.exit(1)

    WranglerLogger.info(args)

    if args.third_party == TOMTOM:
        conflate_TOMTOM()
    elif args.third_party == TM2_NON_MARIN:
        conflate_TM2_NON_MARIN()
    elif args.third_party == TM2_MARIN:
        conflate_TM2_MARIN()
    elif args.third_party == SFCTA:
        conflcate_SFCTA()
    elif args.third_party == CCTA:
        conflate_CCTA()
    elif args.third_party == ACTC:
        conflate_ACTC()

    WranglerLogger.info('complete')
