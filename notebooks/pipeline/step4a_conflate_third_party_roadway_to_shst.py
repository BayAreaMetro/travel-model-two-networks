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
    WranglerLogger.debug('TomTom raw data dtypes:\n{}'.format(tomtom_raw_gdf.dtypes))
    WranglerLogger.debug('TomTom crs:\n{}'.format(tomtom_raw_gdf.crs))

    # convert to ESPG lat-lon
    tomtom_raw_gdf = tomtom_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tomtom_raw_gdf.crs))

    WranglerLogger.info('TomTom data has {:,} links, {:,} unique ID+F_JNCTID+T_JNCTID combination'.format(
        tomtom_raw_gdf.shape[0], len(tomtom_raw_gdf.groupby(["ID", "F_JNCTID", "T_JNCTID"]).count())))

    # There is no existing unique tomtom handle for Bay Area, thus we need to create unique handle
    WranglerLogger.info('creating unique handle tomtom_link_id')
    tomtom_raw_gdf['tomtom_link_id'] = range(1, len(tomtom_raw_gdf) + 1)

    (tomtom_matched_gdf, tomtom_unmatched_gdf) = methods.conflate(
        TOMTOM, tomtom_raw_gdf, ['tomtom_link_id'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.info('finished conflating TomTom data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total TomTom Links.'.format(
        tomtom_matched_gdf.tomtom_link_id.nunique(), tomtom_raw_gdf.shape[0]))

def conflate_TM2_NON_MARIN():
    """
    Conflate TM2 (NonMarin) data with sharedstreets
    TODO: What files are written?
    """
    # Prepare TM2 non-Marin for conflation
    WranglerLogger.info('loading TM2_nonMarin data from {}'.format(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN]))
    tm2_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN])
    WranglerLogger.debug('TM2_Marin raw data dtypes: \n{}'.format(tm2_link_gdf.dtypes))

    # define initial ESPG
    tm2_link_gdf.crs = "esri:102646"

    # convert to ESPG lat-lon
    tm2_link_gdf = tm2_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_nonMarin link data CNTYPE stats: \n{}'.format(
        tm2_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_link_roadway_gdf = tm2_link_gdf.loc[tm2_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]

    WranglerLogger.info('TM2_nonMarin has {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_link_roadway_gdf.shape[0], len(tm2_link_roadway_gdf.groupby(["A", "B"]).count())))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_NON_MARIN, tm2_link_roadway_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.info('finished conflating TM2_nonMarin data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total TM2_nonMarin Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_link_roadway_gdf.shape[0]))
        

def conflate_TM2_MARIN():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare TM2 Marin for conflation
    WranglerLogger.info('loading TM2_Marin data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[TM2_MARIN])))
    tm2_marin_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_MARIN])
    WranglerLogger.debug('TM2_Marin raw data dtypes: \n{}'.format(tm2_marin_link_gdf.dtypes))

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
    WranglerLogger.info('TM2_Marin has {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_marin_link_roadway_gdf.shape[0], len(tm2_marin_link_roadway_gdf.groupby(["A", "B"]).count())))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_MARIN, tm2_marin_link_roadway_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)    

    WranglerLogger.info('finished conflating TM2_Marin data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total TM2_Marin Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_marin_link_roadway_gdf.shape[0]))

def conflcate_SFCTA():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare SFCTA for conflation
    WranglerLogger.info('loading SFCTA data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[SFCTA])))
    sfcta_stick_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[SFCTA])
    WranglerLogger.debug('SFCTA raw data dtypes: \n{}'.format(sfcta_stick_gdf.dtypes))

    # set initial ESPG
    sfcta_stick_gdf.crs = CRS("EPSG:2227")
    # convert to ESPG lat-lon
    sfcta_stick_gdf = sfcta_stick_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(sfcta_stick_gdf.crs))

    # only conflate SF part of the network
    boundary_4_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_04.geojson'))
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

    WranglerLogger.info('finished conflating SFCTA data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total SFCTA Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), sfcta_SF_roadway_gdf.shape[0]))

def conflate_CCTA():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare CCTA for conflation
    WranglerLogger.info('loading CCTA data from {}'.format(THIRD_PARTY_INPUT_FILES[CCTA]))
    ccta_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[CCTA])
    WranglerLogger.debug('CCTA raw data dtypes: \n{}'.format(ccta_raw_gdf.dtypes))
    WranglerLogger.info('CCTA crs:\n{}'.format(ccta_raw_gdf.crs))

    # filter out connectors
    ccta_gdf = ccta_raw_gdf.loc[(ccta_raw_gdf.AB_FT != 6) & (ccta_raw_gdf.BA_FT != 6)]
    WranglerLogger.info('CCTA data has {:,} rows, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # this network is from transcad, for one way streets, dir=1;
    # for two-way streets, there is only one links with dir=0, need to create other direction
    # from shapely.geometry import LineString
    WranglerLogger.debug('creating reversed links')
    two_way_links_gdf = ccta_gdf.loc[ccta_gdf.DIR == 0].copy()
    two_way_links_gdf["geometry"] = two_way_links_gdf.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]),
        axis=1)
    # rename all link attributes for 'AB_' into 'BA_'
    rename_columns = {}
    for colname in [x for x in ccta_gdf.columns if ('AB_' in x)]:
        rename_columns[colname] = colname.replace('AB', 'BA')
    WranglerLogger.debug('renaming columns for reversed links: {}'.format(rename_columns))
    two_way_links_gdf.rename(columns=rename_columns, inplace=True)
    # TODO: why "9000000"? I assume the goal is to exceed the existing largest ID number, so need to be more generic
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

    WranglerLogger.info('finished conflating CCTA data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total CCTA Links.'.format(
        matched_gdf['ID'].nunique(), ccta_gdf.shape[0]))

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

    WranglerLogger.info('finished conflating ACTC data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total ACTC Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), actc_raw_gdf.shape[0]))

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
