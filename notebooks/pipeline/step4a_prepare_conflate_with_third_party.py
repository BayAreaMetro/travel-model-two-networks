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
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

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
    PEMS            : 'todo'
}
THIRD_PARTY_OUTPUT_DIR  = os.path.join(OUTPUT_DATA_DIR, 'step4_third_party_data')
# conflation will be done in [THIRD_PARTY_OUTPUT_DIR]/[third_party]/conflation_shst
CONFLATION_SHST = 'conflation_shst'

def conflate(third_party: str, third_party_gdf: gpd.GeoDataFrame):
    """
    Generic conflation method.  Does the following:
    1) Creates conflation directory (if it doesn't exist)
    2) Writes the third_party_gdf for conflation
    3) Runs the conflation in a docker container
    4) Returns the resulting GeoDataFrame
    """
    # create conflation directory
    conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, third_party, CONFLATION_SHST)
    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    shst_input_file     = os.path.join(conflation_dir, '{}.in.geojson'.format(third_party))
    shst_matched_file   = os.path.join(conflation_dir, '{}.out.matched.geojson'.format(third_party))
    shst_unmatched_file = os.path.join(conflation_dir, '{}.out.unmatched.geojson'.format(third_party))

    # NOTE: skip shst conflation if it was already done and if the files are in place since this process is slow....
    if os.path.exists(shst_input_file) and os.path.exists(shst_matched_file):
        WranglerLogger.info('{} conflation files {} and {} exist -- skipping conflation'.format(
            third_party, shst_input_file, shst_matched_file))

    else:
        # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
        docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
        # create docker container to do the shst matchting
        (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

        # TODO: add partitioning if dataset is large

        # Partitioning by the boundaries is not necessary with NODE_OPTIONS==--max_old_space_size=8192
        WranglerLogger.info('Exporting {:,} rows of {} data to {}'.format(len(third_party_gdf), third_party, shst_input_file))
        third_party_gdf.to_file(shst_input_file, driver="GeoJSON")

        command = ("/bin/bash -c 'cd {}; shst match step4_third_party_data/{}/conflation_shst/{}.in.geojson "
                   "--out=step4_third_party_data/{}/conflation_shst/{}.out.geojson "
                   "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction'".format(
                       docker_output_path, third_party, third_party, third_party, third_party))
        WranglerLogger.info('Executing docker command: {}'.format(command))
        
        (exec_code,exec_output) = container.exec_run(command, stdout=True, stderr=True, stream=True)
        while True:
            try:
                line = next(exec_output)
                # note: this looks a little funny because it's a byte string
                WranglerLogger.debug(line.strip())
            except StopIteration:
                # done
                WranglerLogger.info('...Completed command. exec_code: {}'.format(exec_code))
                break

        WranglerLogger.info('stopping container {}'.format(container.name))
        container.stop()
        client.containers.prune()
    
    matched_gdf   = gpd.read_file(shst_matched_file)
    WranglerLogger.debug('Read {:,} rows from {}; dtypes:\n{}'.format(len(matched_gdf), shst_matched_file, matched_gdf.dtypes))
    
    if os.path.exists(shst_unmatched_file):
        unmatched_gdf = gpd.read_file(shst_unmatched_file)
        WranglerLogger.debug('Read {:,} rows from {}; dtypes:\n{}'.format(len(unmatched_gdf), shst_unmatched_file, unmatched_gdf.dtypes))
    else:
        unmatched_gdf = None
        WranglerLogger.debug('No unmached file {} found'.format(shst_unmatched_file))

    return (matched_gdf, unmatched_gdf)

def conflate_TOMTOM():
    """
    Conflate TomTom data with sharedstreets
    TODO: What files are written?
    """
    # Prepare tomtom for conflation
    WranglerLogger.info('preparing TomTom data from {}'.format(THIRD_PARTY_INPUT_FILES[TOMTOM]))

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
    WranglerLogger.debug('TomTom data info: \n{}'.format(tomtom_raw_gdf.info()))

    # There is no existing unique tomtom handle for Bay Area, thus we need to create unique handle
    WranglerLogger.info('ID + F_JNCTID + T_JNCTID: {}'.format(
        len(tomtom_raw_gdf.groupby(["ID", "F_JNCTID", "T_JNCTID"]).count())))

    # generating unique handle for tomtom
    tomtom_raw_gdf["tomtom_link_id"] = range(1, len(tomtom_raw_gdf) + 1)

    # create conflation directory
    conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, TOMTOM, CONFLATION_SHST)
    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
    # create docker container to do the shst matchting
    (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # Partition tomtom by sub-region boundaries for shst match
    WranglerLogger.info('exporting partitioned TomTom data to {}'.format(conflation_dir))
    for boundary_num in range(1, methods.NUM_SHST_BOUNDARIES+1):
        boundary_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_{:02d}.geojson'.format(boundary_num)))
        sub_tomtom_gdf = tomtom_raw_gdf[tomtom_raw_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()

        sub_tomtom_gdf[["tomtom_link_id", "geometry"]].to_file(
            os.path.join(conflation_dir, 'tomtom_{}.in.geojson'.format(boundary_num)),
            driver="GeoJSON")

    # export modified raw data for merging the conflation results back
    output_file = os.path.join(conflation_dir, "tomtom.feather")
    WranglerLogger.info('exporting TomTom with all attributes to {}'.format(output_file))
    geofeather.to_geofeather(tomtom_raw_gdf, output_file)
    
    WranglerLogger.debug('TomTom has the following dtypes:\n{}'.format(tomtom_raw_gdf.dtypes))
    WranglerLogger.info('finished preparing TomTom data')

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
    WranglerLogger.info('finished preparing TM2_nonMarin data')

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
    WranglerLogger.info('finished preparing TM2_Marin data')

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
    WranglerLogger.info('{:,} SF links, with following fields: \n{}'.format(
        sfcta_SF_gdf.shape[0], list(sfcta_SF_gdf)))

    # remove "special facility" (FT 6)
    sfcta_SF_roadway_gdf = sfcta_SF_gdf[~(sfcta_SF_gdf.FT == 6)]

    # create conflation directory
    conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, SFCTA, CONFLATION_SHST)
    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    docker_output_path = methods.docker_path(OUTPUT_DATA_DIR)
    # create docker container to do the shst matchting
    (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # Write out SFCTA stick network for conflation
    sfcta_SF_roadway_gdf[['A', 'B', "geometry"]].to_file(
        os.path.join(conflation_dir, 'sfcta_in.geojson'), driver="GeoJSON")

    # export modified raw data for merging the conflation results back
    output_file = os.path.join(conflation_dir, "sfcta.feather")
    WranglerLogger.info('exporting SFCTA with all attributes to {}'.format(output_file))
    geofeather.to_geofeather(sfcta_SF_roadway_gdf, output_file)

    WranglerLogger.debug('SFTCA data has the following dtypes: {}'.format(sfcta_SF_roadway_gdf.dtypes))
    WranglerLogger.info('finished preparing SFCTA data')

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

    # conflate
    ID_geometry_cols = ['ID','geometry']
    (matched_gdf, unmatched_gdf) = conflate(CCTA, ccta_gdf[ID_geometry_cols])

    #TODO: whatever we do next

    WranglerLogger.debug('CCTA data has the following dtypes: {}'.format(ccta_gdf.dtypes))
    WranglerLogger.info('finished preparing CCTA data')

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

    # define initial ESPG
    actc_raw_gdf.crs = CRS("EPSG:26910")

    # convert to ESPG lat-lon
    actc_raw_gdf = actc_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(actc_raw_gdf.crs))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 2.5-3 hours
    (actc_matched_gdf, actc_unmatched_gdf) = conflate(ACTC, actc_raw_gdf[["A","B","geometry"]])
    
    # rename columns
    actc_matched_gdf.rename(columns={
        'shstFromIntersectionId': 'fromIntersectionId',
        'shstToIntersectionId'  : 'toIntersectionId',
        'pp_a'                  : 'A',
        'pp_b'                  : 'B'},
        inplace=True)

    actc_matched_gdf = pd.merge(
        left     = actc_matched_gdf,
        right    = actc_raw_gdf.drop(columns=['geometry']),  # we already have this
        how      = 'left',
        on       = ['A','B'])
    WranglerLogger.debug('After join, actc_matched_gdf.dtypes:\n{}'.format(actc_matched_gdf.dtypes))

    # output for debugging
    matched_geofeather = os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'matched.geofeather')
    geofeather.to_geofeather(actc_matched_gdf, matched_geofeather)
    WranglerLogger.info('Wrote {:,} lines to {}'.format(len(actc_matched_gdf), matched_geofeather))

    actc_unmatched_gdf.rename(columns={'a':'A', 'b':'B'}, inplace=True)
    actc_unmatched_gdf = pd.merge(
        left     = actc_unmatched_gdf,
        right    = actc_raw_gdf.drop(columns=['geometry']),  # we already have this
        how      = 'left',
        on       = ['A','B'])
    WranglerLogger.debug('After join, actc_unmatched_gdf.dtypes:\n{}'.format(actc_matched_gdf.dtypes))

    # output for debugging
    unmatched_geofeather = os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'unmatched.geofeather')
    geofeather.to_geofeather(actc_unmatched_gdf, unmatched_geofeather)
    WranglerLogger.info('Wrote {:,} lines to {}'.format(len(actc_unmatched_gdf), unmatched_geofeather))

    # TODO: reconcile different methodologies for dropping duplicates
    unique_actc_match_gdf = actc_matched_gdf.drop_duplicates()

    # in conflation df, aggregate based on shstReferenceId, get all number of lanes for each shstReferenceId
    actc_lanes_conflation_df = unique_actc_match_gdf.loc[unique_actc_match_gdf['BASE_LN'] > 0].groupby(
        ['shstReferenceId'])['BASE_LN'].apply(list).to_frame().reset_index()

    actc_lanes_conflation_df['base_lanes_min'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: min(set(x)))
    actc_lanes_conflation_df['base_lanes_max'] = actc_lanes_conflation_df['BASE_LN'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_lanes_conflation_df.to_csv(os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'actcmodel_legacy_lanes.csv'), index=False)

    # same for bike lane
    actc_bike_conflation_df = unique_actc_match_gdf.groupby(['shstReferenceId'])[['NMT2010', 'NMT2020']].agg(lambda x: list(x)).reset_index()

    actc_bike_conflation_df['nmt2010_min'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2010_max'] = actc_bike_conflation_df['NMT2010'].apply(lambda x: max(set(x)))
    actc_bike_conflation_df['nmt2020_min'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: min(set(x)))
    actc_bike_conflation_df['nmt2020_max'] = actc_bike_conflation_df['NMT2020'].apply(lambda x: max(set(x)))

    # TODO: decide if export or merge into the base network
    actc_bike_conflation_df.to_csv(os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, 'actcmodel_legacy_bike.csv'), index=False)

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

    WranglerLogger.info('Finished conflating ACTC data')

if __name__ == '__main__':
    if not os.path.exists(THIRD_PARTY_OUTPUT_DIR):
        os.makedirs(THIRD_PARTY_OUTPUT_DIR)

    # do one dataset at a time
    # We could split this up into multiple scripts but I think there's enough in common that it's helpful to see in one script
    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('third_party', choices=[TOMTOM,TM2_NON_MARIN,TM2_MARIN,SFCTA,CCTA,ACTC,PEMS], help='Third party data to conflate')
    args = parser.parse_args()

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        THIRD_PARTY_OUTPUT_DIR, args.third_party,
        "step4_conflate_third_party_data_{}_{}.info.log".format(
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
