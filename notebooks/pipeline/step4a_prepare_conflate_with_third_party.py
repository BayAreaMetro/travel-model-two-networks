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
import datetime, os
import methods
import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import Point, shape, LineString
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging

#####################################
# EPSG requirement
# TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

#####################################
# inputs and outputs

INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# third-party network data inputs
THIRD_PARTY_RAW_DATA_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step4a_third_party_data', 'raw')
INPUT_TomTom_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'TomTom networkFGDB', 'network2019', 'Network_region.gdb')
INPUT_TM2_nonMarin_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'TM2_nonMarin', 'mtc_final_network_base.shp')
INPUT_TM2_Marin_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'TM2_Marin', 'mtc_final_network_base.shp')
INPUT_SFCTA_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'sfcta', 'SanFrancisco_links.shp')
INPUT_CCTA_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'ccta_model', 'ccta_2015_network', 'ccta_2015_network.shp')
INPUT_ACTC_FILE = os.path.join(THIRD_PARTY_RAW_DATA_DIR, 'actc_model', 'AlamedaCo_MASTER_20190410_no_cc.shp')

# sub-region boundary polygons to split third-party data
BOUNDARY_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step0_boundaries', 'modified')

# third-party network data outputs
THIRD_PARTY_MODIFIED_DIR = os.path.join(OUTPUT_DATA_DIR, 'external', 'step4a_third_party_data', 'modified')
OUTPUT_TomTom_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'TomTom')
OUTPUT_TM2_nonMarin_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'TM2_nonMarin')
OUTPUT_TM2_Marin_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'TM2_Marin')
OUTPUT_SFCTA_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'sfcta')
OUTPUT_CCTA_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'ccta')
OUTPUT_ACTC_DIR = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'actc')
OUTPUT_ALL_ATTRS_GPKG = os.path.join(THIRD_PARTY_MODIFIED_DIR, 'modified_all_attrs.gpkg')


if __name__ == '__main__':
    # create output directories if not exist
    for i in [OUTPUT_TomTom_DIR, OUTPUT_TM2_nonMarin_DIR, OUTPUT_TM2_Marin_DIR,
              OUTPUT_SFCTA_DIR, OUTPUT_CCTA_DIR, OUTPUT_ACTC_DIR]:
        if not os.path.exists(i):
            WranglerLogger.info('create output folder: {}'.format(i))
            os.makedirs(i)

    # setup logging
    LOG_FILENAME = os.path.join(
        THIRD_PARTY_MODIFIED_DIR,
        "step4a_prepare_third_party_data_for_conflation_{}.info.log".format(
            datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    ####################################
    # Prepare tomtom for conflation
    WranglerLogger.info('preparing TomTom data')

    # print out all the layers from the .gdb file
    layers = fiona.listlayers(INPUT_TomTom_FILE)
    WranglerLogger.info('TomTom gdb has the following layers: {}'.format(layers))
    # load tomtom data, use the street link layer
    WranglerLogger.info('loading TomTom raw data')
    tomtom_raw_gdf = gpd.read_file(INPUT_TomTom_FILE, layer='mn_nw')

    # convert to ESPG lat-lon
    tomtom_raw_gdf = tomtom_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tomtom_raw_gdf.crs))

    WranglerLogger.info('total {} tomtom links'.format(tomtom_raw_gdf.shape[0]))
    WranglerLogger.debug('TomTom data info: \n{}'.format(tomtom_raw_gdf.info()))

    # There is no existing unique tomtom handle for Bay Area, thus we need to create unique handle
    WranglerLogger.info('unique ID + F + T: {}'.format(
        len(tomtom_raw_gdf.groupby(["ID", "F_JNCTID", "T_JNCTID"]).count())))

    # generating unique handle for tomtom
    tomtom_raw_gdf["tomtom_link_id"] = range(1, len(tomtom_raw_gdf) + 1)

    # Partition tomtom by sub-region boundaries for shst match
    WranglerLogger.info('exporting partitioned TomTom data to {}'.format(OUTPUT_TomTom_DIR))
    for i in range(14):
        boundary_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_{}.geojson'.format(str(i + 1))))
        sub_tomtom_gdf = tomtom_raw_gdf[tomtom_raw_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()

        sub_tomtom_gdf[["tomtom_link_id", "geometry"]].to_file(
            os.path.join(OUTPUT_TomTom_DIR, 'tomtom_{}.in.geojson'.format(str(i + 1))),
            driver="GeoJSON")

    # export modified raw data for merging the conflation results back
    WranglerLogger.info('exporting TomTom all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    tomtom_raw_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='tomtom_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing TomTom data')
    WranglerLogger.debug('TomTom has the following attributes: {}'.format(list(tomtom_raw_gdf)))

    #####################################
    # Prepare TM2 non-Marin for conflation

    WranglerLogger.info('loading TM2_nonMarin data from {}'.format(INPUT_TM2_nonMarin_FILE))
    tm2_link_gdf = gpd.read_file(INPUT_TM2_nonMarin_FILE)
    WranglerLogger.debug('TM2_Marin data info: \n{}'.format(tm2_link_gdf.info()))

    # define initial ESPG
    tm2_link_gdf.crs = "esri:102646"

    # convert to ESPG lat-lon
    tm2_link_gdf = tm2_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_nonMarin link data CNTYPE stats: \n{}'.format(
        tm2_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_link_roadway_gdf = tm2_link_gdf.loc[
        tm2_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]
    WranglerLogger.info('\n out of {} links in TM2_non-Marin network, {} are roadway links'.format(
        tm2_link_gdf.shape[0],
        tm2_link_roadway_gdf.shape[0]))

    # double check with AB node pairs
    WranglerLogger.info('# of unique AB node pairs: {}'.format(
        tm2_link_roadway_gdf.groupby(["A", "B"]).count().shape[0]))

    # Partition TM2 Non Marin for shst Match
    WranglerLogger.info('exporting TM2_nonMarin partitioned data to {}'.format(OUTPUT_TM2_nonMarin_DIR))
    for i in range(14):
        boundary_gdf = gpd.read_file(
            os.path.join(BOUNDARY_DIR, 'boundary_{}.geojson'.format(str(i + 1))))
        sub_gdf = tm2_link_roadway_gdf[
            tm2_link_roadway_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["A", "B", "geometry"]].to_file(
            os.path.join(OUTPUT_TM2_nonMarin_DIR, 'tm2nonMarin_{}.in.geojson'.format(str(i + 1))),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    # before exporting, fix NAME that would cause encoding error
    tm2_link_roadway_gdf.loc[
        tm2_link_roadway_gdf.NAME.notnull() & (
            tm2_link_roadway_gdf.NAME.str.contains('Vista Monta')) & (
                tm2_link_roadway_gdf.NAME != 'Vista Montara C'), 'NAME'] = 'Vista Montana'

    WranglerLogger.info('exporting TM2_nonMarin all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    tm2_link_roadway_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='tm2nonMarin_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing TM2_nonMarin data')
    WranglerLogger.debug('TM2_nonMarin data has the following attributes: {}'.format(list(tm2_link_roadway_gdf)))

    #####################################
    # Prepare TM2 Marin for conflation

    WranglerLogger.info('loading TM2_Marin data from {}'.format(INPUT_TM2_Marin_FILE))
    tm2_marin_link_gdf = gpd.read_file(INPUT_TM2_Marin_FILE)
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
    WranglerLogger.info('\n out of {} links in TM2_Marin network, {} are roadway links'.format(
        tm2_marin_link_gdf.shape[0],
        tm2_marin_link_roadway_gdf.shape[0]))

    # double check with AB node pairs
    WranglerLogger.info('# of unique AB node pairs: {}'.format(
        tm2_marin_link_roadway_gdf.groupby(["A", "B"]).count().shape[0]))

    # Partition TM2 Marin for shst Match
    WranglerLogger.info('exporting TM2_Marin partitioned data to {}'.format(OUTPUT_TM2_Marin_DIR))
    for i in range(14):
        boundary_gdf = gpd.read_file(
            os.path.join(BOUNDARY_DIR, 'boundary_{}.geojson'.format(str(i + 1))))
        sub_gdf = tm2_marin_link_roadway_gdf[
            tm2_marin_link_roadway_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["A", "B", "geometry"]].to_file(
            os.path.join(OUTPUT_TM2_Marin_DIR, 'tm2Marin_{}.in.geojson'.format(str(i + 1))),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    # before exporting, fix NAME that would cause encoding error
    tm2_marin_link_roadway_gdf.loc[
        tm2_marin_link_roadway_gdf.NAME.notnull() & (
            tm2_marin_link_roadway_gdf.NAME.str.contains('Vista Monta')) & (
                tm2_marin_link_roadway_gdf.NAME != 'Vista Montara C'), 'NAME'] = 'Vista Montana'

    WranglerLogger.info('exporting TM2_nonMarin all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    tm2_marin_link_roadway_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='tm2Marin_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing TM2_Marin data')
    WranglerLogger.debug('TM2_Marin data has the following attributes: {}'.format(list(tm2_marin_link_roadway_gdf)))

    #####################################
    # Prepare SFCTA for conflation

    WranglerLogger.info('loading SFCTA data from {}'.format(INPUT_SFCTA_FILE))
    sfcta_stick_gdf = gpd.read_file(INPUT_SFCTA_FILE)
    WranglerLogger.debug('SFCTA data info: \n{}'.format(sfcta_stick_gdf.info()))

    # set initial ESPG
    sfcta_stick_gdf.crs = CRS("EPSG:2227")
    # convert to ESPG lat-lon
    sfcta_stick_gdf = sfcta_stick_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(sfcta_stick_gdf.crs))

    # only conflation SF part of the network
    boundary_4_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_4.geojson'))
    sfcta_SF_gdf = sfcta_stick_gdf[
        sfcta_stick_gdf.intersects(boundary_4_gdf.geometry.unary_union)]
    WranglerLogger.info('{} SF links, with following fields: \n{}'.format(
        sfcta_SF_gdf.shape[0], list(sfcta_SF_gdf)))

    # remove "special facility" (FT 6)
    sfcta_SF_roadway_gdf = sfcta_SF_gdf[~(sfcta_SF_gdf.FT == 6)]

    # Write out SFCTA stick network for conflation
    sfcta_SF_roadway_gdf[['A', 'B', "geometry"]].to_file(
        os.path.join(OUTPUT_SFCTA_DIR, 'sfcta_in.geojson'), driver="GeoJSON")

    # export modified raw data for merging the conflation results back
    WranglerLogger.info('exporting SFCTA all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    sfcta_SF_roadway_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='sfcta_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing SFCTA data')
    WranglerLogger.debug('SFTCA data has the following attributes: {}'.format(list(sfcta_SF_roadway_gdf)))

    #####################################
    # Prepare CCTA for conflation

    WranglerLogger.info('loading CCTA data from {}'.format(INPUT_CCTA_FILE))
    ccta_raw_gdf = gpd.read_file(INPUT_CCTA_FILE)
    WranglerLogger.debug('CCTA data info: \n{}'.format(ccta_raw_gdf.info()))

    WranglerLogger.info('CCTA data projection: ' + str(ccta_raw_gdf.crs))

    # filter out connectors
    ccta_gdf = ccta_raw_gdf.loc[(ccta_raw_gdf.AB_FT != 6) & (ccta_raw_gdf.BA_FT != 6)]
    WranglerLogger.info('CCTA data has {} rows, {} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # this network is from transcad, for one way streets, dir=1;
    # for two-way streets, there is only one links with dir=0, need to create other direction
    # from shapely.geometry import LineString
    two_way_links_gdf = ccta_gdf.loc[ccta_gdf.DIR == 0].copy()
    two_way_links_gdf["geometry"] = two_way_links_gdf.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]),
        axis=1
    )
    two_way_links_gdf.rename(columns={'AB_LANES': 'BA_LANES', 'BA_LANES': 'AB_LANES'}, inplace=True)
    two_way_links_gdf['ID'] = two_way_links_gdf['ID'] + 9000000

    ccta_gdf = pd.concat([ccta_gdf, two_way_links_gdf], sort=False, ignore_index=True)
    # double check
    WranglerLogger.info('after creating other direction for two-way roads, ccta data has {} links, {} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # Partition CCTA data for shst Match
    WranglerLogger.info('exporting CCTA partitioned data to {}'.format(OUTPUT_CCTA_DIR))
    for i in range(14):
        boundary_gdf = gpd.read_file(
            os.path.join(BOUNDARY_DIR, 'boundary_{}.geojson'.format(str(i + 1))))
        sub_gdf = ccta_gdf[
            ccta_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["ID", "geometry"]].to_file(
            os.path.join(OUTPUT_CCTA_DIR, 'ccta_{}.in.geojson'.format(str(i + 1))),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    WranglerLogger.info('exporting CCTA all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    ccta_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='ccta_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing CCTA data')
    WranglerLogger.debug('CCTA data has the following attributes: {}'.format(list(ccta_gdf)))

    #####################################
    # Prepare ACTC for conflation
    # connectors were already filtered out

    WranglerLogger.info('loading ACTC data from {}'.format(INPUT_ACTC_FILE))
    actc_raw_gdf = gpd.read_file(INPUT_ACTC_FILE)
    WranglerLogger.info('ACTC data has {} links, {} unique A-B combination'.format(
        actc_raw_gdf.shape[0], len(actc_raw_gdf.groupby(['A', 'B']).count())
    ))
    WranglerLogger.debug('ACTC data info: \n{}'.format(actc_raw_gdf.info()))

    # define initial ESPG
    actc_raw_gdf.crs = CRS("EPSG:26910")

    # convert to ESPG lat-lon
    actc_raw_gdf = actc_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(actc_raw_gdf.crs))

    # Partition ACTC data for shst Match
    WranglerLogger.info('exporting ACTC partitioned data to {}'.format(OUTPUT_CCTA_DIR))
    for i in range(14):
        boundary_gdf = gpd.read_file(
            os.path.join(BOUNDARY_DIR, 'boundary_{}.geojson'.format(str(i + 1))))
        sub_gdf = actc_raw_gdf[
            actc_raw_gdf.intersects(boundary_gdf.geometry.unary_union)].copy()
        sub_gdf[["A", "B", "geometry"]].to_file(
            os.path.join(OUTPUT_ACTC_DIR, 'actc_{}.in.geojson'.format(str(i + 1))),
            driver="GeoJSON")

    # export raw network data for merging back the conflation result
    WranglerLogger.info('exporting ACTC all attributes as a layer to {}'.format(OUTPUT_ALL_ATTRS_GPKG))
    actc_raw_gdf.to_file(OUTPUT_ALL_ATTRS_GPKG, layer='actc_allAttrs', driver='GPKG')

    WranglerLogger.info('finished preparing ACTC data')
    WranglerLogger.debug('ACTC data has the following attributes: {}'.format(list(actc_raw_gdf)))
