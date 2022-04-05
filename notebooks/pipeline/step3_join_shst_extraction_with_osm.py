USAGE = """

Add OSM attributes to extracted SharedStreets network and convert to Network Standard data formats.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
# Input: 
    OSM extraction, [INPUT_DATA_DIR]/external/external/step2_osmnx_extraction/link.geojson
    SharedStreet extraction, [INPUT_DATA_DIR]/external/step1_shst_extraction/mtc_[1-14].out.geojson
# Output: roadway network in standard format, with SharedStreet geometries and OSM link/node attributes,
    [OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_shape.geojson,
    [OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_link.json,
    [OUTPUT_DATA_DIR]/interim/step3_join_shst_extraction_with_osm/step3_node.geojson
"""

import pandas as pd
import geopandas as gpd
import json

from methods import extract_osm_link_from_shst_shape, osm_link_with_shst_info, add_two_way_osm, fill_na, \
    consolidate_osm_way_to_shst_link, highway_attribute_list_to_value, create_node_gdf, read_shst_extract,\
    link_df_to_geojson, point_df_to_geojson, os

from network_wrangler import WranglerLogger, setupLogging
from datetime import datetime

#####################################
# inputs and outputs

INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# OSM extraction and SharedStreet extraction
OSM_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step2_osmnx_extraction')
OSM_LINK_FILE = os.path.join(OSM_EXTRACT_DIR, 'link.geojson')
## OSM_NODE_FILE = os.path.join(OSM_EXTRACT_DIR, 'node.geojson')
SHST_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step1_shst_extraction')
# lookups for roadway type and network type
HIGHWAY_TO_ROADWAY_CROSSWALK_FILE = os.path.join(INPUT_DATA_DIR, 'lookups', 'highway_to_roadway.csv')
NETWORK_TYPE_LOOKUP_FILE = os.path.join(INPUT_DATA_DIR, 'lookups', 'network_type_indicator.csv')

SHST_WITH_OSM_DIR = os.path.join(OUTPUT_DATA_DIR, 'interim', 'step3_join_shst_extraction_with_osm')


if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(SHST_WITH_OSM_DIR):
        WranglerLogger.info('create output folder')
        os.makedirs(SHST_WITH_OSM_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        SHST_WITH_OSM_DIR,
        "step3_join_shst_extraction_with_osm_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    #####################################
    # load OSM data
    WranglerLogger.info('reading osmnx data')
    osmnx_link_gdf = gpd.read_file(OSM_LINK_FILE)
    # looks like osm node data is not used ?
    # osmnx_node_gdf = gpd.read_file(OSM_NODE_FILE)
    WranglerLogger.info('finished reading osmnx data')
    WranglerLogger.info('osmnx link data has the following attributes: {}'.format(list(osmnx_link_gdf)))
    # WranglerLogger.info('osmnx node data has the following attributes: {}'.format(list(osmnx_node_gdf)))

    WranglerLogger.info(osmnx_link_gdf.head(3))

    #####################################
    # load SHST extraction output, and process it to standard network

    WranglerLogger.info('reading SharedStreet data')
    shst_link_gdf = read_shst_extract(SHST_EXTRACT_DIR, "*.out.geojson")
    WranglerLogger.info('finished reading SharedStreet data')
    WranglerLogger.info('SharedStreet data has the following fields: {}'.format(list(shst_link_gdf)))
    WranglerLogger.info(shst_link_gdf.head(3))

    # shst geometry file has duplicates, due to the buffer area along polygon boundries
    WranglerLogger.info('...before removing duplicates, shst extraction has geometry: {}'.format(shst_link_gdf.shape[0]))
    shst_link_non_dup_gdf = shst_link_gdf.drop_duplicates(
        subset=['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId'])
    WranglerLogger.info('...after removing duplicates,\
    shst extraction has geometry: {}'.format(shst_link_non_dup_gdf.shape[0]))

    # obtaining OSM data for SHST links
    WranglerLogger.info('Extracting corresponding osm ways for every shst geometry')

    WranglerLogger.info('...expand each shst extract record into osm segments')
    osm_from_shst_link_list = []
    temp = shst_link_non_dup_gdf.apply(lambda x: extract_osm_link_from_shst_shape(x, osm_from_shst_link_list),
                                       axis=1)

    osm_from_shst_link_df = pd.concat(osm_from_shst_link_list)
    WranglerLogger.debug('osm_from_shst_link_df has the following fields: {}'.format(list(osm_from_shst_link_df)))
    WranglerLogger.debug(osm_from_shst_link_df.head())

    WranglerLogger.info('...add other shst info to these osm segments')
    osm_from_shst_link_gdf = osm_link_with_shst_info(osm_from_shst_link_df,
                                                     shst_link_non_dup_gdf)
    WranglerLogger.debug('osm_from_shst_link_gdf has the following fields: {}'.format(list(osm_from_shst_link_gdf)))
    WranglerLogger.debug(osm_from_shst_link_gdf.head())

    # # note, the sharedstreets extraction using default tile osm/planet 181224
    WranglerLogger.info('...add two-way links')
    osm_from_shst_link_gdf = add_two_way_osm(osm_from_shst_link_gdf, osmnx_link_gdf)
    WranglerLogger.debug('after adding two-way links, osm_from_shst_link_gdf has the following fields: {}'.format(
        list(osm_from_shst_link_gdf)))
    WranglerLogger.debug(osm_from_shst_link_gdf.head())

    # fill NAs for shst links that do not have complete osm info
    WranglerLogger.info('...fill NAs for shst links missing complete osm info')
    osm_from_shst_link_non_na_gdf = fill_na(osm_from_shst_link_gdf)

    # aggregate osm segments back to shst geometry based links
    WranglerLogger.info('...aggregate osm segments back to shst geometry based links')
    link_gdf = consolidate_osm_way_to_shst_link(osm_from_shst_link_non_na_gdf)
    WranglerLogger.info('......after joining back to shst geometry, network has {} links,\
    which are based on {} geometries'.format(link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))
    WranglerLogger.debug('after aggregating osm segments or ways back to shst geometries,\
    the resulting network links has the following fields: {}'.format(
        list(link_gdf)))
    WranglerLogger.debug(link_gdf.head())

    # convert osm "highway" values into standard roadway property
    WranglerLogger.info('Converting OSM highway variable into standard roadway variable')

    highway_to_roadway_df = pd.read_csv(HIGHWAY_TO_ROADWAY_CROSSWALK_FILE)
    highway_to_roadway_df.fillna('', inplace=True)

    highway_to_roadway_dict = pd.Series(highway_to_roadway_df.roadway.values,
                                        index=highway_to_roadway_df.highway).to_dict()
    roadway_hierarchy_dict = pd.Series(highway_to_roadway_df.hierarchy.values,
                                       index=highway_to_roadway_df.roadway).to_dict()

    link_gdf["roadway"] = link_gdf.apply(
        lambda x: highway_attribute_list_to_value(
            x, highway_to_roadway_dict, roadway_hierarchy_dict),
        axis=1)

    WranglerLogger.debug('link counts by roadway types: {}'.format(link_gdf.roadway.value_counts()))
    WranglerLogger.debug('roadway data from roadClass while OSM highway value is missing: {}'.format(
        link_gdf[link_gdf.highway == ''].roadway.value_counts()
    ))

    # there are links with different shstgeomid, but same shstrefid, to/from nodes
    # drop one of the links that have two shstGeomId
    WranglerLogger.info('Dropping link duplicates based on ShstRefenceID and from/to nodes')
    link_gdf.drop_duplicates(subset=["shstReferenceId"], inplace=True)

    # add network type variables
    WranglerLogger.info('Adding network type variables')
    network_type_df = pd.read_csv(NETWORK_TYPE_LOOKUP_FILE)

    link_gdf = pd.merge(link_gdf,
                        network_type_df,
                        how='left',
                        on='roadway')

    WranglerLogger.info('Finished converting SHST extraction into standard network links,\
    network has {} links, which are based on {} geometrics'.format(
        link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))

    # create shapes based on the network links
    WranglerLogger.info('Creating shapes from links')
    shape_gdf = shst_link_non_dup_gdf[shst_link_non_dup_gdf.id.isin(link_gdf.shstGeometryId.tolist())]
    WranglerLogger.info('In the end, there are {} geometries'.format(str(shape_gdf.shape[0])))

    # create node gdf from links and attach network type variable
    WranglerLogger.info('Creating nodes from links')
    node_gdf = create_node_gdf(link_gdf)

    WranglerLogger.info('Adding network type variable for node')
    A_B_df = pd.concat(
        [link_gdf[["u", "drive_access", "walk_access", "bike_access"]].rename(columns={"u": "osm_node_id"}),
         link_gdf[["v", "drive_access", "walk_access", "bike_access"]].rename(columns={"v": "osm_node_id"})],
        sort=False,
        ignore_index=True)
    A_B_df.drop_duplicates(inplace=True)
    A_B_df = A_B_df.groupby("osm_node_id").max().reset_index()
    node_gdf = pd.merge(node_gdf,
                        A_B_df,
                        how="left",
                        on="osm_node_id")

    WranglerLogger.info('{} network nodes created from {} unique osm from/to nodes'.format(
        node_gdf.osm_node_id.nunique(), len(set(link_gdf.u.tolist() + link_gdf.v.tolist()))
    ))
    WranglerLogger.debug('mis-match between network nodes and osm from/to nodes:')
    WranglerLogger.debug(link_gdf[~link_gdf.v.isin(node_gdf.osm_node_id.tolist())])
    WranglerLogger.debug(link_gdf[~link_gdf.u.isin(node_gdf.osm_node_id.tolist())])

    #####################################
    # export link, node, shape

    WranglerLogger.info('Final network links have the following fields: {}'.format(list(link_gdf)))
    WranglerLogger.info('Final network nodes have the following fields: {}'.format(list(node_gdf)))
    WranglerLogger.info('Final network shapes have the following fields: {}'.format(list(shape_gdf)))

    WranglerLogger.info('write out link shape geojson')
    shape_prop = ['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId']
    shape_geojson = link_df_to_geojson(shape_gdf, shape_prop)

    with open(os.path.join(SHST_WITH_OSM_DIR, 'step3_shape.geojson'), "w") as f:
        json.dump(shape_geojson, f)

    WranglerLogger.info('write out link json')
    link_prop = link_gdf.drop(["geometry", "nodeIds", "forward", "roadClass", "oneway"],
                              axis=1).columns.tolist()
    out = link_gdf[link_prop].to_json(orient="records")

    with open(os.path.join(SHST_WITH_OSM_DIR, 'step3_link.json'), 'w') as f:
        f.write(out)

    WranglerLogger.info('write out node geojson')
    node_prop = node_gdf.drop("geometry", axis = 1).columns.tolist()
    node_geojson = point_df_to_geojson(node_gdf, node_prop)

    with open(os.path.join(SHST_WITH_OSM_DIR, 'step3_node.geojson'), "w") as f:
        json.dump(node_geojson, f)
