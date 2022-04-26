USAGE = """

Add OSM attributes to extracted SharedStreets network and convert to Network Standard data formats with links and nodes.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
# Input: 
    SharedStreet extract, [INPUT_DATA_DIR]/external/step1_shst_extracts/mtc_[1-14].out.feather[.crs]
    OSMnx extract,        [INPUT_DATA_DIR]/external/external/step2_osmnx_extracts/link.feather[.crs]
# Output: roadway network in geofeather format, with SharedStreet geometries and OSMX link/node attributes,
    [OUTPUT_DATA_DIR]/interim/step3_join_shst_with_osm/step3_link.feather[.crs]
    [OUTPUT_DATA_DIR]/interim/step3_join_shst_with_osm/step3_node.feather[.crs]
"""

import pandas as pd
import geopandas as gpd
import geofeather  # this is fast
import datetime, json, os, sys

import methods
from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# OSM extraction and SharedStreet extraction
SHST_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step1_shst_extracts')
OSM_EXTRACT_DIR  = os.path.join(INPUT_DATA_DIR, 'external', 'step2_osmnx_extracts')
OSM_LINK_FILE    = os.path.join(OSM_EXTRACT_DIR, 'link.feather')
# lookups for roadway type and network type
HIGHWAY_TO_ROADWAY_CROSSWALK_FILE = os.path.join(INPUT_DATA_DIR, 'lookups', 'highway_to_roadway.csv')
NETWORK_TYPE_LOOKUP_FILE = os.path.join(INPUT_DATA_DIR, 'lookups', 'network_type_indicator.csv')

# This script will write to this directory
SHST_WITH_OSM_DIR = os.path.join(OUTPUT_DATA_DIR, 'interim', 'step3_join_shst_with_osm')


if __name__ == '__main__':
    # create output folder if not exist
    if not os.path.exists(SHST_WITH_OSM_DIR):
        print('creating output folde {}'.format(SHST_WITH_OSM_DIR))
        os.makedirs(SHST_WITH_OSM_DIR)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        SHST_WITH_OSM_DIR,
        "step3_join_shst_extraction_with_osm_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    # 1. Load and consolidate ShSt extracts (from step1_shst_extraction.sh) by reading and combining geofeather files
    WranglerLogger.info('1. Loading SharedStreets extracts from {}'.format(SHST_EXTRACT_DIR))
    shst_link_gdf = methods.read_shst_extract(SHST_EXTRACT_DIR, "*.out.feather")
    WranglerLogger.info('finished reading SharedStreet data; fields: {}'.format(list(shst_link_gdf)))
    #  ['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'roadClass', 'metadata', 'geometry', 'source']

    WranglerLogger.info('Dropping duplicates due to buffer area along polygon boundaries')
    WranglerLogger.info('.. before removing duplicates, shst extract has {} geometries'.format(shst_link_gdf.shape[0]))
    shst_link_gdf.drop_duplicates(subset=['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId'], inplace=True)
    WranglerLogger.info('...after removing duplicates, {} geometries remain'.format(shst_link_gdf.shape[0]))

    # 2. Expand ShSt extract's metadata field into OSM Ways
    #    This step is needed because is come cases, one ShSt extract record (one geometry) contain multiple OSM Ways.
    #    Creates dataframe "osm_ways_from_shst_df" with fields: 
    #       ['nodeIds', 'wayId', 'roadClass', 'oneWay', 'roundabout', 'link', 'name', 'geometryId', 'u', 'v',
    #        'id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry']
    WranglerLogger.info('2. Expanding ShSt extract into OSM Ways with ShSt-specific link attributes')
    osm_ways_from_shst_gdf = methods.extract_osm_links_from_shst_metadata(shst_link_gdf)
    WranglerLogger.info('shst extracts has {} geometries, {} OSM Ways'.format(
        osm_ways_from_shst_gdf.geometryId.nunique(),
        osm_ways_from_shst_gdf.shape[0])
    )
    WranglerLogger.debug('osm_ways_from_shst_gdf has the following OSM fields: {}'.format(list(osm_ways_from_shst_gdf)))

    # 3. Read OSM data from step2_osmnx_extraction.py
    WranglerLogger.info('3. Reading osmnx links from {}'.format(OSM_LINK_FILE))
    osmnx_link_gdf = geofeather.from_geofeather(OSM_LINK_FILE)
    WranglerLogger.info('Finished reading {} rows of osmnx links'.format(len(osmnx_link_gdf)))
    WranglerLogger.debug('osmnx link data has the following attributes:\n{}'.format(osmnx_link_gdf.dtypes))
    # WranglerLogger.info('osmnx node data has the following attributes: {}'.format(list(osmnx_node_gdf)))
    WranglerLogger.debug('head:\n{}'.format(osmnx_link_gdf.head(10)))

    # 4. merge link attributes from OSM extracts with ShSt-derived OSM ways dataframe
    WranglerLogger.info('4. Merging link attributes from OSM extracts with ShSt-derived OSM ways dataframe')
    osmnx_shst_gdf = methods.merge_osmnx_with_shst(osm_ways_from_shst_gdf, osmnx_link_gdf, SHST_WITH_OSM_DIR)
    
    OUTPUT_FILE= os.path.join(SHST_WITH_OSM_DIR, "osmnx_shst.feather")
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {:,} rows to {}".format(len(osmnx_shst_gdf), OUTPUT_FILE))

    # 5. impute lanes counts and clean up turn values to prepare for adding reverse links in the next step, because
    #    OSM data uses different attributes to represent lane count and turn value for two-way and one-way links
    # first, clean up the field types and NAs of lane, turn related attributes
    methods.modify_osmway_lane_accounting_field_type(osmnx_shst_gdf)
    # second, add 'osm_dir_tag' to label two-way and one-way OSM ways
    methods.tag_osm_ways_oneway_twoway(osmnx_shst_gdf)
    # then, impute lane count for each direction
    osmnx_shst_gdf = methods.impute_num_lanes_each_direction_from_osm(osmnx_shst_gdf)
    # also, clean up the strings used in 'turn' values
    methods.cleanup_turns_attributes(osmnx_shst_gdf)
    # impute bus-only lanes
    osmnx_shst_gdf = methods.count_bus_lanes(osmnx_shst_gdf)
    # impute hov lane count
    methods.count_hov_lanes(osmnx_shst_gdf)
    WranglerLogger.debug('osmnx_shst_gdf.dtypes:\n{}'.format(osmnx_shst_gdf.dtypes))

    # 6. add reverse links for two-way OSM ways
    WranglerLogger.info('5. Adding reversed links for two-way OSM ways')
    osmnx_shst_gdf = methods.add_two_way_osm(osmnx_shst_gdf)
    WranglerLogger.debug('after adding two-way links, osm_from_shst_link_gdf has the following fields: {}'.format(
        osmnx_shst_gdf.dtypes))
    WranglerLogger.debug(osmnx_shst_gdf.head())

    # write this to look at it
    OUTPUT_FILE= os.path.join(SHST_WITH_OSM_DIR, "osmnx_shst_gdf.feather")
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {} rows to {}".format(len(osmnx_shst_gdf), OUTPUT_FILE))

    # 7. impute turn lane counts from 'turn:lanes' string


    # 8. fill NAs for ShSt-derived OSM Ways that do not have complete osm info
    # lmz: why is this necessary??
    # WranglerLogger.info('6. Filling NAs for ShSt-derived OSM Ways missing complete osm info')
    # osm_ways_from_shst_non_na_gdf = methods.fill_na(osmnx_shst_gdf)

    # 9. aggregate osm segments back to shst geometry based links
    # WranglerLogger.info('7. Aggregating OSM ways back to shst geometry based links')
    # link_gdf = methods.consolidate_osm_way_to_shst_link(osm_ways_from_shst_non_na_gdf)
    # WranglerLogger.info('......after aggregating back to shst geometry, network has {} links,\
    # which are based on {} geometries'.format(link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))
    # WranglerLogger.debug('the current network link table has the following fields: {}'.format(
    #    list(link_gdf)))
    # WranglerLogger.debug(link_gdf.head())

    # Convert osm "highway" values into standard roadway property based on
    #     the highway_roadway_lookup. For ShSt links containing multiple OSM Ways therefore a list of roadway values,
    #     simplify "highway" value based on the following assumptions:
    #         - if the multiple OSM Ways have the same roadway type, use that type
    #         - if the multiple OSM Ways have different roadway type, use the type with the smallest "hierarchy" value,
    #           i.e. the highest hierarchy. For example, a ShSt link with roadway that contains a 'motorway' OSM Way and
    #           a "footway" OSM Way (['motorway', 'footway']) would be labeled as 'motorway'.
    #         - if missing OSM 'highway' info, use the 'roadClass' field from ShSt extract.
    WranglerLogger.info('7. Converting OSM highway variable into standard roadway variable')
    highway_to_roadway_df = pd.read_csv(HIGHWAY_TO_ROADWAY_CROSSWALK_FILE)

    osmnx_shst_gdf = pd.merge(
        left  = osmnx_shst_gdf, 
        right = highway_to_roadway_df,
        how   = 'left',
        on    = 'highway'
    )
    WranglerLogger.debug('osmnx_shst_gdf.highway.value_counts():\n{}'.format(osmnx_shst_gdf.highway.value_counts()))
    WranglerLogger.debug('osmnx_shst_gdf.roadway_value_counts():\n{}'.format(osmnx_shst_gdf.roadway.value_counts()))

    # 10. clean up: there are links with different shstGeometryId, but same shstReferenceId and to/from nodes. Drop one
    #     of the links with two shstGeometryId. The resulting link table has unique shstReferenceId and to/from nodes.
    ## wait, why?
    # WranglerLogger.info('Dropping link duplicates based on ShstRefenceID and from/to nodes')
    # osmnx_shst_gdf.drop_duplicates(subset=["shstReferenceId"], inplace=True)

    # 11. add network type variables "drive_access", "walk_access", "bike_access" based on pre-defined lookup
    WranglerLogger.info('Adding network type variables "drive_access", "walk_access", "bike_access"')
    network_type_df = pd.read_csv(NETWORK_TYPE_LOOKUP_FILE)
    osmnx_shst_gdf = pd.merge(
        left  = osmnx_shst_gdf,
        right = network_type_df,
        how   = 'left',
        on    = 'roadway')

    WranglerLogger.info('Finished converting SHST extraction into standard network links,' \
        'network has {:,} links, which are based on {:,} geometrics'.format(
        osmnx_shst_gdf.shape[0], osmnx_shst_gdf.shstGeometryId.nunique()))
    WranglerLogger.debug('The standard network links have the following attributes: \n{}'.format(list(osmnx_shst_gdf)))

    # create node gdf from links and attach network type variable
    WranglerLogger.info('Creating nodes from links')
    node_gdf = methods.create_node_gdf(osmnx_shst_gdf)

    WranglerLogger.info('Adding network type variable for node')
    A_B_df = pd.concat(
        [osmnx_shst_gdf[["u", "drive_access", "walk_access", "bike_access"]].rename(columns={"u": "osm_node_id"}),
         osmnx_shst_gdf[["v", "drive_access", "walk_access", "bike_access"]].rename(columns={"v": "osm_node_id"})],
        sort=False,
        ignore_index=True)
    A_B_df.drop_duplicates(inplace=True)
    A_B_df = A_B_df.groupby("osm_node_id").max().reset_index()
    node_gdf = pd.merge(node_gdf,
                        A_B_df,
                        how="left",
                        on="osm_node_id")

    WranglerLogger.info('{:,} network nodes created from {:,} unique osm from/to nodes'.format(
        node_gdf.osm_node_id.nunique(), len(set(osmnx_shst_gdf.u.tolist() + osmnx_shst_gdf.v.tolist()))
    ))
    WranglerLogger.debug('mis-match between network nodes and osm from/to nodes:')
    WranglerLogger.debug(osmnx_shst_gdf[~osmnx_shst_gdf.v.isin(node_gdf.osm_node_id.tolist())])
    WranglerLogger.debug(osmnx_shst_gdf[~osmnx_shst_gdf.u.isin(node_gdf.osm_node_id.tolist())])

    #####################################
    # export link, node, shape

    WranglerLogger.info('Final network links have the following fields: {}'.format(list(osmnx_shst_gdf)))
    WranglerLogger.info('Final network nodes have the following fields: {}'.format(list(node_gdf)))

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_link.feather')
    WranglerLogger.info('Saving links to {}'.format(OUTPUT_FILE))
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_node.feather')
    WranglerLogger.info('Saving nodes to {}'.format(OUTPUT_FILE))
    geofeather.to_geofeather(node_gdf, OUTPUT_FILE)

    WranglerLogger.info('Done')

