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

    # 4a. Record OSMnx 'highway' tag; add columns 'roadway', 'hierarchy' (?), 'drive_access', 'bike_access', 'walk_access'
    osmnx_shst_gdf = methods.recode_osmnx_highway_tag(osmnx_shst_gdf)

    # 5. impute total lane count, bus-only lane count, hov lane count by link direction
    WranglerLogger.info('5. Imputing total lane count and bus-only/hov lane counts')
    # first, clean up the field types and NAs of lane, turn related attributes
    methods.modify_osmway_lane_accounting_field_type(osmnx_shst_gdf)
    # second, add 'osm_dir_tag' to label two-way and one-way OSM ways
    methods.tag_osm_ways_oneway_twoway(osmnx_shst_gdf)
    # count bus-only lanes - add 'forward_bus_lane', 'backward_bus_lane'
    methods.count_bus_lanes(osmnx_shst_gdf, SHST_WITH_OSM_DIR)
    # impute hov lane count - add 'forward_hov_lane'
    methods.count_hov_lanes(osmnx_shst_gdf)
    # then, impute lane count for each direction -- adds 'lane_count_type', 'forward_tot_lanes','backward_tot_lanes','bothways_tot_lanes'
    osmnx_shst_gdf = methods.impute_num_lanes_each_direction_from_osm(osmnx_shst_gdf, SHST_WITH_OSM_DIR)

    WranglerLogger.debug('osmnx_shst_gdf.dtypes:\n{}'.format(osmnx_shst_gdf.dtypes))

    # 6. add reverse links for two-way OSM ways
    WranglerLogger.info('6. Adding reversed links for two-way OSM ways')
    osmnx_shst_gdf = methods.add_two_way_osm(osmnx_shst_gdf)
    WranglerLogger.debug('after adding two-way links, osm_from_shst_link_gdf has the following fields: {}'.format(
        osmnx_shst_gdf.dtypes))
    WranglerLogger.debug(osmnx_shst_gdf.head())

    # write this to look at it
    OUTPUT_FILE= os.path.join(SHST_WITH_OSM_DIR, "osmnx_shst_gdf.feather")
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {:,} rows to {}".format(len(osmnx_shst_gdf), OUTPUT_FILE))

    # 7. impute turn lane counts from 'turn:lanes' string
    WranglerLogger.info('7. Imputing turn lane counts')
    # first, clean up the strings used in 'turn' values
    methods.cleanup_turns_attributes(osmnx_shst_gdf)
    # then, impute
    osmnx_shst_gdf = methods.turn_lane_accounting(osmnx_shst_gdf, SHST_WITH_OSM_DIR)

    # 8. consolidate lane accounting
    WranglerLogger.info('8. Consolidating lane accounting')
    # first, reconcile total lane count
    methods.reconcile_lane_count_inconsistency(osmnx_shst_gdf)
    # lane accounting
    methods.consolidate_lane_accounting(osmnx_shst_gdf)

    # write this to look at it
    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, "osmnx_shst_gdf_lane_accounting_QAQC.feather")
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {} rows to {}".format(len(osmnx_shst_gdf), OUTPUT_FILE))

    # drop interim fields before continue
    osmnx_shst_gdf.drop(columns=[
        'lanes', 'lanes:backward', 'lanes:forward', 'lanes:both_ways',      # raw OSMnx lane count
        'turn', 'turn:lanes', 'turn:lanes:forward', 'turn:lanes:backward',  # raw OSMnx turn info
        'hov', 'hov:lanes', 'lanes:hov',                                    # raw OSMnx hov info
        'bus', 'lanes:bus', 'lanes:bus:forward', 'lanes:bus:backward',      # raw OSMnx bus info
        'forward_tot_lanes', 'backward_tot_lanes',                          # interim lane count
        'bothways_tot_lanes',                                               # interim middle turn count
        'backward_bus_lane', 'forward_bus_lane',                            # interim bus lane count
        'forward_hov_lane',                                                 # interim hov lane count
        'turns:lanes_osmSplit', 'bothways_lane_osmSplit',                   # interim turn info
        'through_only', 'turns_list', 'turns_dict',                         # interim turn lane counts
        'lane_cnt_from_turns', 'lanes_non_gp'                               # validation lane count
        ], inplace=True)

    # 9. consolidate osm ways back to ShSt based links ('shstReferenceId')
    # At this point, osmnx_shst_gdf has duplicated shstReferenceId because some sharedstreets links contain more than
    # one OSM Ways. This step consolidates the values so that each sharedstreets link has one row

    # separate the attributes into different groups based on what consolidation methodology to apply
        # 'attrs_shst_level': attributes that already represent the entire sharedstreets links
    attrs_shst_level = ['id', 'fromIntersectionId', 'toIntersectionId', 'shstReferenceId', 'geometry', 'shstGeometryId']
        # 'attrs_length_based_update': OSM Way attributes; will use the values of the longest OSM way of each
        # sharedstreets link to represent the entire shst link
    attrs_length_based_update = ['link', 'oneway_shst', 'oneway_osmnx', 'osm_dir_tag', 'name_shst_metadata',
                                 'roadClass', 'roundabout', 'highway', 'name',
                                 'maxspeed', 'sidewalk', 'cycleway', 'bridge', 'service', 'width', 'tunnel', 'access',
                                 'ref', 'junction', 'shoulder', 'est_width', 'taxi', 'area', 'lane_count_type',
                                 'lanes_tot', 'lanes_bus', 'lanes_hov', 'lanes_turn', 'lanes_merge_turn', 'lanes_aux',
                                 'lanes_through_turn', 'lanes_middleturn', 'lanes_gp', 'roadway', 'hierarchy',
                                 'drive_access', 'walk_access', 'bike_access']
        # 'attrs_location_based_update': OSM Way attributes; will need to update based on their location in the ShSt link
    attrs_location_based_update = ['nodeIds', 'wayId', 'waySections_len', 'u', 'v', 'osmid', 'length',
                                   '_merge', 'reverse', 'index']

    # length-based updates
    osmnx_shst_gdf = methods.update_attributes_based_on_way_length(osmnx_shst_gdf, attrs_length_based_update)
    # TODO: location-based updates


    # 9. fill NAs for ShSt-derived OSM Ways that do not have complete osm info
    # lmz: why is this necessary??
    # WranglerLogger.info('6. Filling NAs for ShSt-derived OSM Ways missing complete osm info')
    # osm_ways_from_shst_non_na_gdf = methods.fill_na(osmnx_shst_gdf)

    # 10. aggregate osm segments back to shst geometry based links
    # WranglerLogger.info('10. Aggregating OSM ways back to shst geometry based links')
    # link_gdf = methods.consolidate_osm_way_to_shst_link(osm_ways_from_shst_non_na_gdf)
    # WranglerLogger.info('......after aggregating back to shst geometry, network has {} links,\
    # which are based on {} geometries'.format(link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))
    # WranglerLogger.debug('the current network link table has the following fields: {}'.format(
    #    list(link_gdf)))
    # WranglerLogger.debug(link_gdf.head())

    # 10. clean up: there are links with different shstGeometryId, but same shstReferenceId and to/from nodes. Drop one
    #     of the links with two shstGeometryId. The resulting link table has unique shstReferenceId and to/from nodes.
    ## wait, why?
    # WranglerLogger.info('Dropping link duplicates based on ShstRefenceID and from/to nodes')
    # osmnx_shst_gdf.drop_duplicates(subset=["shstReferenceId"], inplace=True)

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

    WranglerLogger.info('Final network links have the following fields:\n{}'.format(osmnx_shst_gdf.dtypes))
    WranglerLogger.info('Final network nodes have the following fields:\n{}'.format(node_gdf.dtypes))

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_link.feather')
    WranglerLogger.info('Saving links to {}'.format(OUTPUT_FILE))
    geofeather.to_geofeather(osmnx_shst_gdf, OUTPUT_FILE)

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_node.feather')
    WranglerLogger.info('Saving nodes to {}'.format(OUTPUT_FILE))
    geofeather.to_geofeather(node_gdf, OUTPUT_FILE)

    WranglerLogger.info('Done')

