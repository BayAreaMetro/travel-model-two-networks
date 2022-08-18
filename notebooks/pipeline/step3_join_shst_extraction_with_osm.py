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

from warnings import WarningMessage
import pandas as pd
import geopandas as gpd
import geofeather  # this is fast
from scipy.spatial import cKDTree
from pyproj import CRS
import datetime, json, os, sys

import methods
from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# OSM extraction and SharedStreet extraction
SHST_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'step1_shst_extracts')
OSM_EXTRACT_DIR  = os.path.join(INPUT_DATA_DIR, 'step2_osmnx_extracts')
OSM_LINK_FILE    = os.path.join(OSM_EXTRACT_DIR, 'link.feather')
# county boundaries shapefile
COUNTY_FILE = os.path.join(INPUT_DATA_DIR, 'step0_boundaries', 'cb_2018_us_county_500k', 'cb_2018_us_county_500k.shp')

# This script will write to this directory
SHST_WITH_OSM_DIR = os.path.join(OUTPUT_DATA_DIR, 'step3_join_shst_with_osm')


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

    # EPSG for nearest match: TARGET_EPSG = 26915
    nearest_match_epsg_str = 'epsg:{}'.format(str(26915))
    WranglerLogger.info('nearest match ESPG: {}'.format(nearest_match_epsg_str))

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
    #    Creates dataframe "osm_ways_from_shst_gdf" with fields:
    #     ['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry', 'link',
    #      'name', 'nodeIds', 'oneWay', 'roadClass', 'roundabout', 'wayId', 'waySections_len', 'waySection_ord', 'geometryId', 'u', 'v']
    WranglerLogger.info('2. Expanding ShSt extract into OSM Ways with ShSt-specific link attributes')
    osm_ways_from_shst_gdf = methods.extract_osm_links_from_shst_metadata(shst_link_gdf)
    WranglerLogger.info('shst extracts has {} geometries, {} OSM Ways'.format(
        osm_ways_from_shst_gdf.geometryId.nunique(),
        osm_ways_from_shst_gdf.shape[0])
    )
    WranglerLogger.debug('osm_ways_from_shst_gdf has the following fields: {}'.format(list(osm_ways_from_shst_gdf)))

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
    osmnx_shst_gdf = methods.recode_osmnx_highway_tag(osmnx_shst_gdf, methods.HIGHWAY_TO_ROADWAY, methods.ROADWAY_TO_ACCESS)

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
    methods.impute_num_lanes_each_direction_from_osm(osmnx_shst_gdf, SHST_WITH_OSM_DIR)

    WranglerLogger.debug('osmnx_shst_gdf.dtypes:\n{}'.format(osmnx_shst_gdf.dtypes))

    # 6. add reverse links for two-way OSM ways
    # Note: after adding reverse links, a link and its reverse link have different 'fromIntersectionId', 'toIntersectionId',
    # 'shstReferenceId', 'geometry', but still the same 'id' and 'shstGeometryId'
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
    # then, count turn lanes from 'turns:lanes_osmSplit'; this adds columns 'through_turn','merge_only',
    # 'through_only',turn_only','lane_count_from_turns','middle_turn','merge_turn'
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
    WranglerLogger.info("Wrote {:,} rows to {}".format(len(osmnx_shst_gdf), OUTPUT_FILE))

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
        'through_only',                                                     # interim turn lane counts
        'lane_count_from_turns', 'lanes_non_gp'                             # validation lane count
        ], inplace=True)

    # 9. aggregate osm ways back to ShSt based links
    # At this point, osmnx_shst_gdf has duplicated shstReferenceId because some sharedstreets links contain more than
    # one OSM Ways. This step consolidates the values so that each sharedstreets link has one row.
    WranglerLogger.info('9. Aggregating OSM ways back to SharedStreets-based links')
    shst_aggregated_gdf = methods.aggregate_osm_ways_back_to_shst_link(osmnx_shst_gdf)
    WranglerLogger.info('Before aggregating osm ways back to sharedstreets-based links, osmnx_shst_gdf has {:,} links, '
                        'representing {:,} unique sharedstreets links; after aggregating, shst_aggregated_gdf has {:,} links'.format(
                            osmnx_shst_gdf.shape[0],
                            osmnx_shst_gdf.drop_duplicates(subset=['id', 'fromIntersectionId',
                                                                   'toIntersectionId', 'shstReferenceId',
                                                                   'shstGeometryId']).shape[0],
                            shst_aggregated_gdf.shape[0]))
    WranglerLogger.info('shst_aggregated_gdf has columns {}; head:\n{}'.format(
        list(shst_aggregated_gdf.columns), shst_aggregated_gdf.head()))                       

    # write this to look at it
    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, "shst_consolidated_gdf_QAQC.feather")
    geofeather.to_geofeather(shst_aggregated_gdf, OUTPUT_FILE)
    WranglerLogger.info("Wrote {:,} rows to {}".format(len(shst_aggregated_gdf), OUTPUT_FILE))

    WranglerLogger.info(
        'Finished converting SHST extraction into standard network links with {:,} links'.format(shst_aggregated_gdf.shape[0]))
    WranglerLogger.debug(
        '{:,} unique shstReferenceId, {:,} unique u/v pairs, {:,} unique fromIntersectionId/toIntersectionId pairs'.format(
            shst_aggregated_gdf['shstReferenceId'].nunique(),
            shst_aggregated_gdf[['u', 'v']].drop_duplicates().shape[0],
            shst_aggregated_gdf[['fromIntersectionId', 'toIntersectionId']].drop_duplicates().shape[0]
    ))
    WranglerLogger.debug('Standard network links have the following attributes: \n{}'.format(list(shst_aggregated_gdf)))

    # 10. create node gdf from links and attach network type variable
    WranglerLogger.info('10. Creating nodes from links')
    node_gdf = methods.create_node_gdf(shst_aggregated_gdf)

    WranglerLogger.info('Adding network type variable for node')
    A_B_df = pd.concat(
        [shst_aggregated_gdf[["u", "drive_access", "walk_access", "bike_access"]].rename(columns={"u": "osm_node_id"}),
         shst_aggregated_gdf[["v", "drive_access", "walk_access", "bike_access"]].rename(columns={"v": "osm_node_id"})],
        sort=False,
        ignore_index=True)
    A_B_df.drop_duplicates(inplace=True)
    A_B_df = A_B_df.groupby("osm_node_id").max().reset_index()
    node_gdf = pd.merge(node_gdf,
                        A_B_df,
                        how="left",
                        on="osm_node_id")

    WranglerLogger.info('{:,} network nodes created from {:,} unique osm way from/to nodes'.format(
        node_gdf.osm_node_id.nunique(), len(set(shst_aggregated_gdf.u.tolist() + shst_aggregated_gdf.v.tolist()))
    ))
    WranglerLogger.debug('mis-match between network nodes and osm from/to nodes:')
    WranglerLogger.debug(shst_aggregated_gdf[~shst_aggregated_gdf.v.isin(node_gdf.osm_node_id.tolist())])
    WranglerLogger.debug(shst_aggregated_gdf[~shst_aggregated_gdf.u.isin(node_gdf.osm_node_id.tolist())])

    # 11. label links and nodes with county names, and remove out-of-region links and nodes
    WranglerLogger.info('11. Tagging links and nodes by county and removing out-of-region links and nodes')

    # # tag nodes and links by county
    node_gdf, shst_aggregated_gdf = methods.tag_nodes_links_by_county_name(node_gdf, shst_aggregated_gdf, COUNTY_FILE)
    WranglerLogger.info('finished tagging nodes with county names. Total {:,} nodes, counts by county:\n{}'.format(
        node_gdf.shape[0],
        node_gdf['county'].value_counts(dropna=False)))
    WranglerLogger.info('finished tagging links with county names. Total {:,} links, counts by county:\n{}'.format(
        shst_aggregated_gdf.shape[0],
        shst_aggregated_gdf['county'].value_counts(dropna=False)))
    
    # write to QAQC
    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, "link_county_tag_QAQC.feather")
    WranglerLogger.info("Writing {:,} rows with columns {} to {}".format(len(shst_aggregated_gdf), 
        list(shst_aggregated_gdf.columns), OUTPUT_FILE))
    geofeather.to_geofeather(shst_aggregated_gdf, OUTPUT_FILE)

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, "node_county_tag_QAQC.feather")
    WranglerLogger.info("Writing {:,} rows with columns {} to {}".format(len(node_gdf), 
        list(node_gdf.columns), OUTPUT_FILE))        
    geofeather.to_geofeather(node_gdf, OUTPUT_FILE)

    # remove out-of-region links and nodes
    # for nodes that are out-of-region but used in cross-region links, keep them and re-label to Bay Area counties
    WranglerLogger.info('dropping out-of-the-region links and nodes')
    link_BayArea_gdf, node_BayArea_gdf = methods.remove_out_of_region_links_nodes(shst_aggregated_gdf, node_gdf)
    WranglerLogger.info('after dropping, {:,} Bay Area links and {:,} Bay Area nodes remain'.format(
        link_BayArea_gdf.shape[0], 
        node_BayArea_gdf.shape[0]))

    # 12. reconcile link length
    # NOTE: the initial Pipeline process didn't extract "length" from OSMnx, but calculated meter length based on geometry.
    # May comapre the two and decide which one to use.
    # Links with 'length_osmnx' == 0 are links in shst, but the corresponding wayId no longer exists in osmnx.
    WranglerLogger.info('12. Reconciling OSMnx extraction link length and geometry-based meter link length')
    # add '_osmnx' suffix to 'length' field from OSMNX
    if 'length' in link_BayArea_gdf.columns:
        link_BayArea_gdf.rename(columns={'length': 'length_osmnx'}, inplace=True)
    
    # geom_length = link_BayArea_gdf[['geometry']].copy()
    # convert to EPSG 26915 for meter unit
    geom_length_gdf = gpd.GeoDataFrame(link_BayArea_gdf[['geometry']].copy(),
                                       geometry = 'geometry',
                                       crs=link_BayArea_gdf.crs)
    geom_length_gdf.to_crs(CRS(nearest_match_epsg_str), inplace=True)
    # calculate meter length
    geom_length_gdf.loc[:, 'length_meter'] = geom_length_gdf.length
    # add to link_BayArea_gdf
    link_BayArea_gdf['length_meter'] = geom_length_gdf['length_meter']

    #####################################
    # some clean up

    # 1. manually correct roadway type and access values at Transbay temporary terminal
    WranglerLogger.debug('manually correction for Transbay temporary terminal')
    transbay_terminal_link_idx = link_BayArea_gdf['shstReferenceId'].isin(["feab62cc90650bfc45dc453816782f9c", "9ab364b22d6b33ec158d8bc4008c1be7"])
    WranglerLogger.debug('links before correction: \n{}'.format(
        link_BayArea_gdf.loc[transbay_terminal_link_idx][["roadway", "drive_access", "walk_access", "bike_access"]]))

    # set roadway type as 'service', drive_access = 1, and hierarchy==12
    link_BayArea_gdf.loc[transbay_terminal_link_idx, "roadway"] = "service"
    link_BayArea_gdf.loc[transbay_terminal_link_idx, "drive_access"] = True
    link_BayArea_gdf.loc[transbay_terminal_link_idx, "hierarchy"] = 12
    WranglerLogger.debug('links after correction: \n{}'.format(
        link_BayArea_gdf.loc[transbay_terminal_link_idx][["roadway", "drive_access", "walk_access", "bike_access"]]))

    # related nodes
    transbay_terminal_node_ls = list(link_BayArea_gdf.loc[transbay_terminal_link_idx].u.unique()) + \
                                list(link_BayArea_gdf.loc[transbay_terminal_link_idx].v.unique())
    WranglerLogger.debug('nodes before correction: \n{}'.format(
        node_BayArea_gdf[node_BayArea_gdf.osm_node_id.isin(transbay_terminal_node_ls)]))
    # set drive_access = 1
    node_BayArea_gdf.loc[node_BayArea_gdf.osm_node_id.isin(transbay_terminal_node_ls), "drive_access"] = True
    WranglerLogger.debug('nodes after correction: \n{}'.format(
        node_BayArea_gdf[node_BayArea_gdf.osm_node_id.isin(transbay_terminal_node_ls)]
    ))

    # 2. drop circular links (u == v) and circular-link-only nodes
    # NOTE: circular links often cause duplicated shstReferenceId, i.e. same shstReferenceId and to/from nodes (same u/v), but
    # different 'shstGeometryId' (from shst) and 'id' (from osmnx), e.g. shstReferenceId == '00f297d3c36358ee88d583809275164c'.
    circular_link_gdf = link_BayArea_gdf.loc[link_BayArea_gdf.u == link_BayArea_gdf.v]
    link_BayArea_gdf = link_BayArea_gdf.loc[
        ~link_BayArea_gdf.shstReferenceId.isin(circular_link_gdf.shstReferenceId.tolist())]

    node_BayArea_gdf = node_BayArea_gdf.loc[
        (node_BayArea_gdf.osm_node_id.isin(link_BayArea_gdf.u.tolist())) | \
        (node_BayArea_gdf.osm_node_id.isin(link_BayArea_gdf.v.tolist()))
    ]

    # 3. drop duplicated ['fromIntersectionId', 'toIntersectionId', 'u', 'v', 'shstReferenceId']
    # There are links with different 'shstGeometryId' (from shst) and 'id' (from osmnx), but same shstReferenceId and to/from nodes.
    # (FYI: a link and its reverse link have the same 'shstGeometryId' and 'id')
    # Different reasons for duplicated shstReferenceId:
    #   1) discrepancy between shst and osmnx (showing in 'osmnx_shst_merge'). e.g. shstReferenceId == '001516bb08e57c92f78df14c9dcfb6d7',
    #      shst has one link between nodeIds '1276183745'/'1276184111' with wayId 112192609, but osmnx has no link with osmid 112192609, 
    #      but has osmid 112192618 between the same nodeIds '1276183745'/'1276184111', with different shstgeometryId and id. 
    #   2) "duplicates" in shst raw data. For two-way road, shst should have only one record, the opposite-diretion link to be created
    #      in methods.add_two_way_osm(). However, there are cases where shst raw data already contains two records, one for each direction,
    #      with opposite 'toIntersectionId' vs 'fromIntersectionId', 'forwardReferenceId' vs 'backReferenceId', and reversed 'nodeIds' orders,
    #      but different 'geometryId' and osmnx 'wayId'. Then in methods.add_two_way_osm(), a reversed link was created for each record,
    #      thus duplicating ['fromIntersectionId', 'toIntersectionId', 'u', 'v', 'shstReferenceId'] with different 'shstGeometryId' and 'id'.
    #      For example, the following two records in shst extracts 'metadata' (with different wayId):
    #      - forwardReferenceId 'fe8c82b6649b0213b0dda0e70199cd9a'
    #        id 'cfde28acf3e40d96f69ae5a666b4d2e3'
    #        geometryId 'cfde28acf3e40d96f69ae5a666b4d2e3'
    #        'osmMetadata': {'name': '',
    #                        'waySections': array([{'link': False, 
    #                                               'name': '', 
    #                                               'nodeIds': array(['4567863491', '342441731'], dtype=object),
    #                                               'oneWay': False, 
    #                                               'roadClass': 'Other', 
    #                                               'roundabout': False, 
    #                                               'wayId': '461257875',
    #                                               'waySections_len': 1, 
    #                                               'waySection_ord': 1, 
    #                                               'geometryId': 'cfde28acf3e40d96f69ae5a666b4d2e3'}], dtype=object)
    #      - forwardReferenceId 'cee421918b93e675a9632648572c6c17'
    #        id 'cbea24b2472d3bf58cb431d06ee81ff9'
    #        geometryId 'cbea24b2472d3bf58cb431d06ee81ff9'
    #        'osmMetadata': {'name': '', 
    #                        'waySections': array([{'link': False, 
    #                                               'name': '', 
    #                                               'nodeIds': array(['342441731', '4567863491'], dtype=object), 
    #                                               'oneWay': False, 
    #                                               'roadClass': 'Other', 
    #                                               'roundabout': False, 
    #                                               'wayId': '461257874', 
    #                                               'waySections_len': 1, 
    #                                               'waySection_ord': 1, 
    #                                               'geometryId': 'cbea24b2472d3bf58cb431d06ee81ff9'}], dtype=object)
    #     Another example: forwardReferenceId '00f297d3c36358ee88d583809275164c' and '869f004cd90e223a50c6a78bb225417e', with
    #         same wayId '8919831', but different geometryId '77c0401e481ae9e40936cf4dee3922bf' and '49d22c14da4261f446c7ab73bb5a3500'
    # 
    # export to debug
    link_BayArea_geometryId_debug = link_BayArea_gdf.copy()
    link_BayArea_geometryId_debug.loc[:, 'shst_link_cnt'] = link_BayArea_geometryId_debug.groupby(
        ['fromIntersectionId', 'toIntersectionId', 'u', 'v', 'shstReferenceId'])['id'].transform('size')
    link_BayArea_geometryId_debug.reset_index(inplace=True, drop=True)
    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, "link_BayArea_geometryId_QAQC.feather")
    geofeather.to_geofeather(link_BayArea_geometryId_debug, OUTPUT_FILE)
    WranglerLogger.info("Wrote {:,} rows to {}".format(len(link_BayArea_geometryId_debug), OUTPUT_FILE))
    del link_BayArea_geometryId_debug

    # In case 1), suggest dropping the one that has no osmnx info (length==0, osmid==0 (due to fillna), index==0, osmnx_shst_merge=='shst_only').
    # In such cases, the duplicates are essentially the same link, so keeping any of them should be fine.
    unique_link_BayArea_gdf = link_BayArea_gdf.sort_values(by=['osmid'], ascending=[False])
    unique_link_BayArea_gdf = unique_link_BayArea_gdf.drop_duplicates(
        subset=['fromIntersectionId', 'toIntersectionId', 'u', 'v', 'shstReferenceId'], 
        keep='first')
    # unique_link_BayArea_gdf become a dataframe after drop_duplicates(), so convert it back
    unique_link_BayArea_gdf = gpd.GeoDataFrame(unique_link_BayArea_gdf, 
                                               geometry='geometry', 
                                               crs=link_BayArea_gdf.crs)
    link_BayArea_gdf = unique_link_BayArea_gdf.copy()

    # 4. drop duplicated links between same u/v pair
    # NOTE: this step relied on links having no duplicated shstReferenceId
    link_BayArea_gdf = methods.drop_duplicated_links_between_same_u_v_pair(link_BayArea_gdf, SHST_WITH_OSM_DIR)

    # TODO: maybe move to later, after transit routing?
    # 5. flag drive dead end, and make dead-end links and nodes drive_access=0
    link_BayArea_gdf, node_BayArea_gdf = methods.make_dead_end_non_drive(link_BayArea_gdf, node_BayArea_gdf)

    #####################################
    # export link, node

    WranglerLogger.info('Final network links have the following fields:\n{}'.format(link_BayArea_gdf.dtypes))
    WranglerLogger.info('Final network nodes have the following fields:\n{}'.format(node_BayArea_gdf.dtypes))

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_link.feather')
    WranglerLogger.info('Saving links to {}'.format(OUTPUT_FILE))
    link_BayArea_gdf.reset_index(inplace=True, drop=True)
    geofeather.to_geofeather(link_BayArea_gdf, OUTPUT_FILE)

    OUTPUT_FILE = os.path.join(SHST_WITH_OSM_DIR, 'step3_node.feather')
    WranglerLogger.info('Saving nodes to {}'.format(OUTPUT_FILE))
    node_BayArea_gdf.reset_index(inplace=True, drop=True)
    geofeather.to_geofeather(node_BayArea_gdf, OUTPUT_FILE)

    WranglerLogger.info('Done')

