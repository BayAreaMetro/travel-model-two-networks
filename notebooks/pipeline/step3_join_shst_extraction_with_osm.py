USAGE = """

Add OSM attributes to extracted SharedStreets network and convert to Network Standard data formats with links and nodes.

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
import geofeather  # this is fast
import datetime, json, os, sys

from methods import extract_osm_links_from_shst_extraction, osm_link_with_shst_info, merge_osmnx_attributes_with_shst,\
    add_two_way_osm, fill_na, consolidate_osm_way_to_shst_link, highway_attribute_list_to_value, create_node_gdf, \
    read_shst_extract, link_df_to_geojson, point_df_to_geojson

from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs

INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# OSM extraction and SharedStreet extraction
OSM_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step2_osmnx_extracts')
OSM_LINK_FILE   = os.path.join(OSM_EXTRACT_DIR, 'link.feather')
## OSM_NODE_FILE = os.path.join(OSM_EXTRACT_DIR, 'node.geojson')
SHST_EXTRACT_DIR = os.path.join(INPUT_DATA_DIR, 'external', 'step1_shst_extracts')
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
        "step3_join_shst_extraction_with_osm_{}.info.log".format(datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))


    #####################################
    # load OSM data
    WranglerLogger.info('Reading osmnx links from {}'.format(OSM_LINK_FILE))
    osmnx_link_gdf = geofeather.from_geofeather(OSM_LINK_FILE)
    # looks like osm node data is not used ?
    # osmnx_node_gdf = gpd.read_file(OSM_NODE_FILE)
    WranglerLogger.info('Finished reading {} rows of osmnx links'.format(len(osmnx_link_gdf)))
    WranglerLogger.info('osmnx link data has the following attributes:\n{}'.format(osmnx_link_gdf.dtypes))
    # WranglerLogger.info('osmnx node data has the following attributes: {}'.format(list(osmnx_node_gdf)))

    WranglerLogger.info(osmnx_link_gdf.head(3))


    #####################################
    # load ShSt extracts, merges in link attributes from OSM extracts, and process to standard roadway network. Steps:
    #  1. loads and consolidates SHST extracts, including method "read_shst_extract" to read and combine geojson files,
    #     and removing duplicates due to buffer area along polygon boundries
    #  2. method "extract_osm_links_from_shst_extraction" expands each ShSt extract record (row) into OSM Ways with
    #     basic osm attributes embedded in ShSt extract's "metadata" field ("metadata/osmMetadata/waySections").
    #     This step is needed because is come cases, one ShSt extract record (one geometry) contain multiple OSM Ways.
    #     Creates dataframe "osmWays_from_shst_df" with fields: ['nodeIds', 'wayId', 'roadClass', 'oneWay',
    #                                                            'roundabout', 'link', 'name', 'geometryId', 'u', 'v'].
    #  3. method "osm_link_with_shst_info" merges ShSt-specific attributes into osmWays_from_shst_df, including:
    #     ['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry'].
    #  4. method "merge_osmnx_attributes_with_shst" merges link attributes from OSM extracts with ShSt-derived OSM Ways.
    #  5. method "add_two_way_osm" creates a reversed link for each OSM two-way Way, and adds it to the previous link
    #     dataframe. The resulting "osmWays_allAttrs_withReverse_gdf" should have one row representing each link in one
    #     direction.
    #  6. method "fill_na" fills NAs for ShSt-derived OSM Ways that do not have complete OSM info.
    #  7. method "consolidate_osm_way_to_shst_link" aggregates ShSt-derived OSM Ways back to ShSt geometry based links.
    #     For ShSt links that contain more than one OSM Ways, the link attributes become a list of the individual Ways'
    #     attributes.
    #  8. method "highway_attribute_list_to_value" converts osm "highway" values into standard roadway property based on
    #     the highway_roadway_lookup. For ShSt links containing multiple OSM Ways therefore a list of roadway values,
    #     simplify "highway" value based on the following assumptions:
    #         - if the multiple OSM Ways have the same roadway type, use that type
    #         - if the multiple OSM Ways have different roadway type, use the type with the smallest "hierarchy" value,
    #           i.e. the highest hierarchy. For example, a ShSt link with roadway that contains a 'motorway' OSM Way and
    #           a "footway" OSM Way (['motorway', 'footway']) would be labeled as 'motorway'.
    #         - if missing OSM 'highway' info, use the 'roadClass' field from ShSt extract.
    #  9. clean up: there are links with different shstGeometryId, but same shstReferenceId and to/from nodes. Drop one
    #     of the links with two shstGeometryId. The resulting link table has unique shstReferenceId and to/from nodes.
    #  10. add network type variables "drive_access", "walk_access", "bike_access" based on pre-defined
    #      network_type_indicator lookup.

    # 1. load and consolidate ShSt extracts
    WranglerLogger.info('1. Loading SharedStreets extracts from {}'.format(SHST_EXTRACT_DIR))
    shst_link_gdf = read_shst_extract(SHST_EXTRACT_DIR, "*.out.feather")
    WranglerLogger.info('finished reading SharedStreet data')
    WranglerLogger.info('SharedStreet data has the following fields: {}'.format(list(shst_link_gdf)))
    WranglerLogger.info(shst_link_gdf.head(3))

    WranglerLogger.info('Dropping duplicates')
    WranglerLogger.info('.. before removing duplicates, shst extract has {} geometries'.format(shst_link_gdf.shape[0]))
    shst_link_gdf.drop_duplicates(
        subset=['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId'], inplace=True)
    WranglerLogger.info('...after removing duplicates, {} geometries remain'.format(shst_link_gdf.shape[0]))

    # 2. expand ShSt extract into OSM Ways
    # Note: this step is memory intensive and time-consuming
    WranglerLogger.info('2. Expanding ShSt extract into OSM Ways with ShSt-specific link attributes')
    # osm_from_shst_link_list = []
    #
    # temp = shst_link_gdf.apply(lambda x: extract_osm_link_from_shst_shape(x, osm_from_shst_link_list),
    #                                    axis=1)
    # osm_from_shst_link_df = pd.concat(osm_from_shst_link_list)
    osmWays_from_shst_df = extract_osm_links_from_shst_extraction(shst_link_gdf)
    WranglerLogger.info('shst extracts has {} geometries, {} OSM Ways'.format(
        osmWays_from_shst_df.geometryId.nunique(),
        osmWays_from_shst_df.shape[0])
    )
    WranglerLogger.debug('osmWays_from_shst_df has the following OSM fields: {}'.format(list(osmWays_from_shst_df)))
    WranglerLogger.debug(osmWays_from_shst_df.head())
    WranglerLogger.debug("osmWays_from_shst_df.waySedtions_len.value_counts():\n{}".format(osmWays_from_shst_df.waySections_len.value_counts()))
    # temp for debugging

    # 3. add ShSt-specific attributes to ShSt-derived OSM Ways
    WranglerLogger.info('3. Adding ShSt-specific attributes to ShSt-derived OSM Ways')
    osmWays_from_shst_gdf = osm_link_with_shst_info(osmWays_from_shst_df,
                                                    shst_link_gdf)
    WranglerLogger.debug('osmWays_from_shst_gdf has the following fields: {}'.format(list(osmWays_from_shst_gdf)))
    WranglerLogger.debug(osmWays_from_shst_gdf.head())

    # 4. merge link attributes from OSM extracts with ShSt-derived OSM Ways dataframe
    WranglerLogger.info('4. Merging link attributes from OSM extracts with ShSt-derived OSM Ways dataframe')
    osmWays_from_shst_allAttrs_gdf = merge_osmnx_attributes_with_shst(osmWays_from_shst_gdf, osmnx_link_gdf)
    WranglerLogger.debug('osmWays_all_attrs_gdf has the following fields: {}'.format(
        list(osmWays_from_shst_allAttrs_gdf)))
    WranglerLogger.debug(osmWays_from_shst_allAttrs_gdf.head())

    # 5. add reversed links for two-way OSM Ways
    WranglerLogger.info('5. Adding reversed links for two-way OSM Ways')
    osmWays_from_shst_allAttrs_withReverse_gdf = add_two_way_osm(osmWays_from_shst_allAttrs_gdf)
    WranglerLogger.debug('after adding two-way links, osm_from_shst_link_gdf has the following fields: {}'.format(
        list(osmWays_from_shst_allAttrs_withReverse_gdf)))
    WranglerLogger.debug(osmWays_from_shst_allAttrs_withReverse_gdf.head())

    # 6. fill NAs for ShSt-derived OSM Ways that do not have complete osm info
    WranglerLogger.info('6. Filling NAs for ShSt-derived OSM Ways missing complete osm info')
    osmWays_from_shst_non_na_gdf = fill_na(osmWays_from_shst_allAttrs_withReverse_gdf)

    # 7. aggregate osm segments back to shst geometry based links
    WranglerLogger.info('7. Aggregating OSM Ways back to shst geometry based links')
    link_gdf = consolidate_osm_way_to_shst_link(osmWays_from_shst_non_na_gdf)
    WranglerLogger.info('......after aggregating back to shst geometry, network has {} links,\
    which are based on {} geometries'.format(link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))
    WranglerLogger.debug('the current network link table has the following fields: {}'.format(
        list(link_gdf)))
    WranglerLogger.debug(link_gdf.head())

    # 8. convert osm "highway" values into standard roadway property
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

    WranglerLogger.debug('link counts by roadway types: \n{}'.format(link_gdf.roadway.value_counts()))
    WranglerLogger.debug('roadway data from roadClass while OSM highway value is missing: \n{}'.format(
        link_gdf[link_gdf.highway == ''].roadway.value_counts()
    ))

    # 9. clean up
    WranglerLogger.info('Dropping link duplicates based on ShstRefenceID and from/to nodes')
    link_gdf.drop_duplicates(subset=["shstReferenceId"], inplace=True)

    # 10. add network type variables "drive_access", "walk_access", "bike_access" based on pre-defined lookup
    WranglerLogger.info('Adding network type variables "drive_access", "walk_access", "bike_access"')
    network_type_df = pd.read_csv(NETWORK_TYPE_LOOKUP_FILE)

    link_gdf = pd.merge(link_gdf,
                        network_type_df,
                        how='left',
                        on='roadway')

    WranglerLogger.info('Finished converting SHST extraction into standard network links,\
    network has {} links, which are based on {} geometrics'.format(
        link_gdf.shape[0], link_gdf.shstGeometryId.nunique()))
    WranglerLogger.debug('The standard network links have the following attributes: \n{}'.format(list(link_gdf)))


    #####################################
    # create shapes based on the network links
    WranglerLogger.info('Creating shapes from links')
    shape_gdf = shst_link_gdf[shst_link_gdf.id.isin(link_gdf.shstGeometryId.tolist())]
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

    # TODO: export the data in other format, probably need to adjust field type before exporting
