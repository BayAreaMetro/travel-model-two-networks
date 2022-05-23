USAGE = """
  

"""

import datetime, json, os
import pandas as pd
import geopandas as gpd
import geofeather
import numpy as np
from scipy.spatial import cKDTree
import methods

# from methods import link_df_to_geojson
# from methods import point_df_to_geojson
# from methods import identify_dead_end_nodes
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging

#####################################
# EPSG requirement: TARGET_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)
nearest_match_epsg_str = 'epsg:{}'.format(str(26915))
WranglerLogger.info('nearest match ESPG: ', nearest_match_epsg_str)

#####################################
# inputs and outputs

INPUT_DATA_DIR = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# links and nodes
# LINK_INPUT = os.path.join(INPUT_DATA_DIR, 'step4d_conflate_with_third_party', 'step4_link.feather')
LINK_INPUT = os.path.join(INPUT_DATA_DIR, 'step3_join_shst_with_osm', 'step3_link.feather')
NODE_INPUT = os.path.join(INPUT_DATA_DIR, 'step3_join_shst_with_osm', 'step3_node.feather')
# county shapefile
COUNTY_FILE = os.path.join(INPUT_DATA_DIR, 'step0_boundaries', 'cb_2018_us_county_500k', 'cb_2018_us_county_500k.shp')
# output
TIDY_ROADWAY_DIR = os.path.join(OUTPUT_DATA_DIR, 'step5_tidy_roadway')
# LINK_SHAPE_OUTPUT = os.path.join(TIDY_ROADWAY_DIR, 'step5_shape.geojson')
# LINK_JSON_OUTPUT = os.path.join(TIDY_ROADWAY_DIR, 'step5_link.json')
LINK_OUTPUT = os.path.join(TIDY_ROADWAY_DIR, 'step5_link.feather')
NODE_OUTPUT = os.path.join(TIDY_ROADWAY_DIR, 'step5_node.feather')

# county list
BayArea_COUNTIES = ['San Francisco', 'Santa Clara', 'Sonoma', 'Marin', 'San Mateo',
                    'Contra Costa', 'Solano', 'Napa', 'Alameda']

if __name__ == '__main__':
    # create output directories if not exist
    if not os.path.exists(TIDY_ROADWAY_DIR):
        WranglerLogger.info('create output folder: {}'.format(TIDY_ROADWAY_DIR))
        os.makedirs(TIDY_ROADWAY_DIR)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        TIDY_ROADWAY_DIR,
        "step5_tidy_roadway_{}.info.log".format(
            datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    ####################################
    # Load network links and nodes
    # WranglerLogger.info('loading link shapes from {}'.format(LINK_SHAPE_INPUT))
    # link_shape_gdf = gpd.read_file(LINK_SHAPE_INPUT)
    # WranglerLogger.debug('{} shapes, with fields:\n{}'.format(link_shape_gdf.shape[0], list(link_shape_gdf)))

    WranglerLogger.info('loading links from {}'.format(LINK_INPUT))
    link_gdf = geofeather.from_geofeather(LINK_INPUT)
    WranglerLogger.debug('{} links, with fields:\n{}'.format(link_gdf.shape[0], list(link_gdf)))

    WranglerLogger.info('loading nodes from {}'.format(NODE_INPUT))
    node_gdf = geofeather.from_geofeather(NODE_INPUT)
    WranglerLogger.debug('{} nodes, with fields:\n{}'.format(node_gdf.shape[0], list(node_gdf)))

    ####################################
    # Join county name to links and nodes

    WranglerLogger.info('loading county shapefile {}'.format(COUNTY_FILE))
    county_gdf = gpd.read_file(COUNTY_FILE)
    WranglerLogger.info('convert to link CRS')
    county_gdf = county_gdf.to_crs(link_gdf.crs)

    # spatial join nodes with county shape
    WranglerLogger.info('Spatially joining nodes with county shape')
    node_county_gdf = gpd.sjoin(node_gdf, county_gdf, how='left', op='intersects')

    # some nodes may get joined to more than one county
    WranglerLogger.debug('# of unique nodes: {}'.format(node_gdf.shape[0]))
    WranglerLogger.debug('# of nodes in spatial join result: {}'.format(node_county_gdf.shape[0]))
    WranglerLogger.debug('# of unique nodes in spatial join result: {}'.format(node_county_gdf.shst_node_id.nunique()))

    # use nearest match to fill in names for nodes that did not get county match (e.g. in the Bay)
    # first, use cKDTree to construct k-dimensional points based on the matched nodes; then, for each unmatched node,
    # find its nearest neighborhood within the matched nodes; last, fill out missing county names
    WranglerLogger.info('Running nearest match for nodes that did not get county join')
    node_county_matched_gdf = node_county_gdf.loc[node_county_gdf.NAME.notnull()]
    node_county_unmatched_gdf = node_county_gdf.loc[node_county_gdf.NAME.isnull()]
    WranglerLogger.info('{} unmatched nodes'.format(node_county_unmatched_gdf.shape[0]))

    WranglerLogger.info('...construct k-dimensional points on matched nodes')
    node_county_matched_gdf = node_county_matched_gdf.to_crs(CRS(nearest_match_epsg_str))
    node_county_matched_gdf['X'] = node_county_matched_gdf.geometry.map(lambda g: g.x)
    node_county_matched_gdf['Y'] = node_county_matched_gdf.geometry.map(lambda g: g.y)
    node_matched_inventory_ref = node_county_matched_gdf[['X', 'Y']].values
    node_matched_tree = cKDTree(node_matched_inventory_ref)

    node_county_unmatched_gdf = node_county_unmatched_gdf.to_crs(CRS(nearest_match_epsg_str))
    node_county_unmatched_gdf['X'] = node_county_unmatched_gdf['geometry'].apply(lambda p: p.x)
    node_county_unmatched_gdf['Y'] = node_county_unmatched_gdf['geometry'].apply(lambda p: p.y)

    WranglerLogger.info('...find nearest neighbor for the unmatched nodes')
    node_county_rematch_gdf = pd.DataFrame()

    for i in range(len(node_county_unmatched_gdf)):
        point = node_county_unmatched_gdf.iloc[i][['X', 'Y']].values
        dd, ii = node_matched_tree.query(point, k=1)
        add_snap_gdf = gpd.GeoDataFrame(node_county_matched_gdf.iloc[ii][["NAME"]]).transpose().reset_index(drop=True)

        add_snap_gdf['shst_node_id'] = node_county_unmatched_gdf.iloc[i]['shst_node_id']

        if i == 0:
            node_county_rematch_gdf = add_snap_gdf.copy()
        else:
            node_county_rematch_gdf = node_county_rematch_gdf.append(add_snap_gdf, ignore_index=True, sort=False)
    WranglerLogger.info('found nearest neighbor for {} nodes'.format(node_county_rematch_gdf.shape[0]))

    WranglerLogger.info('...fill out missing county names based on nearest neighbor')
    node_county_rematch_dict = dict(zip(node_county_rematch_gdf.shst_node_id, node_county_rematch_gdf.NAME))
    node_county_gdf["NAME"] = node_county_gdf["NAME"].fillna(node_county_gdf.shst_node_id.map(node_county_rematch_dict))

    # Remove potential duplicated nodes in county match, e.g. geometry on the boundary
    WranglerLogger.info('drop duplicates due to nodes on county boundaries getting more than one math')
    node_county_gdf.drop_duplicates(subset=['shst_node_id'], inplace=True)

    # merge county name into node_gdf
    node_gdf = pd.merge(
        node_gdf,
        node_county_gdf[['shst_node_id', 'NAME']].rename(columns={'NAME': 'county'}),
        how='left',
        on='shst_node_id')

    WranglerLogger.info('Finished assigning county names to nodes. Node counts by county:\n{}'.format(
        node_gdf['county'].value_counts(dropna=False)))

    # spatial join links with county shape based on link centroids
    WranglerLogger.info('Spatially joining shapes with county shape using link centroids')

    # first, create a temporary unique link identifier for convenience in merging; at this point, link_gdf unique
    # link identifier is based on ['fromIntersectionId', 'toIntersectionId', 'shstReferenceId', 'shstGeometryId']
    link_gdf.loc[:, 'temp_link_id'] = range(len(link_gdf))

    # get link centroids
    link_centroid_gdf = link_gdf.copy()
    link_centroid_gdf["geometry"] = link_centroid_gdf["geometry"].centroid

    # spatial join
    link_centroid_gdf_join = gpd.sjoin(link_centroid_gdf, county_gdf, how="left", op="intersects")

    # merge name to link_gdf
    link_county_gdf = pd.merge(
        link_gdf,
        link_centroid_gdf_join[['temp_link_id', 'NAME']],
        how='left',
        on='temp_link_id'
    )
    link_county_unmatched_gdf = link_county_gdf.loc[link_county_gdf.NAME.isnull()]

    WranglerLogger.debug('{} unique links'.format(link_gdf.shape[0]))
    WranglerLogger.debug('{} links in spatial join result, representing {} unique links'.format(
        link_centroid_gdf_join.shape[0],
        link_centroid_gdf_join['temp_link_id'].nunique()))
    WranglerLogger.debug('{} links failed to get a county match through spatial join'.format(
        link_county_unmatched_gdf.shape[0]))

    # use nearest method for links that did not get county match through spatial join
    # using link centroids to find node-based build k-dimensional points
    WranglerLogger.info('Running nearest match for links that did not get county join')

    WranglerLogger.info('...construct k-dimensional points on matched nodes')
    node_county_matched_gdf = node_county_gdf.loc[node_county_gdf.NAME.notnull()]
    node_county_matched_gdf = node_county_matched_gdf.to_crs(CRS(nearest_match_epsg_str))
    node_county_matched_gdf['X'] = node_county_matched_gdf.geometry.map(lambda g: g.x)
    node_county_matched_gdf['Y'] = node_county_matched_gdf.geometry.map(lambda g: g.y)
    node_matched_inventory_ref = node_county_matched_gdf[['X', 'Y']].values
    node_matched_tree = cKDTree(node_matched_inventory_ref)
 
    link_county_unmatched_gdf = link_county_unmatched_gdf.to_crs(CRS(nearest_match_epsg_str))
    link_county_unmatched_gdf["geometry"] = link_county_unmatched_gdf["geometry"].centroid
    link_county_unmatched_gdf['X'] = link_county_unmatched_gdf['geometry'].apply(lambda p: p.x)
    link_county_unmatched_gdf['Y'] = link_county_unmatched_gdf['geometry'].apply(lambda p: p.y)

    WranglerLogger.info('...find nearest neighbor')
    link_county_rematch_gdf = pd.DataFrame()

    for i in range(len(link_county_unmatched_gdf)):
        point = link_county_unmatched_gdf.iloc[i][['X', 'Y']].values
        dd, ii = node_matched_tree.query(point, k=1)
        add_snap_gdf = gpd.GeoDataFrame(node_county_matched_gdf.iloc[ii][["NAME"]]).transpose().reset_index(drop=True)

        add_snap_gdf['temp_link_id'] = link_county_unmatched_gdf.iloc[i]['temp_link_id']

        if i == 0:
            link_county_rematch_gdf = add_snap_gdf.copy()
        else:
            link_county_rematch_gdf = link_county_rematch_gdf.append(add_snap_gdf, ignore_index=True, sort=False)
    WranglerLogger.info('found nearest neighbor for {} links'.format(link_county_rematch_gdf.shape[0]))

    WranglerLogger.info('...fill out missing county names')
    link_county_rematch_dict = dict(zip(link_county_rematch_gdf.temp_link_id, link_county_rematch_gdf.NAME))
    link_county_gdf['NAME'] = link_county_gdf['NAME'].fillna(
        link_county_gdf.temp_link_id.map(link_county_rematch_dict))

    # merge it back to link_gdf
    link_gdf = pd.merge(
        link_gdf,
        link_county_gdf[['temp_link_id', 'NAME']].rename(columns={'NAME': 'county'}),
        how='left',
        on='temp_link_id')
    
    WranglerLogger.info('Finished joining county names to links. Total {} links, counts by county:\n{}'.format(
        link_gdf.shape[0],
        link_gdf['county'].value_counts(dropna=False)))
    
    ####################################
    # Remove links and nodes outside the 9 counties
    WranglerLogger.info('dropping out-of-the-region links')
    # first, remove out-of-region links
    link_BayArea_gdf = link_gdf.loc[link_gdf['county'].isin(BayArea_COUNTIES)]
    WranglerLogger.info('after dropping, {:,} Bay Area links remain'.format(link_BayArea_gdf.shape[0]))
    # then, remove nodes not use by BayArea links
    WranglerLogger.info('dropping nodes not used by Bay Area links')
    node_BayArea_gdf = node_gdf[node_gdf.shst_node_id.isin(link_BayArea_gdf.fromIntersectionId.tolist() +
                                                           link_BayArea_gdf.toIntersectionId.tolist())]
    # for nodes that are outside the Bay Area but used by BayArea links, need to give them the
    # internal county names for node numbering:
    # select these nodes
    node_BayArea_rename_county_gdf = node_BayArea_gdf.loc[~node_BayArea_gdf.county.isin(BayArea_COUNTIES)]
    # get all the nodes (fromIntersectionId, toIntersectionId) used by BayArea links, and their BayArea county names
    node_link_county_names_df = pd.concat(
        [
            link_BayArea_gdf[['fromIntersectionId', 'county']].drop_duplicates().rename(
                columns={'fromIntersectionId': 'shst_node_id'}),
            link_BayArea_gdf[['toIntersectionId', 'county']].drop_duplicates().rename(
                columns={"toIntersectionId": "shst_node_id"})
        ],
        sort=False,
        ignore_index=True
    )
    # then, merge these internal county names to the out-of-county nodes
    node_BayArea_rename_county_gdf = pd.merge(
        node_BayArea_rename_county_gdf.drop(['county'], axis=1),
        node_link_county_names_df[['shst_node_id', 'county']],
        how='left',
        on='shst_node_id'
    )
    # then, drop duplicates
    node_BayArea_rename_county_gdf.drop_duplicates(subset=['osm_node_id', 'shst_node_id'], inplace=True)

    # finally, add these nodes back to node_BayArea_gdf to replace the initial ones with out-of-the-region names
    node_BayArea_gdf = pd.concat(
        [
            node_BayArea_gdf.loc[node_BayArea_gdf.county.isin(BayArea_COUNTIES)],
            node_BayArea_rename_county_gdf
        ],
        sort=False,
        ignore_index=True
    )
    WranglerLogger.info('after dropping, {:,} nodes remain'.format(node_BayArea_gdf.shape[0]))

    ####################################
    # Clean up link_gdf: add link length in meters, drop circular links and circular-link-only nodes, flag drive dead end

    # 1. create link_gdf
    WranglerLogger.info('Adding link length in meters based on geometry')
    # add '_osmnx' suffix to 'length' field from OSMNX
    if 'length' in link_BayArea_gdf.columns:
        link_BayArea_gdf.rename(columns={'length': 'length_osmnx'}, inplace=True)
    
    geom_length = link_BayArea_gdf[['geometry']]
    # convert to EPSG 26915 for meter unit
    geom_length = geom_length.to_crs(CRS(nearest_match_epsg_str))
    # calculate meter length
    geom_length.loc[:, 'length_meter'] = geom_length.length
    # add to link_BayArea_gdf
    link_BayArea_gdf['length_meter'] = geom_length['length_meter']

    # 2. drop circular links (u == v) and circular-link-only nodes
    WranglerLogger.info('Dropping circular links and circular-link-only nodes')
    circular_link_gdf = link_BayArea_gdf.loc[link_BayArea_gdf.u == link_BayArea_gdf.v]

    # TODO: export circular links and map them to verify 

    link_BayArea_gdf = link_BayArea_gdf.loc[
        ~link_BayArea_gdf.shstReferenceId.isin(circular_link_gdf.shstReferenceId.tolist())]
    WranglerLogger.info('after dropping {:,} circular links, {:,} links remain'.format(
        circular_link_gdf.shape[0],
        link_BayArea_gdf.shape[0]))

    # only keep nodes of non-circular links
    node_BayArea_gdf = node_BayArea_gdf.loc[
        (node_BayArea_gdf.osm_node_id.isin(link_BayArea_gdf.u.tolist())) |
        (node_BayArea_gdf.osm_node_id.isin(link_BayArea_gdf.v.tolist()))]
    WranglerLogger.info('after dropping circular-link-only nodes, {:,} nodes remain'.format(node_BayArea_gdf.shape[0]))

    # 3. flag drive dead end, and make dead-end links and nodes drive_access=0
    WranglerLogger.info('Flagging drive dead end, and make dead-end links and nodes drive_access=0')

    # first, iteratively identify dead-end streets
    non_dead_end_link_handle_df = link_BayArea_gdf.loc[(link_BayArea_gdf.drive_access == 1)][["u", "v"]]

    dead_end_node_list = methods.identify_dead_end_nodes(non_dead_end_link_handle_df)

    cumulative_dead_end_node_list = []

    while len(dead_end_node_list) > 0:
        cumulative_dead_end_node_list = cumulative_dead_end_node_list + dead_end_node_list

        non_dead_end_link_handle_df = non_dead_end_link_handle_df[
            ~(non_dead_end_link_handle_df.u.isin(dead_end_node_list)) &
            ~(non_dead_end_link_handle_df.v.isin(dead_end_node_list))].copy()

        dead_end_node_list = methods.identify_dead_end_nodes(non_dead_end_link_handle_df)

    WranglerLogger.info('...found {:,} dead-end nodes'.format(len(cumulative_dead_end_node_list)))

    # TODO: the remaining steps of updating drive access 
    # # second, update node and link drive access
    # # if u/v in dead end node list, then drive access = 0
    # # if osm_node_id in dead end node list, then drive access = 0
    # WranglerLogger.debug('...drive access stats of links: \n{}'.format(link_BayArea_gdf.drive_access.value_counts()))
    # WranglerLogger.info('...make these streets drive_access=0')
    # link_BayArea_gdf['drive_access'] = np.where(
    #     (
    #             (link_BayArea_gdf.u.isin(cumulative_dead_end_node_list)) |
    #             (link_BayArea_gdf.v.isin(cumulative_dead_end_node_list))) &
    #     ~(link_BayArea_gdf.roadway.isin(
    #         ['primary',
    #          'secondary',
    #          'motorway',
    #          'primary_link',
    #          'motorway_link',
    #          'trunk_link',
    #          'trunk',
    #          'secondary_link',
    #          'tertiary_link'])),
    #     0,
    #     link_BayArea_gdf.drive_access)

    # WranglerLogger.info('...after flagging dead end links, drive access stats of links: \n{}'.format(
    #     link_BayArea_gdf.drive_access.value_counts()))

    # WranglerLogger.info('...update network type variable (drive_access) for nodes')
    # WranglerLogger.debug('drive access stats of nodes: \n{}'.format(node_BayArea_gdf.drive_access.value_counts()))

    # # create a dataframe of all links' starting and ending nodes with updated drive_access
    # A_B_df = pd.concat(
    #     [
    #         link_BayArea_gdf[[
    #             "u", "fromIntersectionId", "drive_access", "walk_access", "bike_access"]].rename(
    #             columns={"u": "osm_node_id", "fromIntersectionId": "shst_node_id"}),
    #         link_BayArea_gdf[["v", "toIntersectionId", "drive_access", "walk_access", "bike_access"]].rename(
    #             columns={"v": "osm_node_id", "toIntersectionId": "shst_node_id"})
    #     ],
    #     sort=False,
    #     ignore_index=True)

    # A_B_df.drop_duplicates(inplace=True)
    # A_B_df = A_B_df.groupby(["osm_node_id", "shst_node_id"]).max().reset_index()

    # # merge the updated access data into nodes
    # node_BayArea_gdf = pd.merge(node_BayArea_gdf.drop(["drive_access", "walk_access", "bike_access"], axis=1),
    #                             A_B_df,
    #                             how="left",
    #                             on=["osm_node_id", "shst_node_id"])

    # WranglerLogger.debug('after flagging dead end links, drive access stats of nodes: \n{}'.format(
    #     node_BayArea_gdf.drive_access.value_counts()))

    # # check: there should be no link that is not accessible by all modes
    # no_access_link = link_BayArea_gdf[
    #         (link_BayArea_gdf.drive_access == 0) & (link_BayArea_gdf.walk_access == 0) & (
    #                 link_BayArea_gdf.bike_access == 0)].shape[0]
    # WranglerLogger.debug('check link not accessible by any of drive, bike, walk - there should be none: \n{}'.format(
    #     no_access_link
    # ))
    # assert no_access_link.shape[0] == 0


    # # double check: roadway types versus drive_access

    # WranglerLogger.debug('roadway types: \n{}\n'.format(link_df.roadway.unique()))

    # WranglerLogger.debug('roadway types of link with drive_access==0: \n{}\n'.format(
    #     link_BayArea_gdf[(link_BayArea_gdf.drive_access == 0)].roadway.value_counts()))

    # TODO: more cleaning up 4. Drop duplicated links between same AB node pair

    # get count of unique links of each u/v pair
    non_unique_AB_links_df = link_BayArea_gdf.groupby(["u", "v"]).shstReferenceId.count().sort_values().reset_index()
    WranglerLogger.debug(non_unique_AB_links_df)


    WranglerLogger.debug('links has {} unique shstReferenceId, {} unique u/v pairs'.format(link_BayArea_gdf.shstReferenceId.nunique(),
                                                                            non_unique_AB_links_df.shape[0]))

    # u/v pairs with multiple links
    non_unique_AB_links_df = non_unique_AB_links_df[non_unique_AB_links_df.shstReferenceId > 1]
    WranglerLogger.debug('{} u/v pairs have multiple links')

    # get their link attributes
    non_unique_AB_links_df = pd.merge(non_unique_AB_links_df[["u", "v"]],
                                    link_BayArea_gdf[["u", "v", "highway", "roadway",
                                                        "drive_access", "bike_access", "walk_access", "length",
                                                        "wayId", "shstGeometryId", "shstReferenceId", "geometry"]],
                                    how="left",
                                    on=["u", "v"])

    # read roadway hierarchy crosswalk
    roadway_hierarchy_df = pd.read_csv("../../data/interim/highway_to_roadway.csv")

    roadway_hierarchy_df = roadway_hierarchy_df.drop_duplicates(subset="roadway")

    # merge roadway hierarchy to u/v pairs with multiple links
    non_unique_AB_links_df = pd.merge(non_unique_AB_links_df,
                                    roadway_hierarchy_df[["roadway", "hierarchy"]],
                                    how="left",
                                    on="roadway")

    # sort on hierarchy (ascending), drive_access(descending), bike_access(descending), walk_access(descending), length(ascending)

    non_unique_AB_links_sorted_df = non_unique_AB_links_df.sort_values(
        by=["hierarchy", "drive_access", "bike_access", "walk_access", "length"],
        ascending=[True, False, False, False, True])

    # keep only one link for each u/v pair 
    unique_AB_links_df = non_unique_AB_links_sorted_df.drop_duplicates(subset=["u", "v"], keep="first")

    # select links that should be dropped
    from_list = non_unique_AB_links_df.shstReferenceId.tolist()
    to_list = unique_AB_links_df.shstReferenceId.tolist()

    drop_link_model_link_id_list = [c for c in from_list if c not in to_list]

    # drop the links
    link_BayArea_gdf = link_BayArea_gdf[~ link_BayArea_gdf.shstReferenceId.isin(drop_link_model_link_id_list)]

    # TODO: add model_link_id and model_node_id - do we need this here?
    # # Numbering Nodes

    # # number ranges for nodes by county
    # county_node_numbering_start_dict = {
    #     "San Francisco": 1000000,
    #     "San Mateo": 1500000,
    #     "Santa Clara": 2000000,
    #     "Alameda": 2500000,
    #     "Contra Costa": 3000000,
    #     "Solano": 3500000,
    #     "Napa": 4000000,
    #     "Sonoma": 4500000,
    #     "Marin": 5000000
    # }

    # # create model_mode_id by county
    # node_BayArea_gdf["model_node_id"] = node_BayArea_gdf.groupby(["county"]).cumcount()
    # node_BayArea_gdf["county_numbering_start"] = node_BayArea_gdf["county"].map(county_node_numbering_start_dict)
    # node_BayArea_gdf["model_node_id"] = node_BayArea_gdf["model_node_id"] + node_BayArea_gdf["county_numbering_start"]

    # node_BayArea_gdf.county.value_counts(dropna=False)

    # # check consistency
    # WranglerLogger.debug('{} unique model_node_id, {} nodes have county tagging'.format(
    #     node_BayArea_gdf.model_node_id.nunique(),
    #     node_BayArea_gdf[node_BayArea_gdf.county.isin(county_node_numbering_start_dict.keys())].shape[0]))

    # # # Numbering Links

    # # number ranges for links by county
    # county_link_numbering_start_dict = {
    #     "San Francisco": 1,
    #     "San Mateo": 1000000,
    #     "Santa Clara": 2000000,
    #     "Alameda": 3000000,
    #     "Contra Costa": 4000000,
    #     "Solano": 5000000,
    #     "Napa": 6000000,
    #     "Sonoma": 7000000,
    #     "Marin": 8000000
    # }

    # # create model_link_id by county
    # link_BayArea_gdf["model_link_id"] = link_BayArea_gdf.groupby(["county"]).cumcount()
    # link_BayArea_gdf["county_numbering_start"] = link_BayArea_gdf["county"].map(county_link_numbering_start_dict)
    # link_BayArea_gdf["model_link_id"] = link_BayArea_gdf["model_link_id"] + link_BayArea_gdf["county_numbering_start"]

    # link_BayArea_gdf.county.value_counts(dropna=False)

    # # check consistency
    # WranglerLogger.debug('{} unique model_link_id, {} links have county tagging'.format(
    #     link_BayArea_gdf.model_link_id.nunique(),
    #     link_BayArea_gdf[link_BayArea_gdf.county.isin(county_link_numbering_start_dict.keys())].shape[0]))

    # # # Numbering Link A/B nodes

    # # shst_node_id - model_node_id dictionary 
    # node_shst_model_id_dict = dict(zip(node_BayArea_gdf.shst_node_id, node_BayArea_gdf.model_node_id))

    # # map shst_node_id to model_node_id as A/B
    # link_BayArea_gdf["A"] = link_BayArea_gdf["fromIntersectionId"].map(node_shst_model_id_dict)
    # link_BayArea_gdf["B"] = link_BayArea_gdf["toIntersectionId"].map(node_shst_model_id_dict)

    # # check: all links should have A and B
    # WranglerLogger.debug(link_BayArea_gdf[link_BayArea_gdf.A.isnull()].county.value_counts())
    # WranglerLogger.debug(link_BayArea_gdf[link_BayArea_gdf.B.isnull()].county.value_counts())

    #####################################
    # export link, node

    WranglerLogger.info('Final network links have the following fields:\n{}'.format(link_BayArea_gdf.dtypes))
    WranglerLogger.info('Final network nodes have the following fields:\n{}'.format(node_BayArea_gdf.dtypes))

    WranglerLogger.info('Saving links to {}'.format(LINK_OUTPUT))
    geofeather.to_geofeather(link_BayArea_gdf, LINK_OUTPUT)

    WranglerLogger.info('Saving nodes to {}'.format(NODE_OUTPUT))
    geofeather.to_geofeather(node_BayArea_gdf, NODE_OUTPUT)

    WranglerLogger.info('Done')
