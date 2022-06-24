USAGE = """
Build transit network from GTFS data to network standard, through the following steps:
    1. extract representative trips
    2. snap stops to roadway nodes
    3. route bus on roadway via osmnx routing
    4. route bus on roadway via shst routing
    5. build non-bus/rail links and nodes
    6. complete network node list that each transit path traverses
    7. frequence based stop time
    8. write out to transit network standard
    9. write out quick QA/QC transit route true shape
    10. write out network standard with rail nodes and links
    11. write out travel model transit network

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: 
    - 

Outputs:  
    -
"""
import methods
import geofeather
import os, datetime
import pandas as pd
import geopandas as gpd
import numpy as np
import osmnx as ox
# import partridge as ptg
# 
# #%matplotlib inline
# import requests
# 
# 
# 

# from shapely.geometry import Point, LineString
# 
# from shapely import wkt
# from scipy.spatial import cKDTree
# 
# from dbfread import DBF
# from osgeo import ogr
# import glob
# import time
# import json


from network_wrangler import WranglerLogger, setupLogging

#####################################
# inputs and outputs

INPUT_DATA_DIR = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# base standard network data
ROADWAY_NETWORK_DIR = os.path.join(INPUT_DATA_DIR, 'step3_join_shst_with_osm')
ROADWAY_LINK_FILE = os.path.join(ROADWAY_NETWORK_DIR, 'step3_link.feather')
ROADWAY_NODE_FILE = os.path.join(ROADWAY_NETWORK_DIR, 'step3_node.feather')
# GTFS raw data
GTFS_DATA_DIR = os.path.join(INPUT_DATA_DIR, 'step6_GTFS')
GTFS_INPUT_DIR  = os.path.join(GTFS_DATA_DIR, '2015_input')
# gtfs shape to sharedstreet conflation result
CONFLATION_SHST = os.path.join(GTFS_DATA_DIR, 'conflation_shst')

# output
GTFS_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, 'step6_gtfs')
GTFS_CONSOLIDATED_DIR = os.path.join(GTFS_OUTPUT_DIR, 'consolidated_gtfs')

# # todo: describe what goes in here
# output_data_version_dir = os.path.join(output_data_interim_dir,'version_12')
# os.makedirs(output_data_version_dir, exist_ok=True)

if __name__ == '__main__':
    # create output directories if not exist
    if not os.path.exists(GTFS_CONSOLIDATED_DIR):
        WranglerLogger.info('create output folder: {}'.format(GTFS_CONSOLIDATED_DIR))
        os.makedirs(GTFS_CONSOLIDATED_DIR)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        GTFS_CONSOLIDATED_DIR,
        "step6b_build_transit_network_from_gtfs_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    ####################################
    # Consolidate all gtfs into one
    gtfs_raw_name_ls = os.listdir(GTFS_INPUT_DIR)
    # remove data not needed
    for gtfs_feed_name in gtfs_raw_name_ls:
        if gtfs_feed_name not in methods.gtfs_name_dict:
            WranglerLogger.debug('skipping GTFS dataset {} for {}'.format(
                gtfs_feed_name, methods.gtfs_name_dict[gtfs_feed_name]))
            gtfs_raw_name_ls.remove(gtfs_feed_name)
    WranglerLogger.info('Consolidating the following GTFS data: {}'.format(gtfs_raw_name_ls))

    # A typical GTFS dataset contains multiple .txt files with information on agency, schedule, routes, stops, trips, fares.
    # The following .txt file will be consolidated:
    # - calendar.txt
    # - agency.txt
    #      * agency_raw_name: same as the gtfs data folder name
    # - routes.txt
    # - trips.txt
    # - stops.txt
    # - shapes.txt
    # - stop_times.txt
    # - fare_attributes.txt
    # - fare_rules.txt

    (all_routes_df, all_trips_df, all_stops_df, all_shapes_df, all_stop_times_df, 
     all_agency_df, all_fare_attributes_df, all_fare_rules_df) = methods.consolidate_all_gtfs(GTFS_INPUT_DIR, gtfs_raw_name_ls)

    WranglerLogger.info('all_routes_df has fields:\n{},\n header:\n{}'.format(all_routes_df.dtypes, all_routes_df.head()))
    WranglerLogger.info('all_trips_df has fields:\n{},\n header:\n{}'.format(all_trips_df.dtypes, all_trips_df.head()))
    WranglerLogger.info('all_stops_df has fields:\n{},\n header:\n{}'.format(all_stops_df.dtypes, all_stops_df.head()))
    WranglerLogger.info('all_shapes_df has fields:\n{},\n header:\n{}'.format(all_shapes_df.dtypes, all_shapes_df.head()))
    WranglerLogger.info('all_stop_times_df has fields:\n{},\n header:\n{}'.format(all_stop_times_df.dtypes, all_stop_times_df.head()))
    WranglerLogger.info('all_agency_df has fields:\n{},\n header:\n{}'.format(all_agency_df.dtypes, all_agency_df.head()))
    WranglerLogger.info('all_fare_attributes_df has fields:\n{},\n header:\n{}'.format(all_fare_attributes_df.dtypes, all_fare_attributes_df.head()))
    WranglerLogger.info('all_fare_rules_df has fields:\n{},\n header:\n{}'.format(all_fare_rules_df.dtypes, all_fare_rules_df.head()))

    # clean up field names in all_fare_rules
    all_fare_rules_df["origin_id"] = np.where(all_fare_rules_df["origin_id"].isnull(),
                                            all_fare_rules_df[" origin_id"],
                                            all_fare_rules_df["origin_id"])
    all_fare_rules_df["destination_id"] = np.where(all_fare_rules_df["destination_id"].isnull(),
                                            all_fare_rules_df[" destination_id"],
                                            all_fare_rules_df["destination_id"])
    all_fare_rules_df["contains_id"] = np.where(all_fare_rules_df["contains_id"].isnull(),
                                            all_fare_rules_df[" contains_id"],
                                            all_fare_rules_df["contains_id"])
    all_fare_rules_df.drop(columns = [" origin_id", " destination_id", " contains_id"], inplace = True)

    # examine transit agencies included
    WranglerLogger.debug('examine agency_raw_name, agency_name, agency_id: \n{}'.format(
        all_agency_df.astype(str).groupby(["agency_raw_name", "agency_name", "agency_id"]).count().reset_index()[[
            "agency_raw_name", "agency_name", "agency_id"]]))

    # modify some 'agency_raw_name', 'agency_name', and 'agency_id'
    for df in [all_routes_df, all_trips_df, all_stops_df, all_shapes_df, 
               all_stop_times_df, all_agency_df, all_fare_attributes_df, all_fare_rules_df]:

        # in the GTFS data, "Sonoma County Transit (id 175)" and "Cloverdale Transit (id 183)" are from the same
        # GTFS feed with the same agency_raw_name "SonomaCounty_2015_8_18". This is also the case in the Cube fare files
        # so, update the lookup to keep "Conoma County Transit" only
        if 'agency_name' in list(df):
            WranglerLogger.debug('recode agency_name "Cloverdale Transit" as "Sonoma County Transit"')
            df.loc[df.agency_name == 'Cloverdale Transit', 'agency_name'] = 'Sonoma County Transit'
        if 'agency_id' in list(df):
            WranglerLogger.debug('recode agency_name 183 (Cloverdale Transit) as 175 (Sonoma County Transit)')
            all_agency_df.loc[all_agency_df.agency_id == '183', 'agency_id'] = '175'
            all_fare_attributes_df.loc[all_fare_attributes_df.agency_id == 183, 'agency_id'] = 175
    
        # to enable sharedstreets conflation, agency_raw_name 'Blue&Gold_gtfs_10_4_2017' and agency_name 'Blue&Gold Fleet' have been
        # modified to 'Blue_Gold_gtfs_10_4_2017' and 'Blue Gold Fleet' respectively; 
        # agency_raw_name 'Union_City_Transit_Aug-01-2015 to Jun-30-2017' has been modified to 'Union_City_Transit_Aug-01-2015_to_Jun-30-2017'.
        if 'agency_name' in list(df):
            WranglerLogger.debug('recode agency_name "Blue&Gold Fleet" as "Blue Gold Fleet"')
            df.loc[df.agency_name == 'Blue&Gold Fleet', 'agency_name'] = 'Blue Gold Fleet'
        if 'agency_raw_name' in list(df):
            WranglerLogger.debug('recode agency_raw_name "Blue&Gold_gtfs_10_4_2017" as "Blue_Gold_gtfs_10_4_2017"')
            df.loc[df.agency_raw_name == 'Blue&Gold_gtfs_10_4_2017', 'agency_raw_name'] = 'Blue_Gold_gtfs_10_4_2017'
            WranglerLogger.debug('recode agency_raw_name "Blue&Gold_gtfs_10_4_2017" as "Blue_Gold_gtfs_10_4_2017"')
            df.loc[df.agency_raw_name == 'Union_City_Transit_Aug-01-2015 to Jun-30-2017', 'agency_raw_name'] = 'Union_City_Transit_Aug-01-2015_to_Jun-30-2017'

    # Re-ID the consolidated gtfs, creating unique route_id, shape_id, trip_id, stop_id across agencies
    WranglerLogger.info('creating unique route_id, shape_id, trip_id, stop_id for consolidated GTFS data')

    unique_route_id_df = all_routes_df.loc[:, ['agency_raw_name', 'route_id']].drop_duplicates()
    # rename the initial route_id to route_id_original and create new route_id
    unique_route_id_df["route_id_original"] = unique_route_id_df["route_id"]
    unique_route_id_df["route_id"] = range(1,  len(unique_route_id_df) + 1)

    unique_trip_id_df = all_trips_df.loc[:, ['agency_raw_name', 'trip_id']].drop_duplicates()
    unique_trip_id_df["trip_id_original"] = unique_trip_id_df["trip_id"]
    unique_trip_id_df["trip_id"] = range(1,  len(unique_trip_id_df) + 1)

    unique_shape_id_df = all_trips_df.loc[:, ['agency_raw_name', 'shape_id']].drop_duplicates()
    unique_shape_id_df["shape_id_original"] = unique_shape_id_df["shape_id"]
    unique_shape_id_df["shape_id"] = range(1,  len(unique_shape_id_df) + 1)

    unique_stop_id_df = all_stops_df.loc[:, ['agency_raw_name', 'stop_id']].drop_duplicates()
    unique_stop_id_df["stop_id_original"] = unique_stop_id_df["stop_id"]
    unique_stop_id_df["stop_id"] = range(1,  len(unique_stop_id_df) + 1)

    # merge unique shape id and stop id back to aggregated gtfs data
    # first, rename the original ID fields
    all_routes_df.rename(    columns = {"route_id": "route_id_original"}, inplace=True)
    all_trips_df.rename(     columns = {"route_id": "route_id_original",
                                        "trip_id" : "trip_id_original",
                                        "shape_id": "shape_id_original"}, inplace=True)
    all_stops_df.rename(     columns = {"stop_id" : "stop_id_original"}, inplace=True)
    all_shapes_df.rename(    columns = {"shape_id": "shape_id_original"}, inplace=True)
    all_stop_times_df.rename(columns = {"trip_id" : "trip_id_original",
                                        "stop_id" : "stop_id_original"}, inplace=True)
    all_fare_rules_df.rename(columns = {"route_id": "route_id_original"}, inplace=True)
    # all_fare_rules_df["route_id_original"] = all_fare_rules_df["route_id_original"].astype(str)

    # then, add the unique IDs
    all_routes_df = all_routes_df.merge(unique_route_id_df,
                                        how = "left",
                                        on = ["agency_raw_name", "route_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_routes_df has new route_id, max route_id is {}'.format(
        all_routes_df.loc[all_routes_df['route_id'].notnull()].shape[0],
        all_routes_df.shape[0],
        all_routes_df['route_id'].max()
    ))

    all_trips_df = all_trips_df.merge(unique_route_id_df,
                                      how = "left",
                                      on = ["agency_raw_name",
                                            "route_id_original"]).merge(unique_trip_id_df,
                                                                        how = "left",
                                                                        on = ["agency_raw_name",
                                                                              "trip_id_original"]).merge(unique_shape_id_df,
                                                                                                         how = "left",
                                                                                                         on = ["agency_raw_name",
                                                                                                               "shape_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_trips_df has new route_id, max route_id is {};\
     {} rows has new trip_id, max trip_id is {}; {} rows has new shape_id, max shape_id is {}'.format(
        all_trips_df.loc[all_trips_df['route_id'].notnull()].shape[0],
        all_trips_df.shape[0],
        all_trips_df['route_id'].max(),
        all_trips_df.loc[all_trips_df['trip_id'].notnull()].shape[0],
        all_trips_df['trip_id'].max(),
        all_trips_df.loc[all_trips_df['shape_id'].notnull()].shape[0],
        all_trips_df['shape_id'].max()
    ))

    all_stops_df = all_stops_df.merge(unique_stop_id_df,
                                      how = "left",
                                      on = ["agency_raw_name", "stop_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_stops_df has new stop_id, max stop_id is {}'.format(
        all_stops_df.loc[all_stops_df['stop_id'].notnull()].shape[0],
        all_stops_df.shape[0],
        all_stops_df['stop_id'].max()
    ))

    all_shapes_df = all_shapes_df.merge(unique_shape_id_df,
                                        how = "left",
                                        on = ["agency_raw_name", "shape_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_shapes_df has new shape_id, max shape_id is {}'.format(
        all_shapes_df.loc[all_shapes_df['shape_id'].notnull()].shape[0],
        all_shapes_df.shape[0],
        all_shapes_df['shape_id'].max()
    ))

    all_stop_times_df = all_stop_times_df.merge(unique_trip_id_df,
                                                how = "left",
                                                on = ["agency_raw_name",
                                                      "trip_id_original"]).merge(unique_stop_id_df,
                                                                                 how = "left",
                                                                                 on = ["agency_raw_name", "stop_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_stop_times_df has new trip_id, max trip_id is {};\
     {} rows has new stop_id, max stop_id is {}'.format(
        all_stop_times_df.loc[all_stop_times_df['trip_id'].notnull()].shape[0],
        all_stop_times_df.shape[0],
        all_stop_times_df['trip_id'].max(),
        all_stop_times_df.loc[all_stop_times_df['stop_id'].notnull()].shape[0],
        all_stop_times_df['stop_id'].max()
    ))

    all_fare_rules_df = all_fare_rules_df.merge(unique_route_id_df,
                                                how = "left",
                                                on = ["agency_raw_name", "route_id_original"])
    WranglerLogger.debug('{} out of {} rows in all_fare_rules_df has new route_id, max route_id is {}'.format(
        all_fare_rules_df.loc[all_fare_rules_df['route_id'].notnull()].shape[0],
        all_fare_rules_df.shape[0],
        all_fare_rules_df['route_id'].max()
    ))
    
    # examine fare_rules rows without 'route_id' - some of them have zonal fares ('origin_id' and 'destination_id' not null) 
    all_fare_rules_no_route_id_df = all_fare_rules_df.loc[all_fare_rules_df['route_id'].isnull()]
    
    zonal_fare_df = all_fare_rules_no_route_id_df.loc[all_fare_rules_no_route_id_df['origin_id'].notnull() & \
                                                      all_fare_rules_no_route_id_df['destination_id'].notnull()]
    WranglerLogger.debug('{} rows of all_fare_rules_df missing new route_id, but have origin_id and destination_id, \
    including agencies: {}'.format(
        zonal_fare_df.shape[0],
        zonal_fare_df['agency_raw_name'].unique()
    ))

    fare_rules_missing_route_id_df = all_fare_rules_no_route_id_df.loc[all_fare_rules_no_route_id_df['origin_id'].isnull() & \
                                                                       all_fare_rules_no_route_id_df['destination_id'].isnull()]
    WranglerLogger.debug('{} rows of all_fare_rules_df missing new route_id, origin_id, destination_id, \
    including the following agencies: {}'.format(
        fare_rules_missing_route_id_df.shape[0],
        fare_rules_missing_route_id_df['agency_raw_name'].unique()
    ))

    # export
    ALL_ROUTES_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'routes.txt')
    WranglerLogger.info('export all_routes_df to {} with fields: \n{}'.format(ALL_ROUTES_FILE, all_routes_df.dtypes))
    all_routes_df.to_csv(ALL_ROUTES_FILE, index = False, sep = ',')

    ALL_TRIPS_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'trips.txt')
    WranglerLogger.info('export all_trips_df to {} with fields: \n{}'.format(ALL_TRIPS_FILE, all_trips_df.dtypes))
    all_trips_df.to_csv(ALL_TRIPS_FILE, index = False, sep = ',')

    ALL_STOPS_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'stops.txt')
    WranglerLogger.info('export all_stops_df to {} with fields: \n{}'.format(ALL_STOPS_FILE, all_stops_df.dtypes))
    all_stops_df.to_csv(ALL_STOPS_FILE, index = False, sep = ',')

    ALL_SHAPES_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'shapes.txt')
    WranglerLogger.info('export all_shapes_df to {} with fields: \n{}'.format(ALL_SHAPES_FILE, all_shapes_df.dtypes))
    all_shapes_df.to_csv(ALL_SHAPES_FILE, index = False, sep = ',')

    ALL_STOP_TIMES_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'stop_times.txt')
    WranglerLogger.info('export all_stop_times_df to {} with fields: \n{}'.format(ALL_STOP_TIMES_FILE, all_stop_times_df.dtypes))
    all_stop_times_df.to_csv(ALL_STOP_TIMES_FILE, index = False, sep = ',')

    ALL_AGENCY_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'agency.txt')
    WranglerLogger.info('export all_agency_df to {} with fields: \n{}'.format(ALL_AGENCY_FILE, all_agency_df.dtypes))
    all_agency_df.to_csv(ALL_AGENCY_FILE, index = False, sep = ',')

    ALL_FARE_ATTRIBUTES_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'fare_attributes.txt')
    WranglerLogger.info('export all_fare_attributes_df to {} with fields: \n{}'.format(ALL_FARE_ATTRIBUTES_FILE, all_fare_attributes_df.dtypes))
    all_fare_attributes_df.to_csv(ALL_FARE_ATTRIBUTES_FILE, index = False, sep = ',')

    ALL_FARE_RULES_FILE = os.path.join(GTFS_CONSOLIDATED_DIR,'fare_rules.txt')
    WranglerLogger.info('export all_fare_rules_df to {} with fields: \n{}'.format(ALL_FARE_RULES_FILE, all_fare_rules_df.dtypes))
    all_fare_rules_df.to_csv(ALL_FARE_RULES_FILE, index = False, sep = ',')

    # drop fields not needed
    all_trips_df.drop(["route_id_original", "trip_id_original", "shape_id_original"], axis = 1, inplace = True)
    all_stops_df.drop(["stop_id_original"], axis = 1, inplace = True)
    all_shapes_df.drop(["agency_raw_name", "shape_id_original"], axis = 1, inplace = True)
    all_stop_times_df.drop(["agency_raw_name", "trip_id_original", "stop_id_original"], axis = 1, inplace = True)


    ####################################
    # extract representative trip for each route by direction and time-of-day
    WranglerLogger.info('Getting representative trip for each route by direction and time-of-day')
    trip_df = methods.get_representative_trip_for_route(all_trips_df, all_stop_times_df)


    ####################################
    # map gtfs stops to roadway nodes

    WranglerLogger.info('Snapping GFTS stops to roadway nodes')

    # first, get candidate nodes to for snapping transit stops to
    WranglerLogger.debug('load roadway links from {} and nodes from {}'.format(ROADWAY_LINK_FILE, ROADWAY_NODE_FILE))
    link_gdf = geofeather.from_geofeather(ROADWAY_LINK_FILE)
    node_gdf = geofeather.from_geofeather(ROADWAY_NODE_FILE)

    drive_link_gdf = link_gdf[link_gdf.drive_access == True]
    drive_node_gdf = node_gdf[node_gdf.drive_access == True]
    WranglerLogger.debug('{:,} out of {:,} links have drive_access==True'.format(drive_link_gdf.shape[0], link_gdf.shape[0]))
    WranglerLogger.debug('{:,} out of {:,} nodes have drive_access==True'.format(drive_node_gdf.shape[0], node_gdf.shape[0]))

    # only keep non-motorway nodes: motorway and motorway are restricted-access freeway and freeway ramps, 
    # shouldn't have transit stops
    # also exclude links with roadway=='unknown' highway=='traffic_island'. See discussion: https://app.asana.com/0/0/1202470211319796/1202491781241679/f 
    non_motorway_links_gdf = drive_link_gdf.loc[~drive_link_gdf.roadway.isin(["motorway", "motorway_link", 'unknown'])]

    node_candidates_for_stops_df = drive_node_gdf.loc[
        drive_node_gdf.shst_node_id.isin(
            non_motorway_links_gdf.fromIntersectionId.tolist() + non_motorway_links_gdf.toIntersectionId.tolist())]
    WranglerLogger.debug('{:,} out of {:,} drive_nodes are candidates to snap transit stops to'.format(
        drive_node_gdf.shape[0],
        node_candidates_for_stops_df.shape[0]
    ))

    stop_gdf = methods.snap_stop_to_node(all_stops_df, node_candidates_for_stops_df)


    ####################################
    # routing

    ## Approach 1: osmnx routing
    # build network routing file for osmnx routing
    G_drive = methods.v12_ox_graph(drive_node_gdf,
                                   drive_link_gdf)

    # route bus using osmnx method, based on representative trips and stops already snapped to roadway drive nodes
    bus_osmnx_link_shape_df, bus_osmnx_broken_trip_list = methods.v12_route_bus_link_osmnx(drive_link_gdf, 
                                                                                           drive_node_gdf,
                                                                                           G_drive, 
                                                                                           all_stop_times_df,
                                                                                           all_routes_df,
                                                                                           trip_df, 
                                                                                           stop_gdf)

    # trips successfully routed by osmnx
    WranglerLogger.info('finished routing bus by osmnx method:\
    all trips contain {:,} unique shape_id, routed trips contain {:,} unique shape_id'.format(
        trip_df.shape_id.nunique(),
        bus_osmnx_link_shape_df.shape_id.nunique()))
    WranglerLogger.debug('dataframe of drive links where bus trips traverse has the following fields:\n{}'.format(
        bus_osmnx_link_shape_df.dtypes
    ))
    WranglerLogger.info(
        'osmnx method failed to route these trips (can be rail modes): {}, containing these shapes: {}'.format(
            bus_osmnx_broken_trip_list,
            trip_df[trip_df.trip_id.isin(bus_osmnx_broken_trip_list)].shape_id.unique()))

    ## Approach 2: shst routing
    # read transit to shst matching result
    shape_shst_matched_df = pd.DataFrame()
    for filename in os.listdir(CONFLATION_SHST):
        if ('_matched.feather' in filename) & ('_matched.feather.crs' not in filename):
            agency_raw_name = filename.split('_matched')[0]
            WranglerLogger.info('read shst matching result for {}'.format(agency_raw_name))
            shst_matched = geofeather.from_geofeather(os.path.join(CONFLATION_SHST, filename))
            # add agency_raw_name
            shst_matched['agency_raw_name'] = agency_raw_name

            shape_shst_matched_df = pd.concat([shape_shst_matched_df, shst_matched], sort = False, ignore_index = True)
    
    # rename 'shape_id' to 'shape_id_original'
    shape_shst_matched_df.rename(columns = {'shape_id': 'shape_id_original'}, inplace=True)
    
    # add other shape attributes
    shape_shst_matched_df = shape_shst_matched_df.merge(unique_shape_id_df,
                                                        on = ['agency_raw_name', 'shape_id_original'],
                                                        how = 'left')

    # since peertree "pt.get_representative_feed" method only keeps the feed for the busiest day, some shape_id
    # won't be in 'unique_shape_id_df', therefore 'shape_shst_matched_df' has shape_id.isnull(). Drop those.
    shape_shst_matched_df = shape_shst_matched_df.loc[shape_shst_matched_df.shape_id.notnull()]

    # route bus using shst routing method
    bus_shst_link_shape_df, incomplete_shape_list = methods.v12_route_bus_link_shst(drive_link_gdf, shape_shst_matched_df)

    WranglerLogger.info('total {} bus_shst links with {} shapes; {} links with {} shapes were successfully routed'.format(
        bus_shst_link_shape_df.shape[0],
        bus_shst_link_shape_df.shape_id.nunique(),
        bus_shst_link_shape_df.shape[0],
        bus_shst_link_shape_df.shape_id.nunique()))
