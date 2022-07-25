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
from operator import index
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
TRANSIT_ROUTING_QAQC_DIR = os.path.join(GTFS_OUTPUT_DIR, 'transit_routing')

# # todo: describe what goes in here
# output_data_version_dir = os.path.join(output_data_interim_dir,'version_12')
# os.makedirs(output_data_version_dir, exist_ok=True)

if __name__ == '__main__':
    # create output directories if not exist
    for output_dir in [GTFS_CONSOLIDATED_DIR, TRANSIT_ROUTING_QAQC_DIR]:
        if not os.path.exists(output_dir):
            WranglerLogger.info('create output folder: {}'.format(output_dir))
            os.makedirs(output_dir)

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        GTFS_OUTPUT_DIR,
        "step6b_build_transit_network_from_gtfs_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    ####################################
    # Consolidate all gtfs into one
    gtfs_raw_name_ls = os.listdir(GTFS_INPUT_DIR)
    # remove data not needed
    for gtfs_feed_name in gtfs_raw_name_ls:
        if gtfs_feed_name not in methods.gtfs_name_dict:
            WranglerLogger.debug('skipping GTFS dataset {}'.format(gtfs_feed_name))
            gtfs_raw_name_ls.remove(gtfs_feed_name)
    # TODO: also remove GGFerries - why?
    WranglerLogger.debug('skipping GTFS dataset GGFerries_2017_3_18')
    gtfs_raw_name_ls.remove('GGFerries_2017_3_18')
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
    # TODO: verify these are indeed operators with zonal fare systems
    WranglerLogger.debug('{} rows of all_fare_rules_df missing new route_id, but have origin_id and destination_id, \
    including agencies: {}'.format(
        zonal_fare_df.shape[0],
        zonal_fare_df['agency_raw_name'].unique()
    ))

    fare_rules_missing_route_id_df = all_fare_rules_no_route_id_df.loc[all_fare_rules_no_route_id_df['origin_id'].isnull() & \
                                                                       all_fare_rules_no_route_id_df['destination_id'].isnull()]
    # TODO: decide if need to clean up missing values
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

    # # drop fields not needed
    # all_trips_df.drop(["route_id_original", "trip_id_original", "shape_id_original"], axis = 1, inplace = True)
    # all_stops_df.drop(["stop_id_original"], axis = 1, inplace = True)
    # all_shapes_df.drop(["agency_raw_name", "shape_id_original"], axis = 1, inplace = True)
    # all_stop_times_df.drop(["agency_raw_name", "trip_id_original", "stop_id_original"], axis = 1, inplace = True)

    ####################################
    # extract representative trip for each route by direction and time-of-day
    WranglerLogger.info('Getting representative trip for each route by direction and time-of-day')
    representative_trip_df = methods.get_representative_trip_for_route(all_trips_df,
                                                        all_stop_times_df,
                                                        methods.TIME_OF_DAY_DICT)
    WranglerLogger.info('representative_trip_df has fields: \n{}'.format(representative_trip_df.dtypes))


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
        node_candidates_for_stops_df.shape[0],
        drive_node_gdf.shape[0]
    ))

    stop_gdf = methods.snap_stop_to_node(all_stops_df, node_candidates_for_stops_df)
    WranglerLogger.debug(
        'stop_gdf has {} rows, with columns: \n{}'.format(stop_gdf.shape[0], stop_gdf.dtypes))

    ####################################
    # route buses - V12 methods
    # 1. osmnx routing using osmnx network graph
    # 2. shst routing based on sharedstreest match
    # 3. combine the two results, prioritizing shst routing

    # 1: bus osmnx routing, based on representative trips and stops already snapped to roadway drive nodes
    WranglerLogger.info('Routing bus using osmnx method')
    bus_osmnx_link_shape_df, bus_osmnx_broken_trip_list = methods.v12_route_bus_osmnx_graph(drive_link_gdf,
                                                                                            drive_node_gdf,
                                                                                            all_stop_times_df,
                                                                                            all_routes_df,
                                                                                            representative_trip_df, 
                                                                                            stop_gdf)

    # trips successfully routed by osmnx
    WranglerLogger.info('finished routing bus by osmnx method: \
    a total of {:,} unique shape_id contained by all trips, routed trips contain {:,} unique shape_id'.format(
        representative_trip_df.shape_id.nunique(),
        bus_osmnx_link_shape_df.shape_id.nunique()))
    WranglerLogger.debug('dataframe of drive links where bus trips traverse has the following fields:\n{}'.format(
        bus_osmnx_link_shape_df.dtypes
    ))
    WranglerLogger.info(
        'osmnx method failed to route these trips (can be rail modes): {}, containing these shapes: {}'.format(
            bus_osmnx_broken_trip_list,
            representative_trip_df.loc[
                representative_trip_df.trip_id.isin(bus_osmnx_broken_trip_list)].shape_id.unique()))
    
    # export for debug
    v12_BUS_OSMNX_ROUTING_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'b12_bus_osmnx_routing.csv')
    WranglerLogger.info('Saving V12 bus osmnx routing result to {}'.format(v12_BUS_OSMNX_ROUTING_QAQC_FILE))
    bus_osmnx_link_shape_df.to_csv(v12_BUS_OSMNX_ROUTING_QAQC_FILE, index=False)

    # 2: bus shst routing, based on gtfs shapes to sharedstreets conflation 
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
    bus_shst_link_shape_df, incomplete_shape_list = methods.v12_route_bus_shst(drive_link_gdf, shape_shst_matched_df)

    WranglerLogger.info('finished routing bus using sharedstreets matching result;\
    total {:,} bus_shst links with {:,} shapes; {:,} links with {:,} shapes were successfully routed'.format(
        bus_shst_link_shape_df.shape[0],
        bus_shst_link_shape_df.shape_id.nunique(),
        bus_shst_link_shape_df.shape[0],
        bus_shst_link_shape_df.shape_id.nunique()))

    WranglerLogger.info(
        'shst method failed to route these shapes: {}'.format(incomplete_shape_list))
    
    # export for debug
    v12_BUS_SHST_ROUTING_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'b12_bus_shst_routing.csv')
    WranglerLogger.info('Saving V12 bus shst routing result to {}'.format(v12_BUS_SHST_ROUTING_QAQC_FILE))
    bus_shst_link_shape_df.to_csv(v12_BUS_SHST_ROUTING_QAQC_FILE, index=False)
    
    # 3: combine routing results of the two approaches
    bus_routed_link_df = methods.v12_route_bus_link_consolidate(bus_osmnx_link_shape_df,
                                                                bus_shst_link_shape_df,
                                                                all_routes_df,
                                                                representative_trip_df,
                                                                incomplete_shape_list)
    WranglerLogger.info('consolidated osmnx routing and shst routing results.\
    Output dataframe bus_routed_link_df has columns:\n{}\n header: \n{}'.format(
        bus_routed_link_df.dtypes,
        bus_routed_link_df.head()
    ))

    # create gdf to visualize and QAQC
    bus_routed_link_gdf = bus_routed_link_df.rename(columns={'u': 'u_busRouting',
                                                             'v': 'v_busRouting'}).merge(link_gdf, 
                                                                                         on=['shstReferenceId', 'shstGeometryId'],
                                                                                         how='left')
    bus_routed_link_gdf = gpd.GeoDataFrame(bus_routed_link_gdf,
                                           geometry='geometry',
                                           crs=link_gdf.crs)
    v12_BUS_ROUTING_ALL_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'v12_bus_routing_all.feather')
    WranglerLogger.info('Saving V12 bus routing solidated results to {}'.format(v12_BUS_ROUTING_ALL_QAQC_FILE))
    geofeather.to_geofeather(bus_routed_link_gdf, v12_BUS_ROUTING_ALL_QAQC_FILE)

    ####################################
    # route buses - Ranch methods
    # 1. osmnx shortest path routing
    # 2. shst routing based on sharedstreest match
    # 3. combine the two routing results
    # 4. update bus stop ? - what does this do?

    # 1. osmnx shortest path routing: iteratively route each shape (represented by one trip), in 3 steps:
    # - routing from the start stop to the end stop of the trip
    # - identifying "bad" stops - whose routed links are all > 50 meters away from the stop
    # - rerouting the "bad" stops, iteratively finding the shortest path between each pair of adjacent "bad" stops

    bus_ranch_shortest_path_routed_link_gdf, bus_ranch_shortest_path_failed_shape_list = \
        methods.ranch_route_bus_shortest_path(
            representative_trip_df,
            stop_gdf,
            all_routes_df,
            all_shapes_df,
            all_stop_times_df,
            drive_link_gdf,
            drive_node_gdf,
            TRANSIT_ROUTING_QAQC_DIR                                                                 
        )
    
    # TODO: compare v12 and Ranch shst routing
    # TODO: compare v12 and Ranch method to combine two routing results
    #  

    ####################################
    # route non-bus (rail and ferry) - V12 methods
    # 1. route rail/ferry based on GTFS shape.txt
    # 2. when shape.txt not available in GTFS (e.g. ACE, CCTA, VINE), route based on stops and stop_times
    # 3. combine the two

    # TODO: v12 code has the following manual correction. I think the shape_id and trip_id will be different
    # when the script is rerun, especially given that shape_id and trip_id are created in this script. Need to 
    # check the routing results and fix it if needed.
    # manual correction for Capitol Corridor: the shape_id from GTFS are wrong, use the trips that go to San Jose
    # representative_trip_df.loc[(representative_trip_df.shape_id==487)&(representative_trip_df.tod=="AM"), 
    #                 "trip_id"] = 8042
    # representative_trip_df.loc[(representative_trip_df.shape_id==487)&(representative_trip_df.tod=="MD"), 
    #                 "trip_id"] = 8049
    # representative_trip_df.loc[(representative_trip_df.shape_id==487)&(representative_trip_df.tod=="PM"), 
    #                 "trip_id"] = 8054
    # representative_trip_df.loc[(representative_trip_df.shape_id==487)&(representative_trip_df.tod=="NT"), 
    #                 "trip_id"] = 8063

    ######### NOTE: 
    # In the following v12 non-bus routing methods, part 1 has been updated based on Ranch, so that rail/ferry links and nodes
    # won't rely on model_link_id and model_node_id, which we've decided to exclude from Pipeline networks.
    # Part 2 hasn't been updated to be consistent with the new part 1. However, the Ranch non-bus routing method already 
    # incoporated cases where GTFS data doesn't have shape.txt. Therefore, should just use Ranch non-bus routing method
    # to replace V12 part 1-3. Keep them for now as reference. Will remove later once the Ranch non-bus routing method is finalized.

    # 1. route rail/ferry based on GTFS shape.txt
    rail_trip_link_gdf, rail_stops_gdf, rail_nodes_gdf, unique_rail_nodes_gdf, unique_rail_links_gdf = \
        methods.v12_route_non_bus(all_stop_times_df,
                                  all_shapes_df,
                                  all_routes_df,
                                  representative_trip_df,
                                  stop_gdf,
                                  drive_link_gdf)

    WranglerLogger.info('finished generating rail links and nodes')
    WranglerLogger.info('{:,} rail nodes, with columns:\n {}'.format(
        rail_nodes_gdf.shape[0],
        rail_nodes_gdf.dtypes))
    WranglerLogger.info('{:,} rail links with {:,} unique shapes; columns:\n{} '.format(
        rail_trip_link_gdf.shape[0],
        rail_trip_link_gdf.shape_id.nunique(),
        rail_trip_link_gdf.dtypes))

    # 2. create lines and nodes for ACE, CCTA, VINE whose GTFS data doesn't have 'shape.txt' info
    WranglerLogger.info('creating lines and nodes for ACE because "ACE_2017_3_20" GTFS feed is missing shape.txt')

    ACE_linestring_gdf, ACE_rail_node_df = methods.v12_create_links_nodes_for_GTFS_missing_shapes(representative_trip_df,
                                                                                                  all_stop_times_df,
                                                                                                  all_stops_df,
                                                                                                  'ACE_2017_3_20')
    WranglerLogger.info('created {:,} ACE nodes, with columns:\n {}'.format(
        ACE_rail_node_df.shape[0],
        ACE_rail_node_df.dtypes))
    WranglerLogger.info('created {:,} ACE links with {:,} unique shapes; columns:\n{} '.format(
        ACE_linestring_gdf.shape[0],
        ACE_linestring_gdf.shape_id.nunique(),
        ACE_linestring_gdf.dtypes))                                                                                        

    # 3. add ACE data into the rest of rail
    # TODO: this part hasn't been updated to be consistent with "v12_route_non_bus()"
    rail_path_link_df = pd.concat([rail_path_link_df,
                                   ACE_linestring_gdf], sort = False, ignore_index = True)
    rail_path_node_df = pd.concat([rail_path_node_df,
                                   ACE_rail_node_df], sort = False, ignore_index = True)
    
    WranglerLogger.info('after adding ACE shapes, there are {:,} rail links with {:,} unique shapes, {:,} rail nodes'.format(
        rail_path_link_df.shape[0],
        rail_path_link_df.shape_id.nunique(),
        rail_path_node_df.shape[0]))
    
    ####################################
    # route non-bus (rail and ferry) - Ranch methods
    ranch_rail_trip_link_gdf, ranch_rail_stops_gdf, ranch_rail_nodes_gdf, ranch_unique_rail_nodes_gdf, ranch_unique_rail_links_gdf = \
        methods.ranch_route_non_bus(
            representative_trip_df,
            all_routes_df,
            all_shapes_df,
            all_stop_times_df,
            stop_gdf,
            drive_link_gdf)

    # write out to QAQC
    RANCH_RAIL_LINKS_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'ranch_rail_trip_link_gdf.feather')
    RANCH_RAIL_NODE_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'ranch_rail_nodes_gdf.feather')
    RANCH_RAIL_STOPS_QAQC_FILE = os.path.join(TRANSIT_ROUTING_QAQC_DIR, 'ranch_rail_stops_gdf.feather')
    WranglerLogger.info('exporting ranch non-bus links to {}'.format(RANCH_RAIL_LINKS_QAQC_FILE))
    geofeather.to_geofeather(ranch_rail_trip_link_gdf, RANCH_RAIL_LINKS_QAQC_FILE)
    WranglerLogger.info('exporting ranch non-bus nodes to {}'.format(RANCH_RAIL_NODE_QAQC_FILE))
    geofeather.to_geofeather(ranch_rail_nodes_gdf, RANCH_RAIL_NODE_QAQC_FILE) 
    WranglerLogger.info('exporting ranch non-bus stops to {}'.format(RANCH_RAIL_STOPS_QAQC_FILE))
    geofeather.to_geofeather(ranch_rail_stops_gdf, RANCH_RAIL_STOPS_QAQC_FILE)
    

    ####################################
    # combine roadway and rail links and nodes (bus nodes and links are already in roadway networks)
    WranglerLogger.info('combine roadway and rail links and nodes')

    roadway_and_rail_link_gdf, \
    roadway_and_rail_node_gdf, \
    unique_rail_link_gdf, \
    unique_rail_node_gdf, \
    rail_path_link_gdf = methods.v12_combine_roadway_and_rail_links_nodes(rail_path_link_df, 
                                                                          rail_path_node_df,
                                                                          link_gdf,
                                                                          node_gdf)

    WranglerLogger.info('{:,} roadway links, {:,} links after adding rail-only links'.format(
        link_gdf.shape[0], 
        roadway_and_rail_link_gdf.shape[0]))
    WranglerLogger.info('{:,} roadway nodes, {:,} nodes after adding rail-only nodes'.format(
        node_gdf.shape[0],
        roadway_and_rail_node_gdf.shape[0]))

    ####################################
    # clean up

    # 1. TODO: re-number rail nodes and links to be consistent with the county numbering ranges, therefore 
    # the rail-only nodes and links will start from each county's latest roadway "model_node_id" / "model_link_id".
    # Since current we are skipping model_node_id and model_link_id creation, skip this part as well.

    # 2. calculate trip frequency for representative trips
    freq_df = methods.create_freq_table(representative_trip_df,
                                        methods.TIME_OF_DAY_DICT,
                                        methods.TOD_NUMHOURS_FREQUENCY_DICT)
    
    # 3. create new shape with complete node list the route passes to replace the gtfs shape.txt
    shape_point_df = methods.create_shape_node_table(roadway_and_rail_node_gdf, 
                                                     bus_routed_link_df,
                                                     rail_path_link_gdf)
    
    ####################################
    # write out standard transit data
    methods.write_standard_transit(GTFS_OUTPUT_DIR,
                                   representative_trip_df,
                                   stop_gdf,
                                   shape_point_df,
                                   freq_df, 
                                   all_stop_times_df, 
                                   all_routes_df, 
                                   all_trips_df, 
                                   unique_rail_node_gdf)
    
    ####################################
    # create rail walk access/egress links and assign them model_link_id
    # TODO: decide where to relocate this step to if it does not belong in Pipeline.


    ####################################
    # export roadway+rail links and nodes
    #  
