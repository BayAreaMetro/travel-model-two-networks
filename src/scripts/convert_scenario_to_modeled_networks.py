import argparse, copy, os, pickle, sys
import pandas as pd
# https://github.com/BayAreaMetro/network_wrangler/tree/generic_agency
from network_wrangler import WranglerLogger 
# https://github.com/BayAreaMetro/Lasso/tree/mtc_parameters
import lasso            

USAGE = """
  Reads a pickled instance of a network_wrangler.Scenario and creates instances of
  lasso.ModelRoadwayNetwork, lasso.CubeTransit, and lasso.EMMETransit, and writes them out.

"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario_pickle_file", help="Input: Specify the scenario pickle file to read.")
    parser.add_argument("transfer_fare_csv",    help="Location of transfer.csv file specifying transfer fares between systems")
    parser.add_argument("--cube_output_dir",    help="Output directory for model network in Cube format")
    parser.add_argument("--emme_output_dir",    help="Output directory for model network in Emme format")
    args = parser.parse_args()

    # log the arguments passed
    WranglerLogger.info("Args:")
    for arg in vars(args):
      WranglerLogger.info("{0: >20}: {1}".format(arg, getattr(args, arg)))

    if (args.cube_output_dir == None) and (args.emme_output_dir == None):
      WranglerLogger.error("No cube_output_dir passed nor emme_output_dir passed.  Nothing to do. Exiting")
      sys.exit("No cube_output_dir passed nor emme_output_dir passed.  Nothing to do. Exiting")
  
    # use the installed lasso directory as the lasso_base_dir
    LASSO_DIR = os.path.dirname(lasso.__file__)
    my_param = lasso.Parameters(lasso_base_dir=LASSO_DIR)
    
    # read the scenario pickle
    WranglerLogger.info("Reading scenario pickle file from {}".format(args.scenario_pickle_file))
    scenario = pickle.load(open(args.scenario_pickle_file, 'rb'))

    # make the model network
    model_net = lasso.ModelRoadwayNetwork.from_RoadwayNetwork(roadway_network_object = scenario.road_net, 
      parameters = my_param)

    # update farezone due to AC Transit, Fairfield, gg ferries Fare change
    # todo: What's this?
    model_net = lasso.mtc.calculate_farezone(
      roadway_network = model_net,
      transit_network = scenario.transit_net,
      parameters = my_param,
      network_variable = 'farezone',
      overwrite = True,
    )

    # todo: What are we expecting here?
    WranglerLogger.debug("model_net.nodes_df.farezone.value_counts():\n{}".format(
      model_net.nodes_df.farezone.value_counts()))

    WranglerLogger.info("Running roadway_standard_to_mtc_network()")
    model_net = lasso.mtc.roadway_standard_to_mtc_network(model_net, my_param)

    # add county
    model_net = lasso.mtc.calculate_county(
      roadway_network = model_net,
      parameters = my_param,
      network_variable = 'county'
    )

    WranglerLogger.debug("model_net.links_mtc_df.county.value_counts():\n{}".format(
      model_net.links_mtc_df.county.value_counts()))

    WranglerLogger.debug("model_net.nodes_mtc_df.county.value_counts():\n{}".format(
      model_net.nodes_mtc_df.county.value_counts()))

    # shorten name
    WranglerLogger.debug("Before shortening, model_links_mtc_df.name max len: {}".format(
      model_net.links_mtc_df['name'].str.len().max()))
    model_net.links_mtc_df['name'] = model_net.links_mtc_df['name'].apply(lambda x: lasso.util.shorten_name(x))
    WranglerLogger.debug("After shortening, model_links_mtc_df.name max len: {}".format(
      model_net.links_mtc_df['name'].str.len().max()))
 
    # write the model_net pickle
    WranglerLogger.info("Writing model_net.pickle")
    pickle.dump(model_net, open("model_net.pickle", 'wb'))

    if args.cube_output_dir:
      # create cube output dir if it doesn't exist
      os.makedirs(args.cube_output_dir, exist_ok=True)
      # write shapefiles first
      model_net.write_roadway_as_shp(
        output_link_shp = os.path.join(args.cube_output_dir, 'links.shp'),
        output_node_shp = os.path.join(args.cube_output_dir, 'nodes.shp'),
        link_output_variables = ["model_link_id", "A", "B", "geometry", "cntype", "lanes_AM", "assignable", "useclass_AM", 'name', 'tollbooth'],
        node_output_variables = ["model_node_id", "N", "geometry", "farezone", "tap_id"],
        data_to_csv = False,
        data_to_dbf = True,
      )

      model_net.write_roadway_as_fixedwidth(
        output_dir = args.cube_output_dir,
        output_link_txt = 'links.txt',
        output_node_txt = 'nodes.txt',
        output_link_header_width_txt = 'links_header_width.txt',
        output_node_header_width_txt = 'nodes_header_width.txt',
        output_cube_network_script   = 'make_complete_network_from_fixed_width_file.s',
        #drive_only = True
      )

      # read special fare transfer file that is an input to this function
      transfer_fare_df = pd.read_csv(args.transfer_fare_csv)
      WranglerLogger.debug("transfer_fare_df.head():\n{}".format(transfer_fare_df.head()))

      lasso.mtc.write_cube_fare_files(
        roadway_network  = model_net,
        transit_network  = scenario.transit_net,
        parameters       = my_param,
        outpath          = args.cube_output_dir,
        transfer_fare_df = transfer_fare_df
      )

      standard_transit_net = lasso.StandardTransit.fromTransitNetwork(scenario.transit_net, parameters = my_param.__dict__)
      # write the agency transit lin files (why?)
      for agency in standard_transit_net.feed.routes.agency_raw_name.unique():
        sub_transit_net = copy.deepcopy(standard_transit_net)
        sub_transit_net.feed.trips = sub_transit_net.feed.trips[sub_transit_net.feed.trips.agency_raw_name == agency]
        lasso.mtc.write_as_cube_lin(sub_transit_net, my_param, 
          outpath = os.path.join(args.cube_output_dir, agency + "_transit.lin"))

      # write the consolidated agency transit lin files
      lasso.mtc.write_as_cube_lin(standard_transit_net, my_param, outpath = os.path.join(args.cube_output_dir, "transit.lin"))

      WranglerLogger.info("Completed writing cube files")

    if args.emme_output_dir:
      from lasso import emme

      # create emme output dir if it doesn't exist
      os.makedirs(args.emme_output_dir, exist_ok=True)

      lasso.emme.create_emme_network(
        links_df = model_net.links_mtc_df,
        nodes_df = model_net.nodes_mtc_df,
        transit_network = standard_transit_net,
        name  = "version 12",
        path  = args.emme_output_dir,
        write_drive_network = True,
        write_maz_active_modes_network = True,
        write_tap_transit_network = True,
        parameters = my_param,
        polygon_file_to_split_active_modes_network = os.path.join(LASSO_DIR, '..', 'mtc_data', 'emme', 'subregion_boundary_for_active_modes.shp') ,
        polygon_variable_to_split_active_modes_network = 'subregion'
      )
    
      WranglerLogger.info("Completed writing emme files")
    
    # Success
    sys.exit(0)
