import argparse, copy, os, pickle, sys
from datetime import datetime
import pandas as pd
# https://github.com/BayAreaMetro/network_wrangler/tree/generic_agency
from network_wrangler import WranglerLogger, setupLogging
# https://github.com/BayAreaMetro/Lasso/tree/mtc_parameters
import lasso
from lasso import mtc

USAGE = """
  Reads a pickled instance of a network_wrangler.Scenario and creates instances of
  lasso.ModelRoadwayNetwork, which is written to model_net.pickle in the working directory.

  If that file is found, then it can be read/used instead of re-converting and re-computing.

  If --gpkg_output_dir is passed, outputs the model network into that directory as a
  GeoPackage.

  If --cube_output_dir is passed, outputs the model network into that directory for Cube, 
  including TrueShape shapefile.  Uses lasso.CubeTransit.
  
  If --emme_output_dir is passed, outputs the model network into that directory for Emme.
  Uses lasso.EMMETransit.

"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario_pickle_file",         help="Input: Specify the scenario pickle file to read.")
    parser.add_argument("transfer_fare_csv",            help="Location of transfer.csv file specifying transfer fares between systems")
    parser.add_argument("--gpkg_output_dir",            help="Output directory for model network in file geodatabase format")
    parser.add_argument("--gpkg_link_filter",           help="Optional link variable to additionally output link subsets (e.g. county)")
    parser.add_argument("--cube_output_dir",            help="Output directory for model network in Cube format")
    parser.add_argument("--emme_output_dir",            help="Output directory for model network in Emme format")
    parser.add_argument("--add_QA_vars",                help="Option to output extra variables for QA purposes; put Y in the arg to indicate yes")
    parser.add_argument("--faresystem_crosswalk_file",  help="The file path to faresystem_crosswalk.txt. A required input. We may want to get rid of this arg in the future.")
    args = parser.parse_args()

    # TODO: Is this defined elsewhere?  Like in lasso\mtc.py?
    MODEL_ROADWAY_LINK_VARIABLES = [
      'A','B','model_link_id','shstGeometryId','name',                              # IDs
      'ft','assignable','cntype','distance','county',                               # Misc attributes
      'bike_access','drive_access','walk_access','rail_only','bus_only','transit',  # Mode attributes
      'managed','tollbooth','tollseg','segment_id',                                 # Managed roadway
      'lanes_EA','lanes_AM','lanes_MD','lanes_PM','lanes_EV',                       # Lanes
      'useclass_EA','useclass_AM','useclass_MD','useclass_PM','useclass_EV',        # Use classes
      'geometry'                                                                    # geometry
    ]

    # Include extra variables if the add_QA_vars option is chosen
    # For now the only extra variable is heuristic_num
    if args.add_QA_vars == "Y":
        MODEL_ROADWAY_LINK_VARIABLES = [
          'A','B','model_link_id','shstGeometryId','name',                              # IDs
          'ft','assignable','cntype','distance','county',                               # Misc attributes
          'bike_access','drive_access','walk_access','rail_only','bus_only','transit',  # Mode attributes
          'managed','tollbooth','tollseg','segment_id',                                 # Managed roadway
          'heuristic_num','lanes_EA','lanes_AM','lanes_MD','lanes_PM','lanes_EV',       # Lanes
          'useclass_EA','useclass_AM','useclass_MD','useclass_PM','useclass_EV',        # Use classes
          'geometry'                                                                    # geometry
        ]

    MODEL_ROADWAY_NODE_VARIABLES = [
      'N','osm_node_id','tap_id',                                                   # IDs
      'county',                                                                     # Misc attributes
      'bike_access','drive_access','walk_access','rail_only','farezone',            # Mode attributes
      'geometry'                                                                    # geometry
    ]

    # create logger files
    LOG_FILENAME = os.path.join(
        os.getcwd(),
        "convert_scenario_to_modeled_networks_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    )
    setupLogging(info_log_filename = LOG_FILENAME,
                 debug_log_filename = LOG_FILENAME.replace('info', 'debug'))


    # log the arguments passed
    WranglerLogger.info("Args:")
    for arg in vars(args):
      WranglerLogger.info("{0: >20}: {1}".format(arg, getattr(args, arg)))

    if (args.cube_output_dir == None) and (args.emme_output_dir == None) and (args.gpkg_output_dir == None):
      WranglerLogger.fatal("No gpkg_output_dir, cube_output_dir nor emme_output_dir passed.  Nothing to do. Exiting")
      sys.exit("No gpkg_output_dir, cube_output_dir nor emme_output_dir passed.  Nothing to do. Exiting")

    if args.gpkg_link_filter and args.gpkg_link_filter not in MODEL_ROADWAY_LINK_VARIABLES:
      WranglerLogger.fatal("gpkg_link_filter argument passed [{}] which is not one of MODEL_ROADWAY_LINK_VARIABLES."
        .format(args.gpkg_link_filter))
      WranglerLogger.fatal("MODEL_ROADWAY_LINK_VARIABLES: {}".format(MODEL_ROADWAY_LINK_VARIABLES))
      WranglerLogger.fatal("Exiting.")
      sys.exit("gpkg_link_filter argument passed [{}] which is not one of MODEL_ROADWAY_LINK_VARIABLES."
        .format(args.gpkg_link_filter))

    faresystem_crosswalk_file = args.faresystem_crosswalk_file
    if os.path.exists(faresystem_crosswalk_file):
      print("The fare system crosswalk file exists: ",os.path.exists(faresystem_crosswalk_file))
    else: 
      WranglerLogger.fatal("faresystem_crosswalk.txt cannot be found.")
      WranglerLogger.fatal("Exiting.")
      sys.exit("faresystem_crosswalk.txt cannot be found. Exiting.")

    # use the installed lasso directory as the lasso_base_dir
    LASSO_DIR = os.path.dirname(lasso.__file__)
    my_param = lasso.Parameters(lasso_base_dir=LASSO_DIR)
    
    # read the scenario pickle
    WranglerLogger.info("Reading scenario pickle file from {}".format(args.scenario_pickle_file))
    scenario = pickle.load(open(args.scenario_pickle_file, 'rb'))

    # give the user the option to read this and use it rather than recreating the model network
    # since these steps are slowwww
    MODEL_NET_PICKLE_FILE = "model_net.pickle"
    read_model_net_pickle = False
    if os.path.exists(MODEL_NET_PICKLE_FILE):
      WranglerLogger.info("{} exists.  Re-generate and recompute? ('y' means regenerate/'n' means read this file)".format(MODEL_NET_PICKLE_FILE))
      response = input("")
      if response in ["n","N"]:
        read_model_net_pickle = True
      WranglerLogger.info("Received response {}; read_model_net_pickle={}".format(response, read_model_net_pickle))



    if read_model_net_pickle:
      model_net = pickle.load(open(MODEL_NET_PICKLE_FILE, 'rb'))
    
    else:
      # make the model network
      model_net = lasso.ModelRoadwayNetwork.from_RoadwayNetwork(
        roadway_network_object = scenario.road_net, 
        parameters = my_param
      )

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
      # background: the 'name' field is a mix of strings (e.g. 'Dublin Boulevard') and lists of strings 
      #             (e.g. ['Brockton Drive', 'Pimlico Drive'], ['', 'Murphy Ranch Road'], ['Sacramento Street', 'Sacramento Street']).
      #             A link would have a list of strings as 'name' if more than one OSM links were matched to 
      #             the same SharedStreets link during Pipeline's conflation. The shorten_name method cleans up 
      #             these list-type names to strings, the new names for the three examples above would be
      #             'Brockton Drive Pimlico Drive', 'Murphy Ranch Road', 'Sacramento Street'.
      # caveat: this method is not 100% "accurate" and may creat confusion, e.g. for 'Brockton Drive Pimlico Drive',
      #         is it really 'Brockton Drive' or 'Pimlico Drive'? A more complicated method may be to use the name of link with the longest
      #         matched length of all match linked. But maybe that is an overkill given the importance of 'name' in modeling.
      WranglerLogger.debug("Before shortening, model_links_mtc_df.name max len: {}".format(
        model_net.links_mtc_df['name'].str.len().max()))
      model_net.links_mtc_df['name'] = model_net.links_mtc_df['name'].apply(lambda x: lasso.util.shorten_name(x))
      WranglerLogger.debug("After shortening, model_links_mtc_df.name max len: {}".format(
        model_net.links_mtc_df['name'].str.len().max()))
 
      # write the model_net pickle
      WranglerLogger.info("Writing {}".format(MODEL_NET_PICKLE_FILE))
      pickle.dump(model_net, open(MODEL_NET_PICKLE_FILE, 'wb'))

    # create lasso.StandardTransit network
    standard_transit_net = lasso.StandardTransit.fromTransitNetwork(scenario.transit_net, parameters = my_param.__dict__)

    if args.gpkg_output_dir:
      # create GeoPackage output dir if it doesn't exist
      os.makedirs(args.gpkg_output_dir, exist_ok=True)

      WranglerLogger.info("Writing full roadway network GeoPackage")
      model_net.write_roadway_as_shp(
        output_dir=args.gpkg_output_dir,
        link_output_variables = MODEL_ROADWAY_LINK_VARIABLES,
        node_output_variables = MODEL_ROADWAY_NODE_VARIABLES,
        output_gpkg = 'model_net.gpkg',
        output_link_gpkg_layer = 'roadway_links',
        output_node_gpkg_layer = 'roadway_nodes',
        output_gpkg_link_filter = args.gpkg_link_filter
      )

      lasso.mtc.write_cube_lines_to_geopackage(
        output_dir               = args.gpkg_output_dir, 
        output_gpkg              = "model_net.gpkg",
        transit_network          = standard_transit_net, 
        standard_roadway_network = scenario.road_net,
        parameters               = my_param,
        # TODO: Deal with this dependency!
        # "C:\\Users\\lzorn\\Documents\\scratch\\tm2_network_building\\processed\\version_12\\network_cube\\faresystem_crosswalk.txt"
        #faresystem_crosswalk_file is now an arg.
        faresystem_crosswalk_file = args.faresystem_crosswalk_file
      )
      
    if args.cube_output_dir:
      # create cube output dir if it doesn't exist
      os.makedirs(args.cube_output_dir, exist_ok=True)
      # write shapefiles first
      model_net.write_roadway_as_shp(
        output_dir = args.cube_output_dir,
        output_link_shp = 'links.shp',
        output_node_shp = 'nodes.shp',
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
      # transfer.csv is the inter-agency transfer cost, created manually using the TM2 legacy fare.far
      transfer_fare_df = pd.read_csv(args.transfer_fare_csv)
      WranglerLogger.debug("transfer_fare_df.head():\n{}".format(transfer_fare_df.head()))

      lasso.mtc.write_cube_fare_files(
        roadway_network  = model_net,
        transit_network  = scenario.transit_net,
        parameters       = my_param,
        outpath          = args.cube_output_dir,
        transfer_fare_df = transfer_fare_df
      )

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
