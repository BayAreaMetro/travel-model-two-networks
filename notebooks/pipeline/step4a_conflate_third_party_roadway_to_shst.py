USAGE = """
Prepares third-party data for SharedStreet conflation.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Inputs: third-party data sources, including -
    - TomTom Bay Area network
    - TM2 non-Marion version network
    - TM2 Marin version network
    - SFCTA Stick network
    - CCTA 2015 network
    - ACTC network
    - PEMS count
Outputs: two sets of data for each of the above sources:
    - data only with 'geometry' and identification, to be used in SharedStreet matching;
      also partitioned to match ShSt sub-region boundaries
    - modified_all_attrs.gpkg: data with all link attributes, to be joined back to the SharedStreet matching result  

"""
import argparse, datetime, os, sys
from textwrap import wrap
import methods
import docker
import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
import geofeather
from shapely.geometry import Point, shape, LineString
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging

# third-party network data types
TOMTOM        = 'TomTom'
TM2_NON_MARIN = 'TM2_nonMarin'
TM2_MARIN     = 'TM2_Marin'
SFCTA         = 'SFCTA'
CCTA          = 'CCTA'
ACTC          = 'ACTC'
PEMS          = 'PeMS'

#####################################
# EPSG requirement
# LAT_LONG_EPSG = 4326
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)
# NEAREST_MATCH_EPSG = 26915
nearest_match_epsg_str = 'epsg:{}'.format(methods.NEAREST_MATCH_EPSG)
WranglerLogger.info('nearest match ESPG: {}'.format(nearest_match_epsg_str))

#####################################
# inputs and outputs
INPUT_DATA_DIR  = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR = os.environ['OUTPUT_DATA_DIR']

# sub-region boundary polygons to split third-party data
BOUNDARY_DIR = os.path.join(INPUT_DATA_DIR, 'step0_boundaries')

# third-party network data inputs
THIRD_PARTY_INPUT_DIR = os.path.join(INPUT_DATA_DIR, 'step4_third_party_data')
THIRD_PARTY_INPUT_FILES = {
    TOMTOM          : os.path.join(THIRD_PARTY_INPUT_DIR, TOMTOM,        'input', 'network2019', 'Network_region.gdb'),
    TM2_NON_MARIN   : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_NON_MARIN, 'input', 'mtc_final_network_base_prj.feather'),
    TM2_MARIN       : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_MARIN,     'input', 'mtc_final_network_base_prj.feather'),
    SFCTA           : os.path.join(THIRD_PARTY_INPUT_DIR, SFCTA,         'input', 'SanFrancisco_links.shp'),
    CCTA            : os.path.join(THIRD_PARTY_INPUT_DIR, CCTA,          'input', 'ccta_2015_network.shp'),
    ACTC            : os.path.join(THIRD_PARTY_INPUT_DIR, ACTC,          'input', 'AlamedaCo_MASTER_20190410_no_cc.shp'),
    PEMS            : os.path.join(THIRD_PARTY_INPUT_DIR, PEMS,          'input', 'pems_period.csv')
}
THIRD_PARTY_OUTPUT_DIR  = os.path.join(OUTPUT_DATA_DIR, 'step4_third_party_data')
# conflation will be done in [THIRD_PARTY_OUTPUT_DIR]/[third_party]/conflation_shst except for PEMS which uses nearest point method
CONFLATION_SHST = 'conflation_shst'
CONFLATION_PEMS_= os.path.join(THIRD_PARTY_OUTPUT_DIR, PEMS, 'nearest_match')


def reverse_TomTom(to_reverse_gdf):
    """
    Reverse the given links, in place, by:

    1. Adds column, reversed = True
    2. Reversing the geometry
    ? Check Route Directional Validity Direction?
    """
    to_reverse_gdf['reversed'] = True
    
    to_reverse_gdf.reset_index(inplace=True)
    to_reverse_gdf.geometry =  methods.reverse_geometry(to_reverse_gdf)

    WranglerLogger.debug('reverse_TomTom(): RTEDIRVD (Route Directional Validity Direction):\n{}'.format(
        to_reverse_gdf['RTEDIRVD'].value_counts(dropna=False)))


def conflate_TOMTOM(docker_container_name):
    """
    Conflate TomTom data with sharedstreets.
    See TomTom documentation by loading web page from Box: https://mtcdrive.box.com/s/f4ytbcfesy3jc71nscjvaho79ygjcetw
    Specifically, please see multinetCore_2020/specifications/mn_format_spec/shp_osl/tables/nw.html

    Subset the data to only those links that have meaningful data that we want, which for now is:
    * ONEWAY    = Direction of Traffic Flow
                  blank: two-way
                  FT: Open in positive direction
                  N: Closed in both directions
                  TF: Open in Negative Direction
    * RAMP      = 1: Exit Ramp, 2: Entrance Ramp
    * FREEWAY   = 0: No Part of Freeway, 1: Part of Freeway
    * FRC       = Functional Road Class
    * METERS    = Network Feature Length in meters
    * FEATTYP   = Network Feature Type
    * ID        = Network Identifier
    * LANES     = Number of Lanes
    * RTEDIR    = Route Directional
    * RTEDIRVD  = Route Directional Validity
    * SHIELDNUM = Route Number on Shield
    * RTETYPE   = Route Number Type
    * TOLLRD    = Toll Road

    Outputs:
    -- tomtom_matched_gdf.feather: TomTom links matched to SharedStreets links under the match command config 
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction".
    -- tomtom_unmatched_gdf.feather: TomTom links failed to find a match. It retains the fields of the TomTom links before shst match call.

    See methods.conflate() documentation for more detail about fields in the matched and unmatched output.

    The following files were also written out, though not used in later steps.  
    -- TomTom.in.feather: TomTom data before shst matching preparation for QA/QC.
    -- TomTom_[01-14].in.geojson: TomTom data as input for shst matching. If the whole dataset is too large, the conflation() method splits it into 14
       smaller geographies, each with an '.in.geojson file. 
    -- TomTom_[01-14].out.matched.geojson: shst matching output for matched links, corresponding to each '.in.geojson file.
    -- TomTom_[01-14].out.unmatched.geojson: shst matching output for unmatched links, corresponding to each '.in.geojson file.

    """
    # Prepare tomtom for conflation
    WranglerLogger.info('Reading TomTom data from {}'.format(THIRD_PARTY_INPUT_FILES[TOMTOM]))

    # print out all the layers from the .gdb file
    layers = fiona.listlayers(THIRD_PARTY_INPUT_FILES[TOMTOM])
    WranglerLogger.info('TomTom gdb has the following layers: {}'.format(layers))
    # load tomtom data, use MultiNet Network (NW) layer
    WranglerLogger.info('loading TomTom raw data from layer mn_nw')
    tomtom_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TOMTOM], layer='mn_nw')
    # the ID fields are not floats but uint64
    tomtom_gdf[['ID','F_JNCTID','T_JNCTID']] = tomtom_gdf[['ID','F_JNCTID','T_JNCTID']].astype(np.uint64)

    WranglerLogger.debug('Read {:,} rows from Network layer; dtypes:\n{}'.format(len(tomtom_gdf), tomtom_gdf.dtypes))

    # filter irrelevant links
    # FEATTYP (Network Feature Type): 4110 = Road Element, 4130 = Ferry Connection, 4165: Adress Area Boundary Element
    # => Filter to 4110 = Road Element
    WranglerLogger.debug('FEATTYP (Network Feature Type) values:\n{}'.format(tomtom_gdf['FEATTYP'].value_counts(dropna=False)))
    tomtom_gdf = tomtom_gdf.loc[ tomtom_gdf.FEATTYP == 4110]
    WranglerLogger.debug('Filtered to FEATTYP=4110, have {:,} rows'.format(len(tomtom_gdf)))

    # FRC (Functional Road Type):
    # -1: Not Applicable
    #  0: Main Road Motorways
    #  1: Roads Not Belonging to Main Road Major Importance
    #  2: Other Major Roads
    #  3: Secondary Roads
    #  4: Local Connecting Roads
    #  5: Local Roads of High Importance
    #  6: Local Roads
    #  7: Local Roads of Minor Importance
    #  8: Others
    # => Filter out 8: Others since they appearr to be foot paths, etc, and we get these from other sources
    WranglerLogger.debug('FRC (Functional Road Type) values:\n{}'.format(tomtom_gdf['FRC'].value_counts(dropna=False)))
    tomtom_gdf = tomtom_gdf.loc[ tomtom_gdf.FRC != 8]
    WranglerLogger.debug('Filtered to FRC != 8, have {:,} rows'.format(len(tomtom_gdf)))

    # ONEWAY (Direction of Traffic Flow)
    #      FT: Open in Positive Direction
    #       N: Closed in Both Directions
    #      TF: Open in Negative Direction
    #     ' ': Open in Both Directions
    WranglerLogger.debug('ONEWAY (Direction of Traffic Flow) list = {} values:\n{}'.format(
        tomtom_gdf['ONEWAY'].value_counts(dropna=False).index.tolist(),
        tomtom_gdf['ONEWAY'].value_counts(dropna=False)))
    # drop N: Closed in Both Directions
    tomtom_gdf = tomtom_gdf.loc[ tomtom_gdf.ONEWAY != 'N']
    WranglerLogger.debug('Filtered to ONEWAY != N, have {:,} rows'.format(len(tomtom_gdf)))

    # Reverse TF: Open in Negative Directions and [blank]: Open in Both Directions
    tomtom_to_reverse_gdf = tomtom_gdf.loc[ (tomtom_gdf.ONEWAY==' ') | (tomtom_gdf.ONEWAY=='TF')].copy()
    WranglerLogger.debug('Reversing " " and TF ONEWAY links, or {:,} rows'.format(len(tomtom_to_reverse_gdf)))
    # reverse the links
    reverse_TomTom(tomtom_to_reverse_gdf)

    # put them back together, dropping the TF since they're retained in the reverse set
    tomtom_gdf['reversed'] = False
    tomtom_gdf = pd.concat([
        tomtom_gdf.loc[tomtom_gdf.ONEWAY != 'TF'],
        tomtom_to_reverse_gdf],
        axis='index'
    )
    WranglerLogger.debug('Full set: {:,} rows:\n{}'.format(len(tomtom_gdf), tomtom_gdf.ONEWAY.value_counts(dropna=False)))

    # How many of these have LANES data?
    WranglerLogger.debug('LANES (Number of Lanes) values:\n{}'.format(tomtom_gdf['LANES'].value_counts(dropna=False)))
    # By Functional Road Type
    WranglerLogger.debug('FRC x LANES values:\n{}'.format(tomtom_gdf[['FRC','LANES']].value_counts(dropna=False)))
    # Roadway Names
    tomtom_gdf['NAME_strlen'] = tomtom_gdf['NAME'].str.len()
    WranglerLogger.debug('NAME_strlen values:\n{}'.format(tomtom_gdf['NAME_strlen'].value_counts(dropna=False)))

    # load the 'Speed Restrictions' table
    tomtom_sr_df = pd.DataFrame(gpd.read_file(THIRD_PARTY_INPUT_FILES[TOMTOM], layer='mn_sr'))
    tomtom_sr_df.drop(columns='geometry', inplace=True) # this is empty
    tomtom_sr_df['id'] = tomtom_sr_df['id'].astype(np.uint64)  # this isn't a float => cast

    WranglerLogger.debug('Read {:,} rows from Speed Restrictions table; dtypes:\n{}'.format(len(tomtom_sr_df), tomtom_sr_df.dtypes))

    # speedtyp: 0 = Undefined, 1 = Maximum Speed, 2 = Recommended Speed, 3 = Land Dependent Maximum Speed, 4 = Speed Bump
    # => Filter to 1 = Maximum Speed
    WranglerLogger.debug('SR speedtyp (Speed Type) values:\n{}'.format(tomtom_sr_df['speedtyp'].value_counts(dropna=False)))
    tomtom_sr_df = tomtom_sr_df.loc[ tomtom_sr_df.speedtyp == "1"]
    WranglerLogger.debug('Filtered to speedtyp=="1", have {:,} rows'.format(len(tomtom_sr_df)))

    # vt: -1 = Not Applicable, 0 = All Vehicle Types, 11 = Passenger Cars, 12 = Residential Vehicle, 16 = Taxi, 17 = Public Bus
    # There are very few that are not 0 = All Vehicle Types, let's dump them
    WranglerLogger.debug('SR vt (Vehicle Type) values:\n{}'.format(tomtom_sr_df['vt'].value_counts(dropna=False)))
    tomtom_sr_df = tomtom_sr_df.loc[ tomtom_sr_df.vt == 0]
    WranglerLogger.debug('Filtered to vt==0, have {:,} rows'.format(len(tomtom_sr_df)))

    # check id -- assert no null values.  Report on duplicates.
    assert(pd.isnull(tomtom_sr_df.id).sum() == 0)
    tomtom_sr_df['duplicated_id'] = tomtom_sr_df.id.duplicated(keep=False) # mark all duplicates as true
    WranglerLogger.debug('SR duplicated id: \n{}'.format(tomtom_sr_df.loc[ tomtom_sr_df.duplicated_id == True].head(30)))
    tomtom_sr_df.drop(columns=['duplicated_id'], inplace=True)

    # debugging - let's see what these look like
    WranglerLogger.debug('SR seqnr (Sequential Number of the restriction on the Feature) values:\n{}'.format(
        tomtom_sr_df['seqnr'].value_counts(dropna=False)))
    WranglerLogger.debug('SR valdir (Validity Direction) values:\n{}'.format(tomtom_sr_df['valdir'].value_counts(dropna=False)))

    # if there are multiple speed restrictions on a single link (e.g. same id and valdir) then drop them
    tomtom_sr_df['duplicate_id_valdir'] = tomtom_sr_df.duplicated(subset=['id','valdir'], keep=False)
    WranglerLogger.debug('Dropping the following {:,} speed restrictions rows with duplicate id,valdir head(30):\n{}'.format(
        tomtom_sr_df['duplicate_id_valdir'].sum(), tomtom_sr_df.loc[ tomtom_sr_df['duplicate_id_valdir']==True ].head(30)
    ))
    tomtom_sr_df = tomtom_sr_df.loc[ tomtom_sr_df['duplicate_id_valdir']==False ]
    tomtom_sr_df.drop(columns=['duplicate_id_valdir'], inplace=True)
    WranglerLogger.debug('Filtered to duplicate_id_valdir==False, have {:,} rows'.format(len(tomtom_sr_df)))

    WranglerLogger.debug('SR speed (Speed Restriction) values:\n{}'.format(tomtom_sr_df['speed'].value_counts(dropna=False)))
    WranglerLogger.debug('SR verified values:\n{}'.format(tomtom_sr_df['verified'].value_counts(dropna=False)))

    # join speed restrictions to links based on id and valdir (Validity Direction)
    # join valdir==1 (Valid in Both Directions) first
    tomtom_gdf = pd.merge(
        left      = tomtom_gdf,
        right     = tomtom_sr_df.loc[tomtom_sr_df.valdir == 1],
        left_on   = 'ID', 
        right_on  = 'id',
        how       = 'left',
        indicator = True
    )
    tomtom_gdf.rename(columns = {'_merge':'merge sr_valdir1'}, inplace=True)
    WranglerLogger.debug('After merging 1, tomtom_gdf["merge sr_valdir1"].value_counts()\n{}'.format(
        tomtom_gdf["merge sr_valdir1"].value_counts(dropna=False)))
    
    # join valdirr==2 (Valid in Positive Line Directions)
    # join valdirr==3 (Valid in Negative Line Directions)
    tomtom_sr_df['reversed'] = False
    tomtom_sr_df.loc[tomtom_sr_df.valdir ==3, 'reversed'] = True
    tomtom_gdf = pd.merge(
        left      = tomtom_gdf,
        right     = tomtom_sr_df.loc[tomtom_sr_df.valdir > 1],
        left_on   = ['ID', 'reversed'],
        right_on  = ['id', 'reversed'],
        how       = 'left',
        indicator = True
    )
    tomtom_gdf.rename(columns = {'_merge':'merge sr_valdir23'}, inplace=True)
    WranglerLogger.debug('After merging 23, tomtom_gdf["merge sr_valdir23"].value_counts()\n{}'.format(
        tomtom_gdf["merge sr_valdir23"].value_counts(dropna=False)))

    WranglerLogger.debug('tomtom_gdf["merge sr_valdir1","merge sr_valdir23"].value_counts()\n{}'.format(
        tomtom_gdf[["merge sr_valdir1","merge sr_valdir23"]].value_counts(dropna=False)))

    # double join means the columns in the Speed Restrictions table have been added twice; consolidate to the first join (_X)
    # id, seqnr, speed, speedtyp, valdir, vt, verified
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both',       'id_x'] = tomtom_gdf[      'id_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both',    'seqnr_x'] = tomtom_gdf[   'seqnr_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both',    'speed_x'] = tomtom_gdf[   'speed_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both', 'speedtyp_x'] = tomtom_gdf['speedtyp_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both',   'valdir_x'] = tomtom_gdf[  'valdir_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both',       'vt_x'] = tomtom_gdf[      'vt_y']
    tomtom_gdf.loc[ tomtom_gdf['merge sr_valdir23'] == 'both', 'verified_x'] = tomtom_gdf['verified_y']
    # drop the _y versions and rename
    tomtom_gdf.drop(columns=['id_y','seqnr_y','speed_y','speedtyp_y','valdir_y','vt_y','verified_y'], inplace=True)
    tomtom_gdf.rename(columns={
        'id_x'      :'id',
        'seqnr_x'   :'seqnr',
        'speed_x'   :'speed',
        'speedtyp_x':'speedtyp',
        'valdir_x'  :'valdir',
        'vt_x'      :'vt',
        'verified_x':'verified'},
    inplace = True)

    # filter out rows that have no usable attributes, where usable attributes = NAME, LANES, speed
    tomtom_gdf['no_data'] = False
    tomtom_gdf.loc[ (tomtom_gdf['NAME_strlen'] <= 1) & \
                    (tomtom_gdf['speed'].isnull()) & \
                    (tomtom_gdf['LANES'] == 0), 'no_data'] = True
    WranglerLogger.debug('Filtering out {:,} no_data rows'.format(tomtom_gdf['no_data'].sum()))
    tomtom_gdf = tomtom_gdf.loc[ tomtom_gdf.no_data == False ]
    tomtom_gdf.drop(columns=['no_data'], inplace=True)
    WranglerLogger.debug('Final TomTom dataset for conflation => {:,} rows'.format(len(tomtom_gdf)))

    # finally, convert tomtom FRC to standard road type
    tomtom_FRC_dict = {
        0: "0-Motorway, Freeway, or Other Major Road",
        1: "1-Major Road Less Important than a Motorway",
        2: "2-Other Major Road",
        3: "3-Secondary Road",
        4: "4-Local Connecting Road",
        5: "5-Local Road of High Importance",
        6: "6-Local Road",
        7: "7-Local Road of Minor Importance",
        8: "8-Other Road"
    }
    tomtom_gdf['FRC_def'] = tomtom_gdf['FRC'].map(tomtom_FRC_dict)
    WranglerLogger.debug('TomTom FRC standardized value counts:\n{}'.format(
        tomtom_gdf.FRC_def.value_counts(dropna=False)))

    (tomtom_matched_gdf, tomtom_unmatched_gdf) = methods.conflate(
        TOMTOM, tomtom_gdf, ['ID','reversed'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)
    
    # evaluate shst matching performance and write out for QAQC
    # tomtom_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TomTom', 'conflation_shst', 'TomTom.in.feather')
    # tomtom_pre_shst_match_gdf = gpd.read_feather(tomtom_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = tomtom_gdf[['ID', 'reversed', 'FRC', 'FRC_def']].rename(columns = {'FRC'    : 'all_links_FRC',
                                                                                   'FRC_def': 'all_links_FRC_def'}),
        right = tomtom_matched_gdf[['ID', 'reversed', 'FRC', 'FRC_def']].drop_duplicates().rename(columns = {'FRC'    : 'matched_links_FRC',
                                                                                                             'FRC_def': 'matched_links_FRC_def'}),
        on    = ['ID', 'reversed'],
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, TOMTOM, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(TOMTOM))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)

    WranglerLogger.info('finished conflating TomTom data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} total TomTom links.'.format(
        len(tomtom_matched_gdf.groupby(['ID','reversed']).count()), tomtom_gdf.shape[0]))


def conflate_TM2_NON_MARIN(docker_container_name):
    """
    Conflate TM2 (NonMarin) data with sharedstreets.
    I didn't find the documentation for this version of the network. Based on visual inspections, the following
    fields are relevant.

    * NUMLANES   = model number of lanes
    * ASSIGNABLE = is link used for assignment
    * CNTYPE	 = link connector type
                   BIKE: bike link (all have 'NUMLANES'==0)
                   CRAIL: commuter rail
                   FERRY: ferry link
                   HRAIL: heavy rail link
                   LRAIL: light rail link
                   MAZ: MAZ connector link
                   PED: ped link (all have 'NUMLANES'==0)
                   TANA: regular network link
                   TAP: TAP link
                   TAZ: TAZ connector link
                   USE: HOV (user class) link
    * TRANSIT	 = is link transit
                   0: not transit
                   1: transit (including CNTYPE 'TAP', 'LRAIL', 'CRAIL', 'HRAIL', 'FERRY')
    * FT	     = facility type
                   0: Connector
                   1: Freeway to Freeway
                   2: Freeway
                   3: Expressway
                   4: Collector
                   5: Ramp
                   6: Special Facility
                   7: Major Arterial
    * USECLASS	 = link user class
                   0: NA; link open to everyone
                   2: HOV 2+
                   3: HOV 3+
                   4: No combination trucks

    TODO: What files are written?
    """
    # Prepare TM2 non-Marin for conflation
    WranglerLogger.info('loading TM2_nonMarin data from {}'.format(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN]))
    tm2_link_gdf = gpd.read_feather(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN])
    WranglerLogger.info('{:,} rows, {:,} unique A+B combos'.format(
        tm2_link_gdf.shape[0],
        tm2_link_gdf[['A', 'B']].drop_duplicates().shape[0]))
    WranglerLogger.debug('TM2_nonMarin raw data dtypes: \n{}'.format(tm2_link_gdf.dtypes))
    WranglerLogger.debug('TM2_nonMarin raw data projection: \n{}'.format(tm2_link_gdf.crs))

    # since it is model network, HOV/non-truck lanes and GP lanes have separate links, represented by 'USE'==2/3 or 4.
    # this step finds the cooresponding 
    tm2_link_gdf = methods.merge_legacy_tm2_network_hov_links_with_gp(tm2_link_gdf)

    # select only road way links
    tm2_link_gdf = tm2_link_gdf.loc[tm2_link_gdf.CNTYPE == 'TANA']
    WranglerLogger.debug('TM2_nonMarin has {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_link_gdf.shape[0], len(tm2_link_gdf.groupby(["A", "B"]).count())))

    # finally, convert legacy TM2 data FT to standard road type
    legacy_TM2_FT_dict = {
        0: "0-Connector",
        1: "1-Freeway to Freeway",
        2: "2-Freeway",
        3: "3-Expressway",
        4: "4-Collector",
        5: "5-Ramp",
        6: "6-Special Facility",
        7: "7-Major Arterial",
    }
    tm2_link_gdf['FT_def'] = tm2_link_gdf['FT'].map(legacy_TM2_FT_dict)
    WranglerLogger.debug('TM2_nonMarin FT standardized value counts:\n{}'.format(
        tm2_link_gdf.FT_def.value_counts(dropna=False)))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_NON_MARIN, tm2_link_gdf, ['A', 'B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    # evaluate shst matching performance and write out for QAQC
    # tm2nonMarin_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_nonMarin', 'conflation_shst', 'TM2_nonMarin.in.feather')
    # tm2nonMarin_pre_shst_match_gdf = gpd.read_feather(tm2nonMarin_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = tm2_link_gdf[['A', 'B', 'FT', 'FT_def']].rename(columns = {'FT'    : 'all_links_FT',
                                                                           'FT_def': 'all_links_FT_def'}),
        right = matched_gdf[['A', 'B', 'FT', 'FT_def']].drop_duplicates().rename(columns = {'FT'    : 'matched_links_FT', 
                                                                                            'FT_def': 'matched_links_FT_def'}),
        on    = ['A', 'B'],
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, TM2_NON_MARIN, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(TM2_NON_MARIN))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)

    WranglerLogger.info('finished conflating TM2_nonMarin data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} total TM2_nonMarin links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_link_gdf.shape[0]))
        

def conflate_TM2_MARIN(docker_container_name):
    """
    Conflate ACTC data with sharedstreets.
    See Marin version of the network documentation at http://bayareametro.github.io/travel-model-two/Marin/input/.
    For FT dictionary, refer to "TAMDM User's Guide (2020-09-17)" at https://www.tam.ca.gov/planning/travel-demand-model-traffic-monitoring/#documentslinks.

    Subset the data to only those links that have meaningful data that we want. Exclude TomTom attributes 
    (FRC, NAME, FREEWAY, TOLLRD, ONEWAY, LANE, RAMP, RTEDIR) since TomTom data is conflated separately.
    # TODO: looking at the data, all TANA links in Marin County have FT = 11, 12, 13, or 14, not in the dictionary!!

    * NUMLANES   = model number of lanes
    # LANES      = TomTom number of lanes
    ? PEMSID	 = PEMS ID
    ? PEMSLANES	 = PEMS number of lanes
    * ASSIGNABLE = is link used for assignment
    * CNTYPE	 = link connector type
                   BIKE: bike link (all have 'NUMLANES'==0)
                   CRAIL: commuter rail
                   FERRY: ferry link
                   HRAIL: heavy rail link
                   LRAIL: light rail link
                   MAZ: MAZ connector link
                   PED: ped link (all have 'NUMLANES'==0)
                   TANA: regular network link
                   TAP: TAP link
                   TAZ: TAZ connector link
                   USE: HOV (user class) link
    * TRANSIT	 = is link transit
    * FT	     = facility type
                   0: Special Connector
                   1: Freeway to Freeway
                   2: Freeway
                   3: Expressway
                   4: Collector
                   5: Ramp
                   6: Special Facility
                   7: Major Arterial
                   11: Minor Arterial
                   12: Major Collector
                   13: Minor Collector
                   14: Local
    * USECLASS	 = link user class
                   0: NA; link open to everyone
                   2: HOV 2+
                   3: HOV 3+
                   4: No combination trucks
    ? TOLLSEG	 = toll segment
    ? TOLLBOOTH	 = toll link. Links with values less than 11 are bridge tolls; 11 or above are value tolls
    * B_CLASS	 = BikeMapper bike class
                   0: Unclassified Street
                   1: Class I Trail
                   2: Class II Route
                   3: Class III Route
    * PED_FLAG	 = BikeMapper pedestrian access
                   Y: yes
                   blank: no
    * BIKEPEDOK	 = BikeMapper bridge that allows bike and peds
                   1: true
                   0: false

    Outputs:
    -- matched_gdf.feather: TM2_Marin links matched to SharedStreets links under the match command config 
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction".
    -- unmatched_gdf.feather: TM2_Marin links failed to find a match. It retains the fields of the TM2_Marin links before shst match call.

    See methods.conflate() documentation for more detail about fields in the matched and unmatched output.

    The following files were also written out, though not used in later steps.
    -- TM2_Marin.in.feather: TM2_Marin data before conflation  
    -- TM2_Marin_[01-14].in.geojson: TM2_Marin data as input for shst matching. If the whole dataset is too large, the conflation() method splits it into 14
       smaller geographies, each with an '.in.geojson file. 
    -- TM2_Marin_[01-14].out.matched.geojson: shst matching output for matched links, corresponding to each '.in.geojson file.
    -- TM2_Marin_[01-14].out.unmatched.geojson: shst matching output for unmatched links, corresponding to each '.in.geojson file.

    """
    # Prepare TM2 Marin for conflation
    WranglerLogger.info('loading TM2_Marin data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[TM2_MARIN])))
    tm2_marin_link_gdf = gpd.read_feather(THIRD_PARTY_INPUT_FILES[TM2_MARIN])
    WranglerLogger.info('{:,} rows, {:,} unique A+B combos'.format(
        tm2_marin_link_gdf.shape[0],
        tm2_marin_link_gdf[['A', 'B']].drop_duplicates().shape[0]))
    WranglerLogger.debug('TM2_Marin raw data dtypes: \n{}'.format(tm2_marin_link_gdf.dtypes))
    WranglerLogger.debug('TM2_Marin raw data projection: \n{}'.format(tm2_marin_link_gdf.crs))

    # - select only road way links, CNTYPE=='TANA' (exclude PED and BIKE since methods.conflate() uses car-rule);
    # - this would also exclude TRANSIT==1, FT==0, FT==6, so no need to drop them separately
    
    # - about bike & ped data: 
    #   - a visual inspection suggests that CNTYPE=='BIKE' links are not true-shape network links, but similar to
    #     TM1.5 network links, so the shst conflation is less likely to be accurate.
    #   - CNTYPE=='PED' appear to be true-shape network links, but they are primarily trails in open space, or paths within a building complex.
    #   - 'B_CLASS', 'PED_PLAG', 'BIKEPEDOK' seem useful. Suggest using these 3 fields to tag "bike_access" and "pedestrian_access",
    #     instead of only relying on the arbitrary config in step3.
    #       tm2_marin_link_gdf.loc[tm2_marin_link_gdf.B_CLASS != 0]['CNTYPE'].value_counts(dropna=False) OR
    #       tm2_marin_link_gdf.loc[tm2_marin_link_gdf.PED_FLAG == 'Y']['CNTYPE'].value_counts(dropna=False):    
    #           TANA: 117152
    #           PED : 9008
    #           BIKE: 6678
    #           TAZ:  246
    #           USE:  96
    #       tm2_marin_link_gdf.loc[(tm2_marin_link_gdf.B_CLASS != 0) & \
    #                              (tm2_marin_link_gdf.CNTYPE == 'TANA')]['FT'].value_counts(dropna=False) OR
    #       tm2_marin_link_gdf.loc[(tm2_marin_link_gdf.PED_FLAG == 'Y') & \
    #                              (tm2_marin_link_gdf.CNTYPE == 'TANA')]['FT'].value_counts(dropna=False):
    #           7 : 72027
    #           4 : 36477
    #           3 : 3976
    #           11: 1617
    #           14: 1504
    #           12: 1459
    #           2 : 60
    #           5 : 23
    #           13: 9
    #       tm2_marin_link_gdf.loc[tm2_marin_link_gdf.BIKEPEDOK == 1]['CNTYPE'].value_counts(dropna=False):
    #           TANA: 23
    #           PED : 34
    #       tm2_marin_link_gdf.loc[(tm2_marin_link_gdf.BIKEPEDOK == 1) & \
    #                              (tm2_marin_link_gdf.CNTYPE == 'TANA')]['FT'].value_counts(dropna=False):
    #           2: 20
    #           4: 2
    #           5: 1

    # since it is model network, HOV/non-truck lanes and GP lanes have separate links, represented by 'USE'==2/3 or 4.
    # this step finds the cooresponding 
    tm2_marin_link_gdf = methods.merge_legacy_tm2_network_hov_links_with_gp(tm2_marin_link_gdf)

    WranglerLogger.info('TM2_Marin link data CNTYPE stats: \n{}'.format(
        tm2_marin_link_gdf.CNTYPE.value_counts(dropna=False)))
    tm2_marin_link_gdf = tm2_marin_link_gdf.loc[tm2_marin_link_gdf.CNTYPE == "TANA"]
    WranglerLogger.info('filtered to CNYTPE==TANA, {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_marin_link_gdf.shape[0], len(tm2_marin_link_gdf.groupby(["A", "B"]).count())))

    # ONEWAY (TomTom data, Direction of Traffic Flow)
    #      FT: Open in Positive Direction
    #       N: Closed in Both Directions
    #      TF: Open in Negative Direction
    #      na: Open in Both Directions
    WranglerLogger.debug('ONEWAY (Direction of Traffic Flow) list = {} values:\n{}'.format(
        tm2_marin_link_gdf['ONEWAY'].value_counts(dropna=False).index.tolist(),
        tm2_marin_link_gdf['ONEWAY'].value_counts(dropna=False)))
    # drop N: Closed in Both Directions
    tm2_marin_link_gdf = tm2_marin_link_gdf.loc[tm2_marin_link_gdf.ONEWAY != 'N']
    WranglerLogger.debug('Filtered to ONEWAY != N, have {:,} rows'.format(len(tm2_marin_link_gdf)))
    # this dataset doesn't have ONEWAY==TF, and two-way links already have reversed lingstrings, no need
    # to create reversed geometry as in the TomTom case

    # finally, convert legacy TM2 data FT to standard road type
    Marin_TM2_FT_dict = {
        0: "0-Connector",
        1: "1-Freeway to Freeway",
        2: "2-Freeway",
        3: "3-Expressway",
        4: "4-Collector",
        5: "5-Ramp",
        6: "6-Special Facility",
        7: "7-Major Arterial",
        11: '11-Minor Arterial',
        12: '12-Major Collector',
        13: '13-Minor Collector',
        14: '14-Local'
    }
    tm2_marin_link_gdf['FT_def'] = tm2_marin_link_gdf['FT'].map(Marin_TM2_FT_dict)
    WranglerLogger.debug('TM2_Marin FT standardized value counts:\n{}'.format(
        tm2_marin_link_gdf.FT_def.value_counts(dropna=False)))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_MARIN, tm2_marin_link_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)    

    # evaluate shst matching performance and write out for QAQC
    # tm2Marin_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'TM2_Marin', 'conflation_shst', 'TM2_Marin.in.feather')
    # tm2Marin_pre_shst_match_gdf = gpd.read_feather(tm2Marin_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = tm2_marin_link_gdf[['A', 'B', 'FT', 'FT_def']].rename(columns = {'FT'    : 'all_links_FT',
                                                                                 'FT_def': 'all_links_FT_def'}),
        right = matched_gdf[['A', 'B', 'FT', 'FT_def']].drop_duplicates().rename(columns = {'FT'    : 'matched_links_FT', 
                                                                                            'FT_def': 'matched_links_FT_def'}),
        on    = ['A', 'B'],
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, TM2_MARIN, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(TM2_MARIN))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)

    WranglerLogger.info('finished conflating TM2_Marin data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} total TM2_Marin links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_marin_link_gdf.shape[0]))


def conflcate_SFCTA(docker_container_name):
    """
    Conflate SFCTA data with sharedstreets.
    See SFCTA network documentation at Box: https://mtcdrive.box.com/s/bqz2snotd4s6ctdsw18mbg74ugm3p16p.

    Subset the data to only those links that have meaningful data that we want, which for now is:
    * ONEWAY     = boolean to determine if link is one-way (two-way links already have reversed geometries)
    * USE        = use restrictions
                   2 and 3: HOV2 and HOV3
                   9: Transit only
                   4: no trucks
    * FT         = facility type
                   1 Fwy-Fwy Connector
                   2 Freeway
                   3 Expressway
                   4 Collector
                   5 Ramp
                   6 Centroid Connector
                   7 Major Arterial
                   8
                   9 Alley
                   10 Metered Ramp
                   11 Local
                   12 Minor Arterial
                   13 Bike-only
                   14
                   15 Super Arterial
    * STREETNAME = name of roads
    ? DISTANCE        = length of link in miles
    * LANE_{AM,PM,OP} = mumber of general purpose lanes in AM, excluding bus lanes
                        AM:
                        PM:
                        OP:
    * BUSLANE_{AM,PM,OP} = type of bus lane on link for time period
                           0: none
                           1: diamond
                           2: side BRT
                           3: center BRT
    * BIKE_CLASS = type of bicycle facility on route
                   0: None
                   1: Class 1 facility (off-street bike path or cycletrack) or Class 4 facility (protected bike lane or
                      bike lane physically buffered by soft-hit, parking, curb, raised)
                   2: Class 2 facility (conventional bike lane or paint-buffered lane with no physical protection)
                   3: Class 3 facility (sharrow, signed bike route, or bicycle boulevard)

    Outputs:
    -- matched_gdf.feather: SFCTA links matched to SharedStreets links under the match command config 
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction".
    -- unmatched_gdf.feather: SFCTA links failed to find a match. It retains the fields of the TM2_Marin links before shst match call.

    See methods.conflate() documentation for more detail about fields in the matched and unmatched output.

    The following files were also written out, though not used in later steps.  
    -- SFCTA.in.feather: SFCTA data before shst matching preparation for QA/QC.
    -- SFCTA.in.geojson: SFCTA data as input for shst matching. If the whole dataset is too large, the conflation() method splits it into 14
       smaller geographies, each with an '.in.geojson file. 
    -- SFCTA.out.matched.geojson: shst matching output for matched links, corresponding to each '.in.geojson file.
    -- SFCTA.out.unmatched.geojson: shst matching output for unmatched links, corresponding to each '.in.geojson file.

    """
    # Prepare SFCTA for conflation
    WranglerLogger.info('loading SFCTA data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[SFCTA])))
    sfcta_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[SFCTA])
    WranglerLogger.debug('SFCTA data has {:,} rows and dtypes: \n{}'.format(len(sfcta_gdf), sfcta_gdf.dtypes))

    # only conflate SF part of the network
    boundary_4_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_04.geojson'))
    sfcta_gdf = sfcta_gdf.to_crs(epsg=methods.LAT_LONG_EPSG) # match the boundary crs
    sfcta_gdf = sfcta_gdf.loc[sfcta_gdf.intersects(boundary_4_gdf.geometry.unary_union)]
    WranglerLogger.info('After filtering to links inside boundary_04, SFCTA network has {:,} links'.format(len(sfcta_gdf)))

    # remove "special facility" (FT 6)
    sfcta_gdf = sfcta_gdf.loc[(sfcta_gdf.FT != 6) & sfcta_gdf.FT.notnull()]
    WranglerLogger.info('After filter to FT != 6 and FT not null, SFCTA network has {:,} links'.format(len(sfcta_gdf)))

    # drop 6 transit-only links with USE==9, LANE==0 and BUSLANE==1; these links have "PROJ" == "FolsomTransitLanes", are bus lane created for the transportation project
    # drop 142 links with USE == 0 and LANE==0, either bike-only links (bike paths in open space) with FT==13, or planned bike lane (has 'PROJ').                                                                  
    sfcta_gdf = sfcta_gdf.loc[(sfcta_gdf.USE != 9) & (sfcta_gdf.USE != 0)]
    WranglerLogger.info('After filter to USE != 9 and USE != 0, SFCTA network has {:,} links'.format(len(sfcta_gdf)))

    # filter out TYPE=='PATH' or 'PLAZA' which are not road (some 'PATH' have USE==0 and were already filtered out in previous step)
    sfcta_gdf = sfcta_gdf.loc[(sfcta_gdf.TYPE != 'PATH') & (sfcta_gdf.TYPE != 'PLAZA')]
    WranglerLogger.info('After filter to TYPE != PATH and TYPE != PLAZA, SFCTA network has {:,} links'.format(len(sfcta_gdf)))

    # separate drive links and bike-only links
    sfcta_drive_gdf = sfcta_gdf.loc[sfcta_gdf.FT != 13]
    # cannot easily consolidate HOV links (USE=2, 3) to the corresponding GP links, therefore drop them from the drive links
    sfcta_drive_gdf = sfcta_drive_gdf.loc[sfcta_drive_gdf.USE == 1]
    WranglerLogger.info('{:} drive links after dropping HOV links'.format(sfcta_drive_gdf.shape[0]))

    sfcta_bike_gdf = sfcta_gdf.loc[sfcta_gdf.FT == 13]
    WranglerLogger.info('{:,} drive links, {:,} bike-only links'.format(sfcta_drive_gdf.shape[0], sfcta_bike_gdf.shape[0]))

    # finally, convert FT to readily interpretable definitions
    sfcta_FT_dict = {
        1: '1-Fwy-Fwy Connector',
        2: '2-Freeway',
        3: '3-Expressway',
        4: '4-Collector',
        5: '5-Ramp',
        6: '6-Centroid Connector',
        7: '7-Major Arterial',
        9: '9-Alley',
        10: '10-Metered Ramp',
        11: '11-Local',
        12: '12-Minor Arterial',
        13: '13-Bike-only',
        15: '15-Super Arterial'}
    sfcta_drive_gdf['FT_def'] = sfcta_drive_gdf['FT'].map(sfcta_FT_dict)
    WranglerLogger.debug('SFCTA FT definitions value counts:\n{}'.format(sfcta_drive_gdf.FT_def.value_counts(dropna=False)))

    # conflate the given drive network dataframe with SharedStreets using car rule
    (matched_gdf, unmatched_gdf) = methods.conflate(
        SFCTA, sfcta_drive_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    # evaluate shst matching performance and write out for QAQC
    # sfcta_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'SFCTA', 'conflation_shst', 'SFCTA.in.feather')
    # sfcta_pre_shst_match_gdf = gpd.read_feather(sfcta_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = sfcta_drive_gdf[['A', 'B', 'FT', 'FT_def']].rename(columns = {'FT'    : 'all_links_FT',
                                                                              'FT_def': 'all_links_FT_def'}),
        right = matched_gdf[['A', 'B', 'FT', 'FT_def']].drop_duplicates().rename(columns = {'FT'    : 'matched_links_FT',
                                                                                            'FT_def': 'matched_links_FT_def'}),
        on    = ['A', 'B'],
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, SFCTA, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(SFCTA))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)

    # # TODO (maybe): conflate the bike-only links with SharedStreets using bike rule or pedestrian rule
    # (bike_matched_gdf, bike_unmatched_gdf) = methods.conflate(
    #     SFCTA, sfcta_bike_gdf, ['A','B'], 'bike_link',
    #     THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    WranglerLogger.info('finished conflating SFCTA data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} SFCTA drive links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), sfcta_drive_gdf.shape[0]))


def conflate_CCTA(docker_container_name):
    """
    Conflate ACTC data with sharedstreets

    NOTE: CCTA network documentation at Box: https://mtcdrive.box.com/s/lsnml5tpbrhrcjfiabw8zbjd5y9r1ow6. However,
    how "decennial_model_update_Model User Guide.pdf" contains info on network data dictionary (Table 4.1, Table 4.2).
    The network doesn't have A, B fields, therefore cannot apply method.merge_legacy_tm2_network_hov_links_with_gp().
    
    Link attributes relevant to conflation:
    * DIR           = tag of one-way or two-way links
                      1: one-way
                      0: two-way
    * [AB/BA]_LANES = number of lanes of each direction
    * [AB/BA]_FT    = facility type
                      1: Freeway to freeway connector
                      2: Freeway
                      3: Expressway
                      4: Collector
                      5: Freeway Ramp
                      6: Dummy Link
                      7: Major Arterial
                      8: Metered Ramp
                      9: Special (not used)
                      10: Special (not used)
                      11: Local Street (potential new facility type)
                      12: Minor Arterial (potential new facility type)
    * [AB/BA]_USE   = use restrictions
                      1: Facility open to all vehicles
                      2 and 3: HOV2 and HOV3
                      4: no trucks

    Outputs:
    -- matched_gdf.feather: ACTC links matched to SharedStreets links under the match command config 
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction".
    -- unmatched_gdf.feather: ACTC links failed to find a match. It retains the fields of the ACTC links before shst match call.

    See methods.conflate() documentation for more detail about fields in the matched and unmatched output.

    The following files were also written out, though not used in later steps.  
    -- CCTA.in.feather: CCTA data before shst matching preparation for QA/QC.
    -- CCTA.in.geojson: CCTA data as input for shst matching. If the whole dataset is too large, the conflation() method splits it into 14
       smaller geographies, each with an '.in.geojson file. 
    -- CCTA.out.matched.geojson: shst matching output for matched links, corresponding to each '.in.geojson file.
    -- CCTA.out.unmatched.geojson: shst matching output for unmatched links, corresponding to each '.in.geojson file.
    """

    # Prepare CCTA for conflation
    WranglerLogger.info('loading CCTA data from {}'.format(THIRD_PARTY_INPUT_FILES[CCTA]))
    ccta_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[CCTA])
    WranglerLogger.debug('CCTA raw data has {:,} rows, {:,} unique ID'.format(
        ccta_raw_gdf.shape[0], ccta_raw_gdf.ID.nunique()))
    WranglerLogger.debug('CCTA raw data dtypes: \n{}'.format(ccta_raw_gdf.dtypes))
    WranglerLogger.debug('CCTA crs:\n{}'.format(ccta_raw_gdf.crs))

    # filter out connectors (FT == 6)
    ccta_gdf = ccta_raw_gdf.loc[(ccta_raw_gdf.AB_FT != 6) & (ccta_raw_gdf.BA_FT != 6)]
    WranglerLogger.debug('{:,} rows after dropping connector links'.format(ccta_gdf.shape[0]))
    # NOTE: there are links with FT == 13 which is not included in the dictionary. Based on visual inspection, they
    # appear to be HOV links.

    # this network is from transcad, for one way streets, dir=1;
    # for two-way streets, there is only one links with dir=0, need to create other direction
    WranglerLogger.debug('DIR value counts:\n{}'.format(ccta_gdf['DIR'].value_counts(dropna=False)))
    # from shapely.geometry import LineString
    WranglerLogger.debug('creating reversed links')
    two_way_links_gdf = ccta_gdf.loc[ccta_gdf.DIR == 0].copy()
    two_way_links_gdf["geometry"] = two_way_links_gdf.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]),
        axis=1)
    # rename links attributes by switching between 'AB' and 'BA'
    rename_columns = {}
    for colname in [x for x in ccta_gdf.columns if ('AB_' in x)]:
        rename_columns[colname] = colname.replace('AB', 'BA')
    for colname in [x for x in ccta_gdf.columns if ('BA_' in x)]:
        rename_columns[colname] = colname.replace('BA', 'AB')
    WranglerLogger.debug('renaming columns for reversed links: {}'.format(rename_columns))
    two_way_links_gdf.rename(columns=rename_columns, inplace=True)
    WranglerLogger.debug('max existing ID: {}'.format(two_way_links_gdf['ID'].max()))
    # TODO: why "9000000"? I assume the goal is to exceed the existing largest ID number, so need to be more generic
    two_way_links_gdf['ID'] = two_way_links_gdf['ID'] + 9000000
    two_way_links_gdf['reversed'] = True

    ccta_gdf['reversed'] = False
    ccta_gdf = pd.concat([ccta_gdf, two_way_links_gdf], sort=False, ignore_index=True)
    # double check
    WranglerLogger.debug('after creating other direction for two-way roads, ccta data has {:,} links, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))
    
    # after creating reversed links for two-way streets, drop the fields starting with 'BA_'
    drop_cols = [c for c in ccta_gdf.columns if c[:3] == 'BA_']
    ccta_gdf.drop(columns=drop_cols, axis=1, inplace=True)

    # convert FT to readily interpretable definitions
    ccta_FT_dict = {
        1: '1-Freeway to freeway connector',
        2: '2-Freeway',
        3: '3-Expressway',
        4: '4-Collector',
        5: '5-Freeway Ramp',
        6: '6-Dummy Link',
        7: '7-Major Arterial',
        8: '8-Metered Ramp',
        11: '11-Local Street',
        12: '12-Minor Arterial'}
    ccta_gdf['AB_FT_def'] = ccta_gdf['AB_FT'].map(ccta_FT_dict)
    WranglerLogger.debug('CCTA FT definitions value counts:\n{}'.format(ccta_gdf.AB_FT_def.value_counts(dropna=False)))  

    # this is a model network which contains hov and non-truck links, but it doesn't have A, B field, therefore unable to
    # easily map hov/non-truck links to the corresponding GP links. Separate these links.
    ccta_gp_links_gdf = ccta_gdf.loc[ccta_gdf['AB_USE'] == 1]
    ccta_gp_links_gdf.rename(columns = {'AB_LANES': 'AB_LANES_GP'}, inplace=True)
    WranglerLogger.debug('{:,} GP links'.format(ccta_gp_links_gdf.shape[0]))
    ccta_hovnontruck_links_gdf = ccta_gdf.loc[ccta_gdf['AB_USE'] != 1]
    ccta_hovnontruck_links_gdf.rename(columns = {'AB_LANES': 'AB_LANES_nonGP'}, inplace=True)
    WranglerLogger.debug('{:,} GP links'.format(ccta_hovnontruck_links_gdf.shape[0]))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        CCTA, ccta_gp_links_gdf, ['ID'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    # evaluate shst matching performance and write out for QAQC
    # ccta_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'CCTA', 'conflation_shst', 'CCTA.in.feather')
    # ccta_pre_shst_match_gdf = gpd.read_feather(ccta_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = ccta_gp_links_gdf[['ID', 'AB_FT', 'AB_FT_def']].rename(columns = {'AB_FT'    : 'all_links_FT',
                                                                                  'AB_FT_def': 'all_links_FT_def'}),
        right = matched_gdf[['ID', 'AB_FT', 'AB_FT_def']].drop_duplicates().rename(columns = {'AB_FT'    : 'matched_links_FT',
                                                                                              'AB_FT_def': 'matched_links_FT_def'}),
        on    = 'ID',
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, CCTA, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(CCTA))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)

    # TODO: (maybe): conflate ccta_hov_nontruck_links_gdf using --tile-hierarchy setting only for freeway, however, shst lacks documentation
    # on --tile-hierarchy numbering, pending.

    WranglerLogger.info('finished conflating CCTA data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} CCTA GP links.'.format(
        matched_gdf['ID'].nunique(), ccta_gp_links_gdf.shape[0]))


def conflate_ACTC(docker_container_name):
    """
    Conflate ACTC data with sharedstreets
    See ACTC network documentation at Box: https://mtcdrive.box.com/s/ta7mn21vhnemv07ch8irz87vfdgunw2a, in 'Documents -> 19752_Alameda_Countywide_Model_20190404.pdf'.

    Link attributes relevant to conflation:
    * FT                    = facility type
                              1: Freeway to freeway ramp
                              2: Freeway
                              3: Expressway/Highway
                              4: Collector
                              5: Ramp
                              6: Connector link
                              7: Arterial
                              8: Metered ramp
                              9: Special types
    * USE                   = use restrictions
    #                         TODO: the documentation is missing info on 'USE"
    * LN                    = number of lanes in each direction (through lanes only, excluding turn lanes, including auxiliary lane)
    * AUX                   = number of auxiliary lanes
    * NMT[2010/2020/2040]   = bike facility class (added to the network during the 2014-2015 model update)
                              0: No bicycle or pedestrian use (freeways, etc...)
                              1: Bicycles/pedestrians allowed, no special facilities
                              2: Bike lanes (Class II)
                              3: Bike paths (Class I)
                              4: Cycle tracks (Class IV)
    
    Each attribute, except for NMT, has 4 sets of fields, representing a base (2000) condition and up to 3 planned or already implemented improvements.
    For example, LN:
    * BASE_LN       = 2000 number of lanes
    * IMP1_YEAR     = year of 1st round of road/bike improvement if applicable
    * IMP1_LN       = number of lanes corresponding to IMP1_YEAR
    * IMP2_YEAR     = year of 2nd round of road/bike improvement if applicable
    * IMP2_LN       = number of lanes corresponding to IMP2_YEAR
    * IMP3_YEAR     = year of 3rd round of road/bike improvement if applicable
    * IMP3_LN       = number of lanes corresponding to IMP3_YEAR

    NOTE on the attributes:
    --- Will consolidate 2000 value and improvement values into a set of 2015 value.
    --- For improvements, the documentation only provides the LN example, IMP1_LN == 0 means the road no longer exists. However, for other attributes,
        e.g. FT and USE, most links have IMP1_FT/IMP1_USE == 0, which doesn't seem to be a real improvement change, but should be no change instead.
    --- TODO: 'IMP1_YEAR', 'IMP2_YEAR', 'IMP3_YEAR' have value 9999 - what is it? 
    --- LN (lanes): WSP's code for the bi-county modeling project only uses 'BASE_LN'. However, when 'IMP1_YEAR'/'IMP2_YEAR'/'IMP3_YEAR' == 2015,
        corresponding 'IMP1_LN'/'IMP2_LN'/'IMP3_LN' better reflect network status in year 2015.
    --- NMT (bike): WSP's code for the bi-county modeling project used both 'NMT2010' and 'NMT2020'. 
    --- AUX: it appears that the AUX lane data cannot accurately represent merge lanes, mainly because the links are not granular enough, therefore
        a link with AUX=1 could cover a long stretch of road without auxiliary lane. So, will not use 'AUX' attributes for conflation.
    --- The network is a model network, where HOV lane(s) are in their individual links, with different A, B nodes from the corresponding GP link.
        However, since the dataset already excluded links with FT==6, no dummy link is available for consolidating hov and gp links, therefore,
        dropping links other than USE==1 before running shst matching.
        TODO: may consider run shst matching on links with USE != 1 separately using different matching rules, e.g. --tile-hierarchy.

    Outputs:
    -- matched_gdf.feather: ACTC links matched to SharedStreets links under the match command config 
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction".
    -- unmatched_gdf.feather: ACTC links failed to find a match. It retains the fields of the ACTC links before shst match call.

    See methods.conflate() documentation for more detail about fields in the matched and unmatched output.

    The following files were also written out, though not used in later steps.  
    -- ACTC.in.feather: ACTC data before shst matching preparation for QA/QC.
    -- ACTC.in.geojson: ACTC data as input for shst matching. If the whole dataset is too large, the conflation() method splits it into 14
       smaller geographies, each with an '.in.geojson file. 
    -- ACTC.out.matched.geojson: shst matching output for matched links, corresponding to each '.in.geojson file.
    -- ACTC.out.unmatched.geojson: shst matching output for unmatched links, corresponding to each '.in.geojson file.

    """
    WranglerLogger.info('loading ACTC data from {}'.format(THIRD_PARTY_INPUT_FILES[ACTC]))
    actc_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[ACTC])
    WranglerLogger.info('ACTC raw data has {:,} links, {:,} unique A-B combination'.format(
        actc_raw_gdf.shape[0], len(actc_raw_gdf.groupby(['A', 'B']).count())
    ))
    WranglerLogger.debug('ACTC raw data dtypes:\n{}'.format(actc_raw_gdf.dtypes))
    WranglerLogger.debug('ACTC crs:\n{}'.format(actc_raw_gdf.crs))

    # consolidate 2000 attributes and improvements that occurred in 2015 to get 2015 lane count, and drop links with lane==0 in 2015 (many of them have FT==9)
    # Please refer to the 'NOTE on the attributes' part above.
    attrs_to_impute = ['LN', 'FT', 'USE']
    for attr in attrs_to_impute:
        WranglerLogger.debug('imputing 2015 {}'.format(attr))
        # default to 2000 value
        actc_raw_gdf['2015_'+attr] = actc_raw_gdf['BASE_'+attr]

        # if IMP1_YEAR between 2000 and 2015, update to IMP1_attr
        if attr == 'LN':
            IMP1_2015_idx = (actc_raw_gdf['IMP1_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP1_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP1_'+attr] != actc_raw_gdf['2015_'+attr])   # the improvement changed lane count
        elif attr in ['FT', 'USE']:
            IMP1_2015_idx = (actc_raw_gdf['IMP1_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP1_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP1_'+attr] != actc_raw_gdf['2015_'+attr]) & \
                            (actc_raw_gdf['IMP1_'+attr] != 0)                            # the new FT or USE value is not 0 
        WranglerLogger.debug('update {} for {:,} links from IMP1'.format(attr, IMP1_2015_idx.sum()))
        actc_raw_gdf.loc[IMP1_2015_idx, '2015_'+attr] = actc_raw_gdf['IMP1_'+attr]

        # if IMP2_YEAR between 2000 and 2015, update to IMP2_attr
        if attr == 'LN':
            IMP2_2015_idx = (actc_raw_gdf['IMP2_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP2_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP2_'+attr] != actc_raw_gdf['2015_'+attr])
        elif attr in ['FT', 'USE']:
            IMP2_2015_idx = (actc_raw_gdf['IMP2_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP2_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP2_'+attr] != actc_raw_gdf['2015_'+attr]) & \
                            (actc_raw_gdf['IMP2_'+attr] != 0)
        WranglerLogger.debug('update {} for {:,} links from IMP2'.format(attr, IMP2_2015_idx.sum()))
        actc_raw_gdf.loc[IMP2_2015_idx, '2015_'+attr] = actc_raw_gdf['IMP2_'+attr]

        # if IMP1_YEAR between 2000 and 2015, update to IMP3_attr
        if attr == 'LN':
            IMP3_2015_idx = (actc_raw_gdf['IMP3_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP3_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP3_'+attr] != actc_raw_gdf['2015_'+attr])
        elif attr in ['FT', 'USE']:
            IMP3_2015_idx = (actc_raw_gdf['IMP3_YEAR'] <= 2015) & \
                            (actc_raw_gdf['IMP3_YEAR'] > 2000) & \
                            (actc_raw_gdf['IMP3_'+attr] != actc_raw_gdf['2015_'+attr]) & \
                            (actc_raw_gdf['IMP3_'+attr] != 0)        
        WranglerLogger.debug('update {} for {:,} links from IMP3'.format(attr, IMP3_2015_idx.sum()))

        actc_raw_gdf.loc[IMP3_2015_idx, '2015_'+attr] = actc_raw_gdf['IMP3_'+attr]
        WranglerLogger.debug(
            'finished imputing, {} value counts:\n{}'.format('2015_'+attr, actc_raw_gdf['2015_'+attr].value_counts(dropna=False)))

    # drop links with lane==0 in 2015, these are links added post 2015
    actc_gdf = actc_raw_gdf.loc[actc_raw_gdf['2015_LN'] > 0].reset_index(drop=True)
    WranglerLogger.debug('after dropping 2015_LN==0, {:,} links remain'.format(actc_gdf.shape[0]))

    # drop links with FT==6 in 2015
    actc_gdf = actc_gdf.loc[actc_gdf['2015_FT'] != 6].reset_index(drop=True)
    WranglerLogger.debug('after dropping 2015_FT==6, {:,} links remain'.format(actc_gdf.shape[0]))

    # convert FT to readily interpretable definitions
    actc_FT_dict = {
        1: '1-Freeway to freeway ramp',
        2: '2-Freeway',
        3: '3-Expressway/Highway',
        4: '4-Collector',
        5: '5-Ramp',
        6: '6-Connector link',
        7: '7-Arterial',
        8: '8-Metered Ramp',
        9: '9-Special types'}
    actc_gdf['2015_FT_def'] = actc_gdf['2015_FT'].map(actc_FT_dict)
    WranglerLogger.debug('ACTC FT definitions value counts:\n{}'.format(actc_gdf['2015_FT_def'].value_counts(dropna=False)))

    # separate GP links and hov/non-truck links
    actc_gp_links_gdf = actc_gdf.loc[actc_gdf['2015_USE'] == 1].reset_index(drop=True)
    actc_gp_links_gdf.rename(columns = {'2015_LN': '2015_LN_GP'}, inplace=True)
    WranglerLogger.debug('{:,} GP links'.format(actc_gp_links_gdf.shape[0]))
    actc_hov_nontruck_links_gdf = actc_gdf.loc[actc_gdf['2015_USE'].isin([2, 3, 4])].reset_index(drop=True)
    actc_hov_nontruck_links_gdf.rename(columns = {'2015_LN': '2015_LN_nonGP'}, inplace=True)
    WranglerLogger.debug('{:,} HOV/non-truck links'.format(actc_hov_nontruck_links_gdf.shape[0]))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 2.5-3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        ACTC, actc_gp_links_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    # evaluate shst matching performance and write out for QAQC
    # actc_pre_shst_match_gdf_FILE = os.path.join(THIRD_PARTY_MATCHED_DIR, 'ACTC', 'conflation_shst', 'ACTC.in.feather')
    # actc_pre_shst_match_gdf = gpd.read_feather(actc_pre_shst_match_gdf_FILE)
    shst_match_eval = pd.merge(
        left  = actc_gp_links_gdf[['A', 'B', '2015_FT', '2015_FT_def']].rename(columns = {'2015_FT'    : 'all_links_FT',
                                                                                          '2015_FT_def': 'all_links_FT_def'}),
        right = matched_gdf[['A', 'B', '2015_FT', '2015_FT_def']].drop_duplicates().rename(columns = {'2015_FT'    : 'matched_links_FT',
                                                                                                      '2015_FT_def': 'matched_links_FT_def'}),
        on    = ['A', 'B'],
        how   = 'left')
    shst_match_eval_FILE = os.path.join(THIRD_PARTY_OUTPUT_DIR, ACTC, CONFLATION_SHST, 'shst_match_eval_{}.csv'.format(ACTC))
    shst_match_eval.to_csv(shst_match_eval_FILE, index=False)
    
    # TODO (maybe): conflate actc_hov_nontruck_links_gdf using --tile-hierarchy setting only for freeway, however, shst lacks documentation
    # on --tile-hierarchy numbering, pending.

    WranglerLogger.info('finished conflating ACTC data')
    WranglerLogger.info('Sharedstreets matched {:,} out of {:,} ACTC GP links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), actc_gp_links_gdf.shape[0]))


if __name__ == '__main__':
    # do one dataset at a time
    # We could split this up into multiple scripts but I think there's enough in common that it's helpful to see in one script
    parser = argparse.ArgumentParser(description=USAGE)
    parser.add_argument('third_party', choices=[TOMTOM,TM2_NON_MARIN,TM2_MARIN,SFCTA,CCTA,ACTC,PEMS], help='Third party data to conflate')
    parser.add_argument('--docker_container_name', required=False, help='Docker conainer name to use; otherwise, will create a new one.')
    args = parser.parse_args()

    if not os.path.exists(os.path.join(THIRD_PARTY_OUTPUT_DIR, args.third_party)):
        os.makedirs(os.path.join(THIRD_PARTY_OUTPUT_DIR, args.third_party))

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = os.path.join(
        THIRD_PARTY_OUTPUT_DIR, args.third_party, CONFLATION_SHST,
        "step4_conflate_third_party_{}_{}.info.log".format(
            args.third_party, datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    # do this first to error fast
    # set these based on what create_docker_container() will do
    if OUTPUT_DATA_DIR.startswith('E:'):
        output_mount_target = '/usr/e_volume'
    elif OUTPUT_DATA_DIR.startswith('C:/Users/{}'.format(os.environ['USERNAME'])):
        output_mount_target = '/usr/home'
    else:
        WranglerLogger.error('Only USERPROFILE dir and E: are currently supported for OUTPUT_DATA_DIR')
        sys.exit(1)

    WranglerLogger.info(args)

    if args.third_party == TOMTOM:
        conflate_TOMTOM(args.docker_container_name)
    elif args.third_party == TM2_NON_MARIN:
        conflate_TM2_NON_MARIN(args.docker_container_name)
    elif args.third_party == TM2_MARIN:
        conflate_TM2_MARIN(args.docker_container_name)
    elif args.third_party == SFCTA:
        conflcate_SFCTA(args.docker_container_name)
    elif args.third_party == CCTA:
        conflate_CCTA(args.docker_container_name)
    elif args.third_party == ACTC:
        conflate_ACTC(args.docker_container_name)

    WranglerLogger.info('complete')
