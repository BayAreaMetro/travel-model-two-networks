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
    TM2_NON_MARIN   : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_NON_MARIN, 'input', 'mtc_final_network_base.shp'),
    TM2_MARIN       : os.path.join(THIRD_PARTY_INPUT_DIR, TM2_MARIN,     'input', 'mtc_final_network_base.shp'),
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
       "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction". It has three sets of columns: 
            * TomTom link attributes
            * SharedStreets link attributes: 'shstReferenceId', 'shstGeometryId', 'fromIntersectionId', 'toIntersectionId' (to be used as
                                             the merge key to merge into sharedstreets-based link_gdf)
            * shst matching metrics: 'gisReferenceId', 'gisGeometryId', 'gisTotalSegments', 'gisSegmentIndex',
                                     'gisFromIntersectionId', 'gisToIntersectionId', 'startSideOfStreet', 'endSideOfStreet',
                                     'sideOfStreet', 'score', 'matchType'
       In most cases, TomTom links and sharedstreets links do not have one-to-one match. Two situations:
        * One TomTom link matched to multiple sharedstreets links, usually when sharedstreets links break the TomTom link;
          this results in multiple rows, each with its own SharedStreets link attributes, but the same TomTom link attributes. 
        * Multiple TomTom links matched to the same sharedstreets links, usually when the sharedstreets link is more aggregated
          than TomTom links; this also results in multiple rows, but each with its own TomTom link attributes, same SharedStreets link attributes.
       In both cases, the output links follow sharedstreet links' shapes instead of TomTom links' shapes. But the geometries
       represent the matched segments, not the geometries of the entire sharedstreet links.

    -- tomtom_unmatched_gdf.feather: TomTom links failed to find a match. It retains the fields of the TomTom links before shst match call.

    The following files were also written out, though not used in later steps.  
    -- TomTom.in.feather: TomTom data after shst matching preparation for QA/QC.
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

    # finally, filter out rows that have no usable attributes, where usable attributes = NAME, LANES, speed
    tomtom_gdf['no_data'] = False
    tomtom_gdf.loc[ (tomtom_gdf['NAME_strlen'] <= 1) & \
                    (tomtom_gdf['speed'].isnull()) & \
                    (tomtom_gdf['LANES'] == 0), 'no_data'] = True
    WranglerLogger.debug('Filtering out {:,} no_data rows'.format(tomtom_gdf['no_data'].sum()))
    tomtom_gdf = tomtom_gdf.loc[ tomtom_gdf.no_data == False ]
    tomtom_gdf.drop(columns=['no_data'], inplace=True)
    WranglerLogger.debug('Final TomTom dataset for conflation => {:,} rows'.format(len(tomtom_gdf)))

    # convert to ESPG lat-lon
    tomtom_gdf = tomtom_gdf.to_crs(epsg=methods.LAT_LONG_EPSG)
    WranglerLogger.info('converted to projection: ' + str(tomtom_gdf.crs))

    WranglerLogger.info('total {:,} tomtom links'.format(tomtom_gdf.shape[0]))

    (tomtom_matched_gdf, tomtom_unmatched_gdf) = methods.conflate(
        TOMTOM, tomtom_gdf, ['ID','reversed'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    WranglerLogger.debug('TomTom has the following dtypes:\n{}'.format(tomtom_gdf.dtypes))
    WranglerLogger.info('finished conflating TomTom data')

def conflate_TM2_NON_MARIN():
    """
    Conflate TM2 (NonMarin) data with sharedstreets
    TODO: What files are written?
    """
    # Prepare TM2 non-Marin for conflation
    WranglerLogger.info('loading TM2_nonMarin data from {}'.format(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN]))
    tm2_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_NON_MARIN])
    WranglerLogger.debug('TM2_Marin raw data dtypes: \n{}'.format(tm2_link_gdf.dtypes))

    # define initial ESPG
    tm2_link_gdf.crs = "esri:102646"

    # convert to ESPG lat-lon
    tm2_link_gdf = tm2_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_nonMarin link data CNTYPE stats: \n{}'.format(
        tm2_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_link_roadway_gdf = tm2_link_gdf.loc[tm2_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]

    WranglerLogger.info('TM2_nonMarin has {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_link_roadway_gdf.shape[0], len(tm2_link_roadway_gdf.groupby(["A", "B"]).count())))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_NON_MARIN, tm2_link_roadway_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.info('finished conflating TM2_nonMarin data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total TM2_nonMarin Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_link_roadway_gdf.shape[0]))
        

def conflate_TM2_MARIN():
    """
    Conflate ACTC data with sharedstreets.
    See Marin version of the network documentation at http://bayareametro.github.io/travel-model-two/Marin/input/

    Subset the data to only those links that have meaningful data that we want.
    # TODO: evaluate if still need to conflate all third-party datasets given the overlap information, e.g. Marin version
    # of the network contains TomTom info and PEMS info. 

    * NUMLANES   = model number of lanes
    * FRC	     = TomTom functional road class
    * NAME	     = TomTom road name
    * FREEWAY	 = TomTom freeway 
    * TOLLRD	 = TomTom toll road
    * ONEWAY	 = TomTom one way
    * LANES	     = TomTom number of lanes
    * RAMP	     = TomTom exit/entrance ramp
    * RTEDIR	 = TomTom route directional
    * ASSIGNABLE = is link used for assignment
    * CNTYPE	 = link connector type
                   BIKE: bike link
                   CRAIL: commuter rail
                   FERRY: ferry link
                   HRAIL: heavy rail link
                   LRAIL: light rail link
                   MAZ: MAZ connector link
                   PED: ped link
                   TANA: regular network link
                   TAP: TAP link
                   TAZ: TAZ connector link
                   USE: HOV (user class) link
    * TRANSIT	 = is link transit
    * TAP_DRIVE	 = MTC TAP link to parking lot
                   1: true
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
    * PEMSID	 = PEMS ID
    * PEMSLANES	 = PEMS number of lanes
    * TOLLSEG	 = toll segment
    * TOLLBOOTH	 = toll link. Links with values less than 11 are bridge tolls; 11 or above are value tolls
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

    TODO: What files are written?
    """
    # Prepare TM2 Marin for conflation
    WranglerLogger.info('loading TM2_Marin data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[TM2_MARIN])))
    tm2_marin_link_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[TM2_MARIN])
    WranglerLogger.debug('TM2_Marin raw data dtypes: \n{}'.format(tm2_marin_link_gdf.dtypes))

    # define initial ESPG
    tm2_marin_link_gdf.crs = CRS("esri:102646")

    # convert to ESPG lat-lon
    tm2_marin_link_gdf = tm2_marin_link_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(tm2_marin_link_gdf.crs))

    # select only road way links
    WranglerLogger.info('TM2_Marin link data CNTYPE stats: \n{}'.format(
        tm2_marin_link_gdf.CNTYPE.value_counts(dropna=False)))

    tm2_marin_link_roadway_gdf = tm2_marin_link_gdf.loc[
        tm2_marin_link_gdf.CNTYPE.isin(["BIKE", "PED", "TANA"])]
    WranglerLogger.info('TM2_Marin has {:,} roadway links, {:,}  unique A-B combination'.format(
        tm2_marin_link_roadway_gdf.shape[0], len(tm2_marin_link_roadway_gdf.groupby(["A", "B"]).count())))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        TM2_MARIN, tm2_marin_link_roadway_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)    

    WranglerLogger.info('finished conflating TM2_Marin data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total TM2_Marin Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), tm2_marin_link_roadway_gdf.shape[0]))

def conflcate_SFCTA(docker_container_name):
    """
    Conflate ACTC data with sharedstreets.
    See SFCTA Stick network documentation at Box: https://mtcdrive.box.com/s/bqz2snotd4s6ctdsw18mbg74ugm3p16p.

    Subset the data to only those links that have meaningful data that we want, which for now is:
    * ONEWAY     = boolean to determine if link is one-way
    * TOLL       =       int64
    * USE        = use restrictions
                   2 and 3: HOV2 and HOV3
                   9: Transit only
                   4: no trucks
    * FT         = facility type
    * STREETNAME = name of roads
    * TYPE       = ?
    * MTYPE      = ?
    * SPEED           = freeflow speed in mph, based on FT and AT
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
    ? TOLL{AM,MD,PM,EV,EA}_{DA,SR2,SR3} = Toll in 1989 cents for the link in given time period for the given group
    ? VALUETOLL_F                       = value toll flag on link
                                          0: no value toll
                                          1: toll on this link is a value toll 
    ? PASSTHRU = if the value toll doesn't get a 'pass, 1 if it does
    * BIKE_CLASS = type of bicycle facility on route
                   0: None
                   1: Class 1 facility (off-street bike path or cycletrack) or Class 4 facility (protected bike lane or
                      bike lane physically buffered by soft-hit, parking, curb, raised)
                   2: Class 2 facility (conventional bike lane or paint-buffered lane with no physical protection)
                   3: Class 3 facility (sharrow, signed bike route, or bicycle boulevard)

    TODO: What files are written?
    """
    # Prepare SFCTA for conflation
    WranglerLogger.info('loading SFCTA data from {}'.format(os.path.join(THIRD_PARTY_INPUT_FILES[SFCTA])))
    sfcta_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[SFCTA])
    WranglerLogger.debug('SFCTA raw data dtypes: \n{}'.format(sfcta_gdf.dtypes))

    # set initial ESPG
    sfcta_gdf.crs = CRS("EPSG:2227")
    # convert to ESPG lat-lon
    sfcta_gdf = sfcta_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(sfcta_gdf.crs))

    # only conflate SF part of the network
    boundary_4_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary_04.geojson'))
    sfcta_gdf = sfcta_gdf.loc[
        sfcta_gdf.intersects(boundary_4_gdf.geometry.unary_union)]

    # remove "special facility" (FT 6)
    sfcta_gdf = sfcta_gdf.loc[sfcta_gdf.FT != 6]

    WranglerLogger.info('after removing links outside boundary_04 and FT=6, SF network has {:,} links'.format(
        len(sfcta_gdf)))

    # conflate the given dataframe with SharedStreets
    (matched_gdf, unmatched_gdf) = methods.conflate(
        SFCTA, sfcta_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name)

    WranglerLogger.info('finished conflating SFCTA data')


def conflate_CCTA():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?
    """
    # Prepare CCTA for conflation
    WranglerLogger.info('loading CCTA data from {}'.format(THIRD_PARTY_INPUT_FILES[CCTA]))
    ccta_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[CCTA])
    WranglerLogger.debug('CCTA raw data dtypes: \n{}'.format(ccta_raw_gdf.dtypes))
    WranglerLogger.info('CCTA crs:\n{}'.format(ccta_raw_gdf.crs))

    # filter out connectors
    ccta_gdf = ccta_raw_gdf.loc[(ccta_raw_gdf.AB_FT != 6) & (ccta_raw_gdf.BA_FT != 6)]
    WranglerLogger.info('CCTA data has {:,} rows, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # this network is from transcad, for one way streets, dir=1;
    # for two-way streets, there is only one links with dir=0, need to create other direction
    # from shapely.geometry import LineString
    WranglerLogger.debug('creating reversed links')
    two_way_links_gdf = ccta_gdf.loc[ccta_gdf.DIR == 0].copy()
    two_way_links_gdf["geometry"] = two_way_links_gdf.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]),
        axis=1)
    # rename all link attributes for 'AB_' into 'BA_'
    rename_columns = {}
    for colname in [x for x in ccta_gdf.columns if ('AB_' in x)]:
        rename_columns[colname] = colname.replace('AB', 'BA')
    WranglerLogger.debug('renaming columns for reversed links: {}'.format(rename_columns))
    two_way_links_gdf.rename(columns=rename_columns, inplace=True)
    # TODO: why "9000000"? I assume the goal is to exceed the existing largest ID number, so need to be more generic
    two_way_links_gdf['ID'] = two_way_links_gdf['ID'] + 9000000

    ccta_gdf = pd.concat([ccta_gdf, two_way_links_gdf], sort=False, ignore_index=True)
    # double check
    WranglerLogger.info('after creating other direction for two-way roads, ccta data has {:,} links, {:,} unique ID'.format(
        ccta_gdf.shape[0], ccta_gdf.ID.nunique()))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        CCTA, ccta_gdf, ['ID'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.info('finished conflating CCTA data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total CCTA Links.'.format(
        matched_gdf['ID'].nunique(), ccta_gdf.shape[0]))

def conflate_ACTC():
    """
    Conflate ACTC data with sharedstreets
    TODO: What files are written?  Is there documentation on the ACTC input file fields?
    """
    WranglerLogger.info('loading ACTC data from {}'.format(THIRD_PARTY_INPUT_FILES[ACTC]))
    actc_raw_gdf = gpd.read_file(THIRD_PARTY_INPUT_FILES[ACTC])
    WranglerLogger.info('ACTC raw data has {:,} links, {:,} unique A-B combination'.format(
        actc_raw_gdf.shape[0], len(actc_raw_gdf.groupby(['A', 'B']).count())
    ))
    WranglerLogger.debug('ACTC raw data dtypes:\n{}'.format(actc_raw_gdf.dtypes))
    WranglerLogger.debug('ACTC crs:\n{}'.format(actc_raw_gdf.crs))

    # convert to ESPG lat-lon
    actc_raw_gdf = actc_raw_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(actc_raw_gdf.crs))

    # conflate the given dataframe with SharedStreets
    # lmz: this step takes me 2.5-3 hours
    (matched_gdf, unmatched_gdf) = methods.conflate(
        ACTC, actc_raw_gdf, ['A','B'], 'roadway_link',
        THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR)

    WranglerLogger.info('finished conflating ACTC data')
    WranglerLogger.info('Sharedstreets matched {} out of {} total ACTC Links.'.format(
        len(matched_gdf.groupby(['A', 'B']).count()), actc_raw_gdf.shape[0]))

# TODO: def conflate_pums():

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
        THIRD_PARTY_OUTPUT_DIR, args.third_party,
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
        conflate_TM2_NON_MARIN()
    elif args.third_party == TM2_MARIN:
        conflate_TM2_MARIN()
    elif args.third_party == SFCTA:
        conflcate_SFCTA(args.docker_container_name)
    elif args.third_party == CCTA:
        conflate_CCTA()
    elif args.third_party == ACTC:
        conflate_ACTC()

    WranglerLogger.info('complete')
