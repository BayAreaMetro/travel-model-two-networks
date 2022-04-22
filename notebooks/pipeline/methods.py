import errno, glob, math, os, sys
import pandas as pd
import numpy as np
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point, shape, LineString
from scipy.spatial import cKDTree
from network_wrangler import WranglerLogger
import geofeather

# some parameters shared by Pipeline scripts
LAT_LONG_EPSG = 4326

# way (link) tags we want from OpenStreetMap (OSM)
# osmnx defaults are viewable here: https://osmnx.readthedocs.io/en/stable/osmnx.html?highlight=util.config#osmnx.utils.config
# and configurable as useful_tags_way
# These are used in step2_osmnx_extraction.py
OSM_WAY_TAGS = [
    'highway',            # https://wiki.openstreetmap.org/wiki/Key:highway
    'tunnel',             # https://wiki.openstreetmap.org/wiki/Key:tunnel
    'bridge',             # https://wiki.openstreetmap.org/wiki/Key:bridge
    'junction',           # https://wiki.openstreetmap.org/wiki/Key:junction
    'oneway',             # https://wiki.openstreetmap.org/wiki/Key:oneway
    'name',               # https://wiki.openstreetmap.org/wiki/Key:name
    'ref',                # https://wiki.openstreetmap.org/wiki/Key:ref
    'width',              # https://wiki.openstreetmap.org/wiki/Key:width
    'est_width',          # https://wiki.openstreetmap.org/wiki/Key:est_width
    'access',             # https://wiki.openstreetmap.org/wiki/Key:access
    'area',               # https://wiki.openstreetmap.org/wiki/Key:area
    'service',            # https://wiki.openstreetmap.org/wiki/Key:service
    'maxspeed',           # https://wiki.openstreetmap.org/wiki/Key:maxspeed
    # lanes accounting
    'lanes',              # https://wiki.openstreetmap.org/wiki/Key:lanes
    'lanes:backward',     # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:forward',      # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:both_ways',    # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'bus',                # https://wiki.openstreetmap.org/wiki/Key:bus
    'lanes:bus',          # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:forward',  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:backward', # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'hov',                # https://wiki.openstreetmap.org/wiki/Key:hov
    'hov:lanes',          # https://wiki.openstreetmap.org/wiki/Key:hov
    'hov:lanes:forward',  # https://wiki.openstreetmap.org/wiki/Key:hov
    'hov:lanes:backward', # https://wiki.openstreetmap.org/wiki/Key:hov
    'taxi',               # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:taxi',         # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:hov',          # https://wiki.openstreetmap.org/wiki/Key:hov
    'shoulder',           # https://wiki.openstreetmap.org/wiki/Key:shoulder
    'turn',               # https://wiki.openstreetmap.org/wiki/Key:turn
    'turn:lanes',         # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:forward', # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:backward',# https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    # active modes
    'sidewalk',             # https://wiki.openstreetmap.org/wiki/Key:sidewalk
    'cycleway',             # https://wiki.openstreetmap.org/wiki/Key:cycleway
]

def extract_osm_links_from_shst_metadata(shst_gdf):
    """
    Expand each shst extract record into osm ways; the information from this is within the metadata for the row:
    https://github.com/sharedstreets/sharedstreets-ref-system#sharedstreets-osm-metadata

    The returned GeoDataFrame contains the following fields: 
        'nodeIds'           : from SharedStreets OSM Metadata waySections, OSM node IDs as a list of strings (which are ints)
        'wayId'             : from SharedStreets OSM Metadata waySections, OSM way ID as an int
        'roadClass'         : from SharedStreets OSM Metadata waySections; string, I'm guessing it's from the highway tag?  https://wiki.openstreetmap.org/wiki/Key:highway#Roads
        'oneWay'            : from SharedStreets OSM Metadata waySections; boolean, I'm guessing it's from the oneway tag?   https://wiki.openstreetmap.org/wiki/Key:oneway
        'roundabout'        : from SharedStreets OSM Metadata waySections; boolean, I'm guessing it's from the junction tag? https://wiki.openstreetmap.org/wiki/Tag:junction%3Droundabout
        'link'              : from SharedStreets OSM Metadata waySections; boolean, I'm guessing it's from the highway tag?  https://wiki.openstreetmap.org/wiki/Highway_link
        'name'              : from SharedStreets OSM Metadata waySections; string
        'waySections_len'   : from SharedStreets OSM Metadata waySections; number of waySections
        'geometryId'        : from SharedStreets OSM Metadata
        'u','v'             : from SharedStreets OSM Metadata waySections, first and last elements in nodeIds
        'id'                : SharedStreets id of each geometry, equivalent to "geometryId" in SharedStreets OSM Metadata; 32-character hex
        'forwardReferenceId': SharedStreets referenceId of the forward link on the given SharedStreets geometry; 32-character hex
        'backReferenceId'   : SharedStreets referenceId of the backward link on the given SharedStreets geometry if the geometry represents a two-way street; 32-character hex
        'fromIntersectionId': SharedStreets id of the "from" node of the link represented by "forwardReferenceId"; 32-character hex
        'toIntersectionId'  : SharedStreets id of the "to" node of the link represented by "forwardReferenceId"; 32-character hex
                              (for the link represented by "backReferenceId", from/to intersections are reversed)
        'geometry'          : SharedStreets geometry
    """

    # It is fast to iterate through a list
    WranglerLogger.debug("Converting shst_gdf metadata to list")
    metadata_list = shst_gdf['metadata'].tolist()
    WranglerLogger.debug("metadata_list is length {}; first 10 items: {}".format(len(metadata_list), metadata_list[:10]))

    # sharedstreet metadata example: 
    # { 
    #   "gisMetadata": [], 
    #   "geometryId": "7fd0e10cc0a694e96701e99c7c6f4525", 
    #   "osmMetadata": {
    #     "waySections": [ 
    #       {
    #         "nodeIds": ["65324846", "4763953722", "4763953417"], 
    #         "wayId": "255168049", 
    #         "roadClass": "Tertiary", 
    #         "oneWay": false, 
    #         "roundabout": false, 
    #         "link": false, 
    #         "name": ""
    #       }, 
    #       {
    #         "nodeIds": ["4763953417", "65324849"], 
    #         "wayId": "514442927", 
    #         "roadClass": "Tertiary", 
    #         "oneWay": false, 
    #         "roundabout": false, 
    #         "link": false, 
    #         "name": ""
    #       }
    #     ], 
    #     "name": "18th Street"
    #   }
    # }

    # will create a list of dicts to make a dataframe
    # each dict will be an OSM way
    osm_from_shst_link_list = []
    for metadata in metadata_list:
        name = metadata.get('osmMetadata').get('name')
        waySections_len = len(metadata.get('osmMetadata').get('waySections'))
        geometryId = metadata.get('geometryId')
        for osm_way in metadata.get('osmMetadata').get('waySections'):
            osm_dict = osm_way
            osm_dict['name'] = name
            osm_dict['waySections_len'] = waySections_len
            osm_dict['geometryId'] = geometryId
            osm_from_shst_link_list.append(osm_dict)
    WranglerLogger.debug("osm_from_shst_link_list has length {}".format(len(osm_from_shst_link_list)))

    osm_from_shst_link_df = pd.DataFrame.from_records(osm_from_shst_link_list)
    # convert wayId to numeric and waySections_len to int8
    osm_from_shst_link_df["wayId"]           = osm_from_shst_link_df["wayId"].astype(int)
    osm_from_shst_link_df["waySections_len"] = osm_from_shst_link_df["waySections_len"].astype(np.int8)

    WranglerLogger.debug("osm_from_shst_link_df has length {} and dtypes:\n{}".format(len(osm_from_shst_link_df),
        osm_from_shst_link_df.dtypes))
    # link                 bool
    # name               object
    # nodeIds            object
    # oneWay               bool
    # roadClass          object
    # roundabout           bool
    # wayId               int32
    # waySections_len      int8
    # geometryId         object        
    WranglerLogger.debug("osm_from_shst_link_df.head:\n{}".format(osm_from_shst_link_df.head()))

    WranglerLogger.debug("osm_ways_from_shst_df.waySections_len.value_counts():\n{}".format(
        osm_from_shst_link_df.waySections_len.value_counts()))

    # add fields to represent each link's starting_node ("u") and ending_node ("v") from the nodeIds field
    osm_from_shst_link_df['u'] = osm_from_shst_link_df.nodeIds.apply(lambda x: int(x[0]))
    osm_from_shst_link_df['v'] = osm_from_shst_link_df.nodeIds.apply(lambda x: int(x[-1]))

    # add remaining fields from shared streets geodataframe, including geometry, which makes it a GeoDataFrame with the SharedStreets geometries
    osm_from_shst_link_gdf = pd.merge(
        left     = shst_gdf[['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry']],
        right    = osm_from_shst_link_df,
        how      = "left",
        left_on  = "id",
        right_on = "geometryId"
    )

    WranglerLogger.debug("osm_from_shst_link_gdf has length {} and dtypes:\n{}".format(len(osm_from_shst_link_gdf),
                                                                                       osm_from_shst_link_gdf.dtypes))
    # link                      bool
    # name                    object
    # nodeIds                 object
    # oneWay                    bool
    # roadClass               object
    # roundabout                bool
    # wayId                    int32
    # waySections_len           int8
    # geometryId              object
    # u                        int64
    # v                        int64
    # id                      object
    # fromIntersectionId      object
    # toIntersectionId        object
    # forwardReferenceId      object
    # backReferenceId         object
    # geometry              geometry
    WranglerLogger.debug("osm_from_shst_link_gdf.head:\n{}".format(osm_from_shst_link_gdf.head()))
    return osm_from_shst_link_gdf


def merge_osmnx_with_shst(osm_ways_from_shst_gdf, osmnx_link_gdf, OUTPUT_DIR):
    """
    merges link attributes and geometries from OSM extract into ShSt-derived OSM Ways dataframe

    Parameters
    ------------
    osm_ways_from_shst_gdf: osm Ways from shst extracts
    osmnx_link_gdf: osm extract
    OUTPUT_DIR: temporary for writing debug file(s)

    Return
    ------------
    OSMNX GeoDataFrame (including all the tags) merged with OSM ways from SharedStreets metadata
    """
    WranglerLogger.debug(
        "merge_osmnx_attributes_with_shst called with osm_ways_from_shst_gdf (type {}) and osmnx_link_gdf (type {})".format(
            type(osm_ways_from_shst_gdf), type(osmnx_link_gdf)))

    # rename name to make it clear it's from shst metadata
    # and rename "oneWay" to "oneway" (same as in osmnx extracts), so later when merging with osmnx extracts, suffixes
    # will be added to differentiate the source
    osm_ways_from_shst_gdf.rename(columns={"name": "name_shst_metadata",
                                           "oneWay": "oneway"}, inplace=True)

    # OSM way links can be chopped up into many nodes, presumably to give it shape
    # for example, this link has a single osmid but 10 nodes:
    # https://www.openstreetmap.org/way/5149900
    # consolidate these -- we expect all the columns to be the same except for length, u, v, key and the geometry
    osm_way_match_cols = list(osmnx_link_gdf.columns.values)
    for remove_col in ['length', 'u', 'v', 'key', 'geometry']:
        osm_way_match_cols.remove(remove_col)

    # Log some debug info about this
    osmnx_link_gdf['dupes'] = osmnx_link_gdf.duplicated(subset=osm_way_match_cols, keep=False)
    WranglerLogger.debug("duplicates in osmnx_link_gdf based on {}: {} rows; " \
        "head(50):\n{}".format(osm_way_match_cols, osmnx_link_gdf['dupes'].sum(), 
        osmnx_link_gdf.loc[ osmnx_link_gdf['dupes'] == True].head(50)))

    # And consolidate to the each OSM; way we will drop the geometry here so it's a df now. The "geometry" field in the
    # merged "osmnx_shst_gdf" is from sharedstreets, therefore multiple OSM ways derived from one sharedstreet record
    # would have the same geometry.  Retain the length of the OSM way (in meters)
    # Note: I would have liked to use geopandas.dissolve() and keep/aggregate the geometry but I don't think it's possible
    agg_dict = {}
    for col in osm_way_match_cols:
        if col=='osmid': continue # this is our groupby key
        agg_dict[col] = 'first' # these are all the same for each osmid so take the first
    agg_dict['length'] = 'sum' # sum this one
    osmnx_link_df = osmnx_link_gdf.groupby(by=['osmid']).agg(agg_dict).reset_index(drop=False)
    WranglerLogger.debug("After aggregating to osm ways, osmnx_link_df len={}, head():\n{}".format(len(osmnx_link_df), osmnx_link_df.head()))

    # to keep this as a dataframe, call merge with geodataframe as left
    # https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#attribute-joins
    osmnx_shst_gdf = pd.merge(
        left      = osm_ways_from_shst_gdf,
        right     = osmnx_link_df,
        left_on   = 'wayId',
        right_on  = 'osmid',
        how       = 'outer',
        indicator = True,
        suffixes  = ['_shst', '_osmnx']
    )
    # recode indicator to be more clear
    osmnx_shst_gdf['_merge'] = osmnx_shst_gdf['_merge'].cat.rename_categories({
        'both'      : 'both',
        'left_only' : 'shst_only',
        'right_only': 'osmnx_only'
    })

    WranglerLogger.debug("osmnx_shst_gdf type {}, len {}, dtypes:\n{}".format(
        type(osmnx_shst_gdf), len(osmnx_shst_gdf), osmnx_shst_gdf.dtypes
    ))
    WranglerLogger.debug("osmnx_shst_gdf.head():\n{}".format(osmnx_shst_gdf.head()))

    # stats on merge results
    #   - "shst_only" rows: osm ways in the sharedstreets extracts only. I believe they are "private" ways since we
    #     pass network_type='all' rather than 'all_private' to osmnx.graph.graph_from_polygon() in step2.
    #   - "osmnx_only" rows: osm links in the osmnx extracts only, mostly likely roads added to the OSM network after
    #      the sharedstreets network was built. They also have geometry as None.
    WranglerLogger.debug("merge indicator statistics:\n{}".format(osmnx_shst_gdf['_merge'].value_counts()))

    #   Log rows with geometry as None (row count should be the same as 'osmnx_only') and remove
    null_shst_geom_df = osmnx_shst_gdf.loc[pd.isnull(osmnx_shst_gdf.geometry)].copy()
    WranglerLogger.debug("osmnx_shst_gdf has {} rows with null geometry; head:\n{}".format(
        len(null_shst_geom_df), null_shst_geom_df.head()
    ))
    WranglerLogger.debug('null_shst_geom_df._merge.value_counts():\n{}'.format(null_shst_geom_df._merge.value_counts()))
    # temporary(?): drop null shst columns, and add geometry from osmnx extracts, and save them to look at
    null_shst_geom_df.drop(
        columns=['id', 'fromIntersectionId', 'toIntersectionId', 'forwardReferenceId', 'backReferenceId', 'geometry'],
        inplace=True)
    null_shst_geom_gdf = pd.merge(
        left = osmnx_link_gdf[['osmid', 'geometry']],
        right= null_shst_geom_df,
        how  = 'right',
        on   = 'osmid',
    )
    null_shst_geom_gdf.reset_index(drop=True, inplace=True)
    OSMNX_ONLY_DEBUG_FILE = os.path.join(OUTPUT_DIR, 'osmnx_ways_without_shst.feather')
    geofeather.to_geofeather(null_shst_geom_gdf, OSMNX_ONLY_DEBUG_FILE)
    WranglerLogger.debug('Wrote null_osmnx_geom_gdf to {}'.format(OSMNX_ONLY_DEBUG_FILE))

    # remove those rows which didn't correspond to osmnx ways
    osmnx_shst_gdf = osmnx_shst_gdf.loc[pd.notnull(osmnx_shst_gdf.geometry)]
    # double check '_merge' indicator should only have 'both' and 'shst_only', not 'osmnx_only'
    WranglerLogger.debug(
        'Double check _merge indicator - should only have "both" and "shst_only":\n{}'.format(
            osmnx_shst_gdf['_merge'].value_counts()
        ))
    osmnx_shst_gdf.reset_index(drop=True, inplace=True)

    # log oneway_shst and oneway_osm comparison, export to check on the map
    WranglerLogger.debug('Compare oneway_shst with oneway_osmnx:\n{}\n{}\n{}\n{}'.format(
        'oneway_shst value counts', osmnx_shst_gdf.oneway_shst.value_counts(),
        'oneway_osmnx value counts', osmnx_shst_gdf.oneway_osmnx.value_counts()
    ))
    # export rows with different oneway_shst and oneway_osmnx
    # TODO: decide which one is more accurate, and modify function 'add_two_way_osm()' accordingly. Now it uses oneway_shst.
    oneway_diff = osmnx_shst_gdf.loc[(osmnx_shst_gdf.oneway_shst != osmnx_shst_gdf.oneway_osmnx) &
                                     osmnx_shst_gdf.oneway_osmnx.notnull()].copy()
    oneway_diff.reset_index(drop=True, inplace=True)
    ONEWAY_DEBUG_FILE = os.path.join(OUTPUT_DIR, 'shst_osmnx_oneway_diff.feather')
    geofeather.to_geofeather(oneway_diff, ONEWAY_DEBUG_FILE)
    WranglerLogger.debug('Wrote oneway_diff to {}'.format(ONEWAY_DEBUG_FILE))

    return osmnx_shst_gdf


def add_two_way_osm(osmnx_shst_gdf):
    """
    Selects rows that represent two-way links, and adds the reverse direction of that link

    Parameters
    ------------
    osmnx_shst_gdf: osm ways from shst extracts, with links attributes also from OSM extract

    return
    ------------
    complete osm links from shst extraction records
    """
    WranglerLogger.debug("add_two_way_osm with type(osmnx_shst_gdf): {}".format(type(osmnx_shst_gdf)))
    # get two-way links; basing our judgement on what these are using the SharedStreet assesment
    # of oneway because the geometries are from SharedStreets, which combines some link+reverse
    # links into a single bi-directional link sometimes, and we want to create the reverse of those
    reverse_osmnx_shst_gdf = osmnx_shst_gdf.loc[
        (osmnx_shst_gdf.oneway_shst == False) &
        (osmnx_shst_gdf.forwardReferenceId != osmnx_shst_gdf.backReferenceId) &
        (osmnx_shst_gdf.u != osmnx_shst_gdf.v)
        ].copy()

    WranglerLogger.debug('osmnx_shst_gdf has {} two-way OSM ways, which contain {} geometries'.format(
        len(reverse_osmnx_shst_gdf),
        reverse_osmnx_shst_gdf.id.nunique())
    )

    # revert their u, v, forwardReferenceId, backReferenceId to create links of the opposite direction
    reverse_osmnx_shst_gdf.rename(
        columns={
            "u": "v",
            "v": "u",
            "forwardReferenceId": "backReferenceId",
            "backReferenceId": "forwardReferenceId",
            "fromIntersectionId": "toIntersectionId",
            "toIntersectionId": "fromIntersectionId",
        },
        inplace=True,
    )
    # reverse the geometries themselves, enabling offset and arrows to work when this is drawn in GIS
    reverse_osmnx_shst_gdf.reset_index(inplace=True)
    forward_linestrings = reverse_osmnx_shst_gdf['geometry'].tolist()
    WranglerLogger.debug('forward_linestrings len={} type(forward_linestrings[0])={} first 5={}'.format(
        len(forward_linestrings), type(forward_linestrings[0]), forward_linestrings[:5]
    ))
    reverse_linestrings = []
    for forward_linestring in forward_linestrings:
        # forward_linstring is a shapely.geometry.LineString object
        reverse_coordinates = list(forward_linestring.coords)[::-1]
        reverse_linestrings.append(LineString(reverse_coordinates))
    reverse_osmnx_shst_gdf['geometry'] = reverse_linestrings

    # add variable to note that it's a reverse that we've created
    osmnx_shst_gdf["reverse"] = 0
    reverse_osmnx_shst_gdf["reverse"] = 1

    # concatenate the reversed links and the initial links
    link_all_gdf = pd.concat( [osmnx_shst_gdf, reverse_osmnx_shst_gdf], sort=False, ignore_index=True)

    # update "forwardReferenceId" and "backReferenceId": rename the former to shstReferenceId because the link now only
    # represents the 'forward' direction; drop the latter because the opposite link is represented by another row
    link_all_gdf.rename(columns={"forwardReferenceId": "shstReferenceId",
                                 "geometryId": "shstGeometryId"},
                        inplace=True)
    link_all_gdf.drop(columns=["backReferenceId"], inplace=True)

    WranglerLogger.debug(
        'after adding the opposite link of two-way OSM Ways, the ShSt-derived OSM Ways have {} OSM links, {} geometries, {} ShSt references'.format(
            link_all_gdf.shape[0],
            link_all_gdf.shstGeometryId.nunique(),
            link_all_gdf.groupby(["shstReferenceId", "shstGeometryId"]).count().shape[0]
        )
    )

    WranglerLogger.debug('of these links, {} are missing OSM extracts info, due to shst extracts (default tile 181224) containing " \
        "{} osmids that are not included in the latest OSM extracts, e.g. private streets, closed streets.'.format(
            link_all_gdf.loc[link_all_gdf.osmid.isnull()].shape[0],
            link_all_gdf.loc[link_all_gdf.osmid.isnull()].wayId.nunique()
        )
    )
    WranglerLogger.debug("add_two_way_osm returning link_all_gdf with type(link_all_gdf): {}".format(type(link_all_gdf)))
    return link_all_gdf


def consolidate_osm_way_to_shst_link(osm_link):
    """
    if a shst link has more than one osm ways, aggregate info into one, e.g. series([1,2,3]) to cell value [1,2,3]
    
    Parameters
    ----------
    osm_link: ShSt-derived OSM Ways with a number of link attributes
    
    return
    ----------
    ShSt geometry based links with the same link attributes
    
    """
    osm_link_gdf = osm_link.copy()

    GROUPBY_COLS = [
        "shstReferenceId",
        "id",
        "shstGeometryId",
        "fromIntersectionId",
        "toIntersectionId"
    ]
    agg_dict = {"geometry": lambda x: x.iloc[0],
                "u": lambda x: x.iloc[0],
                "v": lambda x: x.iloc[-1],
                "waySections_len": "first"}

    def make_tuple(x):
        """ Agregation function, tuple-izes series with multiple values
        """
        T = tuple(x)
        # if it's a series with one object, e.g. 
        # 0    [2401244716, 2401244713, 2401244712] 
        # dtype: object
        if isinstance(x, pd.Series) and x.dtype == object and len(x) == 1:
            return T

        if len(T) > 1:
            if isinstance(T, (pd.Series, pd.Index, np.ndarray)) and len(T) != 1:
                WranglerLogger.debug("T:{} type:{} len:{}".format(T, type(T), len(T)))
            return T

        if isinstance(T[0], (pd.Series, pd.Index, np.ndarray)) and len(T[0]) != 1:
            WranglerLogger.debug("T[0]:{} type:{} len:{}".format(T[0], type(T[0]), len(T[0])))
            WranglerLogger.debug("=> T:{} type:{} len:{}".format(T, type(T), len(T)))
            WranglerLogger.debug("=> x:{} type:{} len:{}".format(x, type(x), len(x)))
        return T[0]

    # these columns are going to be aggregated by making them into tuples, so convert them to object dtypes
    object_columns = {}
    for c in osm_link_gdf.columns:
        # groupby cols, no need to aggregate these
        if c in GROUPBY_COLS: continue
        # these aggregation methods are already defined
        if c in agg_dict.keys(): continue
        # use make_tuple() for the rest
        agg_dict[c] = make_tuple
        object_columns[c] = object

    WranglerLogger.debug("......start aggregating osm segments to one shst link for forward links")
    forward_link_gdf = osm_link_gdf[osm_link_gdf.reverse_out == 0].copy()

    WranglerLogger.debug('forward_link_gdf.dtypes:\n{}'.format(forward_link_gdf.dtypes))
    WranglerLogger.debug('forward_link_gdf.head:\n{}'.format(forward_link_gdf.head(30)))
    WranglerLogger.debug('agg_dict:{}'.format(agg_dict))

    if len(forward_link_gdf) > 0:
        forward_link_gdf = forward_link_gdf.astype(dtype=object_columns)
        WranglerLogger.debug("converted forward_link_gdf columns for groupby to:\n{}".format(forward_link_gdf.dtypes))
        forward_link_gdf = forward_link_gdf.groupby(GROUPBY_COLS).agg(agg_dict).reset_index()
        forward_link_gdf["forward"] = 1
    else:
        forward_link_gdf = None

    print("......start aggregating osm segments to one shst link for backward links")

    backward_link_gdf = osm_link_gdf[osm_link_gdf.reverse_out == 1].copy()

    if len(backward_link_gdf) > 0:
        agg_dict.update({"u": lambda x: x.iloc[-1],
                         "v": lambda x: x.iloc[0]})

        backward_link_gdf = backward_link_gdf.groupby(
            ["shstReferenceId",
             "id",
             "shstGeometryId",
             "fromIntersectionId",
             "toIntersectionId"]
        ).agg(agg_dict).reset_index()
    else:
        backward_link_gdf = None

    shst_link_gdf = None

    if forward_link_gdf is None:
        print("back")
        shst_link_gdf = backward_link_gdf

    if backward_link_gdf is None:
        print("for")
        shst_link_gdf = forward_link_gdf

    if (forward_link_gdf is not None) and (backward_link_gdf is not None):
        print("all")
        shst_link_gdf = pd.concat([forward_link_gdf, backward_link_gdf],
                                  sort=False,
                                  ignore_index=True)

    shst_link_gdf = gpd.GeoDataFrame(shst_link_gdf,
                                     crs={'init': 'epsg:{}'.format(LAT_LONG_EPSG)})

    return shst_link_gdf


def create_node_gdf(link_gdf):
    """
    create shst node gdf from shst geometry
    
    Paramters
    ---------
    link_gdf:  shst links with osm info
    
    return
    ---------
    shst nodes with osm info
    
    """
    print("-------start creating shst nodes--------")
    # geometry only matches for forward direction
    forward_link_gdf = link_gdf[link_gdf.reverse == 0].copy()

    # create point geometry from shst linestring
    forward_link_gdf["u_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[0]), axis=1)
    forward_link_gdf["v_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[-1]), axis=1)

    # get from points
    point_gdf = forward_link_gdf[["u", "fromIntersectionId", "u_point"]].copy()

    point_gdf.rename(columns={"u": "osm_node_id",
                              "fromIntersectionId": "shst_node_id",
                              "u_point": "geometry"},
                     inplace=True)

    # append to points
    point_gdf = pd.concat([
        point_gdf,
        forward_link_gdf[["v", "toIntersectionId", "v_point"]].rename(columns={"v": "osm_node_id",
                                                                               "toIntersectionId": "shst_node_id",
                                                                               "v_point": "geometry"})],
        sort=False,
        ignore_index=True)

    # drop duplicates
    point_gdf.drop_duplicates(subset=["osm_node_id", "shst_node_id"], inplace=True)

    point_gdf = gpd.GeoDataFrame(point_gdf,
                                 crs={'init': 'epsg:{}'.format(LAT_LONG_EPSG)})

    return point_gdf


def link_df_to_geojson(df, properties):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    geojson = {"type": "FeatureCollection", "features": []}
    for _, row in df.iterrows():
        feature = {"type": "Feature",
                   "properties": {},
                   "geometry": {"type": "LineString",
                                "coordinates": []}}
        feature["geometry"]["coordinates"] = [[x, y] for (x, y) in list(row["geometry"].coords)]
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson


def point_df_to_geojson(df: pd.DataFrame, properties: list):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """

    geojson = {"type": "FeatureCollection", "features": []}
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": []},
        }
        feature["geometry"]["coordinates"] = [row["geometry"].x, row["geometry"].y]
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson


def fill_na(df_na):
    """
    fill str NaN with ""
    fill numeric NaN with 0
    """
    df = df_na.copy()
    num_col = list(df.select_dtypes([np.number]).columns)
    print("numeric columns: ", num_col)
    object_col = list(df.select_dtypes(['object']).columns)
    print("str columns: ", object_col)

    for x in list(df.columns):
        if x in num_col:
            df[x].fillna(0, inplace=True)
        elif x in object_col:
            df[x].fillna("", inplace=True)

    return df


def identify_dead_end_nodes(links):
    """
    iteratively find the dead end in networks
    """

    A_B_df = pd.concat([links,
                        links.rename(columns={"u": "v", "v": "u"})],
                       ignore_index=True,
                       sort=False)

    A_B_df.drop_duplicates(inplace=True)

    A_B_df = A_B_df.groupby(["u"]).count().reset_index()

    single_node_list = A_B_df[A_B_df.v == 1].u.tolist()

    return single_node_list


def read_shst_extract(path, suffix):
    """
    read all shst extraction geojson file
    """
    shst_gdf = pd.DataFrame()

    shst_files = glob.glob(path + "/**/" + suffix, recursive=True)

    # raise an error if no files are found
    if len(shst_files) == 0:
        raise FileNotFoundError(errno.ENOENT, path + "/**/" + suffix)

    WranglerLogger.debug("----------start reading shst extraction data-------------")
    for shst_file in shst_files:
        (dirname, filename) = os.path.split(shst_file)
        WranglerLogger.debug("reading shst extraction data: {}".format(filename))
        new_gdf = geofeather.from_geofeather(shst_file)
        new_gdf['source'] = shst_file
        shst_gdf = pd.concat([shst_gdf, new_gdf],
                             ignore_index=True,
                             sort=False)
    WranglerLogger.debug("----------finished reading shst extraction data-------------")
    WranglerLogger.debug("shst extraction head:{}\n".format(shst_gdf.head(10)))

    return shst_gdf


def highway_attribute_list_to_value(x, highway_to_roadway_dict, roadway_hierarchy_dict):
    """
    clean up osm highway for ShSt links with more than one OSM Ways

    Assumption:
    - if multiple OSM ways of the same SHST link have the same roadway type (converted from 'highway'), use that type
    - if multiple OSM ways of the same SHST link have different roadway type,
      use the type with the smallest "hierarchy" value, i.e. the highest hierarchy,
      For example, a SHST link that contains a "motorway" OSM way and a "footway" OSM way
      would be labeled as "motorway".
    - if missing OSM 'highway' info, use 'roadClass' field which is from shst extraction.

    """
    if type(x.highway) == list:
        value_list = list(set([highway_to_roadway_dict[c] for c in x.highway]))
        if len(value_list) == 1:
            if value_list[0] != "":
                return value_list[0]
            else:
                if type(x.roadClass) == list:
                    return highway_to_roadway_dict[x.roadClass[0].lower()]
                else:
                    return highway_to_roadway_dict[x.roadClass.lower()]

        else:
            ret_val = value_list[0]
            ret_val_level = roadway_hierarchy_dict[ret_val]
            for c in value_list:
                val_level = roadway_hierarchy_dict[c]
                if val_level < ret_val_level:
                    ret_val = c
                    ret_val_level = val_level
                else:
                    continue
            return ret_val
    else:
        if x.highway == "":
            return highway_to_roadway_dict[x.roadClass.lower()]
        else:
            if x.highway not in highway_to_roadway_dict.keys():
                print(x)
            else:
                return highway_to_roadway_dict[x.highway]


def ox_graph(nodes_df, links_df):
    """
        create an osmnx-flavored network graph
        osmnx doesn't like values that are arrays, so remove the variables
        that have arrays.  osmnx also requires that certain variables
        be filled in, so do that too.
        Parameters
        ----------
        nodes_df : GeoDataFrame
        link_df : GeoDataFrame
        Returns
        -------
        networkx multidigraph
    """
    try:
        graph_nodes = nodes_df.drop(
            ["inboundReferenceId", "outboundReferenceId"], axis=1
        )
    except:
        graph_nodes = nodes_df.copy()

    graph_nodes.gdf_name = "network_nodes"
    graph_nodes['id'] = graph_nodes['shst_node_id']

    graph_links = links_df.copy()
    graph_links['id'] = graph_links['shstReferenceId']
    graph_links['key'] = graph_links['shstReferenceId']

    G = ox.gdfs_to_graph(graph_nodes, graph_links)

    return G


def reproject(link, node, epsg):
    """
    reporoject link and node geodataframes
    
    for nodes, update X and Y columns
    
    """

    link = link.to_crs(epsg=epsg)
    node = node.to_crs(epsg=epsg)

    node['X'] = node['geometry'].apply(lambda p: p.x)
    node['Y'] = node['geometry'].apply(lambda p: p.y)

    return link, node


def num_of_drive_loadpoint_per_centroid(existing_drive_cc_df, existing_node_gdf):
    """
    decide number of loading point for drive access per centroid
    
    logic: for drive, find the closest points to the existing loading point
    
    return: 
    dataframe
    for each existing drive loading point, number of new loading point needs to be generated. currently set to 1.
    
    """
    existing_pairs_of_centroid_loadpoint_df = existing_drive_cc_df.groupby(['c', 'non_c']).count().reset_index().drop(
        ['A', 'B'], axis=1)

    existing_num_of_loadpoint_per_c_df = existing_drive_cc_df.groupby(['c', 'non_c']).count().groupby('c').count()[
        ['A']].rename(columns={'A': 'abm_num_load'}).reset_index()

    num_drive_loadpoint_new_near_old = pd.merge(existing_pairs_of_centroid_loadpoint_df,
                                                existing_num_of_loadpoint_per_c_df,
                                                how='left',
                                                on='c')

    num_drive_loadpoint_new_near_old['osm_num_load'] = 1

    num_drive_loadpoint_new_near_old = pd.merge(num_drive_loadpoint_new_near_old,
                                                existing_node_gdf[['N', 'X', 'Y']],
                                                how='left',
                                                left_on='non_c',
                                                right_on='N')
    return num_drive_loadpoint_new_near_old


def num_of_walk_bike_loadpoint_per_centroid(existing_centroid_df):
    """
    decide number of loading point for walk and bike access per centroid
    
    logic: find 5 closest points to centroid
    
    return: 
    dataframe
    for each centroid, number of loading point needs to be generated.
    
    """

    num_loadpoint = existing_centroid_df[['N', 'X', 'Y']].copy()
    num_loadpoint['osm_num_load'] = np.int(5)
    num_loadpoint.rename(columns={'N': 'c'}, inplace=True)

    return num_loadpoint


def find_new_load_point(abm_load_ref_df, all_node):
    """
    find the loading points in osm nodes
    
    input: osm node, loading point reference input
    
    output:  dataframe of pairs of centroid and loading point, with point geometry of loading point
    
    works in epsg = 26915
    
    """

    all_node_gdf = all_node.copy()

    all_node_gdf = all_node_gdf.to_crs(epsg=26915)
    all_node_gdf["X"] = all_node_gdf["geometry"].apply(lambda g: g.x)
    all_node_gdf["Y"] = all_node_gdf["geometry"].apply(lambda g: g.y)

    inventory_node_df = all_node_gdf.copy()
    inventory_node_ref = inventory_node_df[["X", "Y"]].values
    tree_default = cKDTree(inventory_node_ref)

    new_load_point_gdf = gpd.GeoDataFrame()

    for i in range(len(abm_load_ref_df)):

        point = abm_load_ref_df.iloc[i][['X', 'Y']].values
        c_id = abm_load_ref_df.iloc[i]['c']
        n_neigh = abm_load_ref_df.iloc[i]['osm_num_load']

        if "c" in all_node_gdf.columns:
            inventory_node_df = all_node_gdf[all_node_gdf.c == c_id].copy().reset_index()
            if len(inventory_node_df) == 0:
                continue
            else:
                inventory_node_ref = inventory_node_df[["X", "Y"]].values
                tree = cKDTree(inventory_node_ref)

        else:
            inventory_node_df = all_node_gdf.copy()
            tree = tree_default

        dd, ii = tree.query(point, k=n_neigh)
        if n_neigh == 1:
            add_gdf = gpd.GeoDataFrame(
                inventory_node_df[['osm_node_id', "shst_node_id", "model_node_id", 'geometry']].iloc[ii]) \
                .transpose().reset_index(drop=True)
        else:
            add_gdf = gpd.GeoDataFrame(
                inventory_node_df[['osm_node_id', "shst_node_id", "model_node_id", 'geometry']].iloc[ii]) \
                .reset_index(drop=True)
        add_gdf['c'] = int(abm_load_ref_df.iloc[i]['c'])
        if i == 0:
            new_load_point_gdf = add_gdf.copy()

        else:
            new_load_point_gdf = new_load_point_gdf.append(add_gdf, ignore_index=True, sort=False)

    return new_load_point_gdf.rename(columns={'geometry': 'geometry_ld'})


def generate_centroid_connectors(run_type, existing_drive_cc_df, node_gdf, existing_node_df):
    """
    calls function to generate loading point reference table, 
    and calls function to find loading points
    
    build linestring based on pairs of centroid and loading point
    
    return centroid connectors and centroids
    """

    if run_type == 'drive':
        abm_load_ref_df = num_of_drive_loadpoint_per_centroid(existing_drive_cc_df, existing_node_df)
    if (run_type == 'walk') | (run_type == 'bike'):
        abm_load_ref_df = num_of_walk_bike_loadpoint_per_centroid(existing_node_df)

    new_load_point_gdf = find_new_load_point(abm_load_ref_df, node_gdf)

    new_load_point_gdf = pd.merge(new_load_point_gdf,
                                  existing_node_df[['N', 'X', 'Y']],
                                  how='left',
                                  left_on='c',
                                  right_on='N')

    new_load_point_gdf['geometry_c'] = [Point(xy) for xy in zip(new_load_point_gdf['X'], new_load_point_gdf['Y'])]
    new_load_point_gdf.drop(['N', 'X', 'Y'], axis=1, inplace=True)

    # centroid coordinates
    new_centroid_gdf = new_load_point_gdf.copy()[['c', 'geometry_c']]
    new_centroid_gdf.rename(columns={'c': 'model_node_id', 'geometry_c': 'geometry'}, inplace=True)
    new_centroid_gdf.drop_duplicates(['model_node_id'], inplace=True)

    new_centroid_gdf = gpd.GeoDataFrame(new_centroid_gdf)

    # inbound cc
    new_cc_gdf = new_load_point_gdf.copy()
    new_cc_gdf['geometry'] = [LineString(xy) for xy in zip(new_cc_gdf['geometry_ld'], new_cc_gdf['geometry_c'])]

    new_cc_gdf["fromIntersectionId"] = new_cc_gdf['shst_node_id']
    new_cc_gdf["shstGeometryId"] = range(1, 1 + len(new_cc_gdf))
    new_cc_gdf["shstGeometryId"] = new_cc_gdf["shstGeometryId"].apply(lambda x: "cc" + str(x))
    new_cc_gdf["id"] = new_cc_gdf["shstGeometryId"]

    new_cc_gdf = new_cc_gdf.rename(columns={'model_node_id': 'A',
                                            'c': 'B',
                                            "osm_node_id": "u"})

    # remove duplicates
    new_cc_gdf.drop_duplicates(['A', 'B'], inplace=True)

    new_cc_gdf.crs = {'init': 'epsg:26915'}
    new_cc_gdf = new_cc_gdf.to_crs(epsg=4326)
    new_centroid_gdf.crs = {'init': 'epsg:26915'}
    new_centroid_gdf = new_centroid_gdf.to_crs(epsg=4326)

    return new_cc_gdf, new_centroid_gdf


def consolidate_cc(link, node, new_drive_cc, new_walk_cc=pd.DataFrame(), new_bike_cc=pd.DataFrame()):
    """
    consolidates TAZ/MAZ drive concetroid connectors with walk and bike centroid connectors if exist (for MAZ),
    returns a link file and a shape file of all TAZ/MAZ centroid connectors
    """

    link_gdf = link.copy()
    node_gdf = node.copy()
    new_drive_cc_gdf = new_drive_cc.copy()

    if len(new_walk_cc) > 0:
        new_walk_cc_gdf = new_walk_cc.copy()
        new_walk_cc_gdf["walk_access"] = int(1)
    else:
        new_walk_cc_gdf = pd.DataFrame()
    if len(new_bike_cc) > 0:
        new_bike_cc_gdf = new_bike_cc.copy()
        new_bike_cc_gdf["bike_access"] = int(1)
    else:
        new_bike_cc_gdf = pd.DataFrame()

    new_drive_cc_gdf["drive_access"] = int(1)
    new_drive_cc_gdf["walk_access"] = int(0)
    new_drive_cc_gdf["bike_access"] = int(0)

    new_cc_gdf = pd.concat([new_drive_cc_gdf,
                            new_walk_cc_gdf,
                            new_bike_cc_gdf],
                           sort=False,
                           ignore_index=True)

    new_cc_gdf["u"] = new_cc_gdf["u"].astype(np.int64)
    new_cc_gdf["A"] = new_cc_gdf["A"].astype(np.int64)

    new_cc_geometry_gdf = new_cc_gdf[["A", "B", "geometry", "fromIntersectionId", "u"]] \
        .drop_duplicates(subset=["A", "B"]).copy()

    new_cc_geometry_gdf["shstGeometryId"] = range(1, 1 + len(new_cc_geometry_gdf))
    new_cc_geometry_gdf["shstGeometryId"] = new_cc_geometry_gdf["shstGeometryId"].apply(lambda x: "cc" + str(x))
    new_cc_geometry_gdf["id"] = new_cc_geometry_gdf["shstGeometryId"]

    unique_cc_gdf = new_cc_gdf.groupby(["A", "B"]).agg({"drive_access": "max",
                                                        "walk_access": "max",
                                                        "bike_access": "max"}).reset_index()

    unique_cc_gdf = pd.merge(unique_cc_gdf,
                             new_cc_geometry_gdf,
                             how="left",
                             on=["A", "B"])

    # add the other direction
    cc_gdf = pd.concat([unique_cc_gdf,
                        unique_cc_gdf.rename(columns={
                            "A": "B",
                            "B": "A",
                            "u": "v",
                            "fromIntersectionId": "toIntersectionId"})],
                       ignore_index=True,
                       sort=False)

    cc_link_columns_list = ["A", "B", "drive_access", "walk_access", "bike_access",
                            "shstGeometryId", "id", "u", "v", "fromIntersectionId", "toIntersectionId"]
    cc_link_df = cc_gdf[cc_link_columns_list].copy()

    cc_shape_columns_list = ["id", "geometry", "fromIntersectionId", "toIntersectionId"]
    cc_shape_gdf = cc_gdf[cc_shape_columns_list].drop_duplicates(subset=["id"]).copy()

    return cc_link_df, cc_shape_gdf


def project_geometry(geometry, crs=None, to_crs=None, to_latlong=False):
    """
    Project a shapely geometry from its current CRS to another.
    If to_crs is None, project to the UTM CRS for the UTM zone in which the
    geometry's centroid lies. Otherwise project to the CRS defined by to_crs.
    Parameters
    ----------
    geometry : shapely.geometry.Polygon or shapely.geometry.MultiPolygon
        the geometry to project
    crs : dict or string or pyproj.CRS
        the starting CRS of the passed-in geometry. if None, it will be set to
        settings.default_crs
    to_crs : dict or string or pyproj.CRS
        if None, project to UTM zone in which geometry's centroid lies,
        otherwise project to this CRS
    to_latlong : bool
        if True, project to settings.default_crs and ignore to_crs
    Returns
    -------
    geometry_proj, crs : tuple
        the projected geometry and its new CRS
    """
    if crs is None:
        crs = {"init": "epsg:4326"}

    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=crs)
    gdf_proj = project_gdf(gdf, to_crs=to_crs, to_latlong=to_latlong)
    geometry_proj = gdf_proj["geometry"].iloc[0]
    return geometry_proj, gdf_proj.crs


def project_gdf(gdf, to_crs=None, to_latlong=False):
    """
    Project a GeoDataFrame from its current CRS to another.
    If to_crs is None, project to the UTM CRS for the UTM zone in which the
    GeoDataFrame's centroid lies. Otherwise project to the CRS defined by
    to_crs. The simple UTM zone calculation in this function works well for
    most latitudes, but may not work for some extreme northern locations like
    Svalbard or far northern Norway.
    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        the GeoDataFrame to be projected
    to_crs : dict or string or pyproj.CRS
        if None, project to UTM zone in which gdf's centroid lies, otherwise
        project to this CRS
    to_latlong : bool
        if True, project to settings.default_crs and ignore to_crs
    Returns
    -------
    gdf_proj : geopandas.GeoDataFrame
        the projected GeoDataFrame
    """
    if gdf.crs is None or len(gdf) < 1:
        raise ValueError("GeoDataFrame must have a valid CRS and cannot be empty")

    # if to_latlong is True, project the gdf to latlong
    if to_latlong:
        gdf_proj = gdf.to_crs({"init": "epsg:4326"})
        # utils.log(f"Projected GeoDataFrame to {settings.default_crs}")

    # else if to_crs was passed-in, project gdf to this CRS
    elif to_crs is not None:
        gdf_proj = gdf.to_crs(to_crs)
        # utils.log(f"Projected GeoDataFrame to {to_crs}")

    # otherwise, automatically project the gdf to UTM
    else:
        # if CRS.from_user_input(gdf.crs).is_projected:
        #   raise ValueError("Geometry must be unprojected to calculate UTM zone")

        # calculate longitude of centroid of union of all geometries in gdf
        avg_lng = gdf["geometry"].unary_union.centroid.x

        # calculate UTM zone from avg longitude to define CRS to project to
        utm_zone = int(math.floor((avg_lng + 180) / 6.0) + 1)
        utm_crs = f"+proj=utm +zone={utm_zone} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

        # project the GeoDataFrame to the UTM CRS
        gdf_proj = gdf.to_crs(utm_crs)
        # utils.log(f"Projected GeoDataFrame to {gdf_proj.crs}")

    return gdf_proj


def buffer1(polygon):
    buffer_dist = 10
    poly_proj, crs_utm = project_geometry(polygon)
    poly_proj_buff = poly_proj.buffer(buffer_dist)
    poly_buff, _ = project_geometry(poly_proj_buff, crs=crs_utm, to_latlong=True)

    return poly_buff


def buffer2(polygon):
    return polygon.minimum_rotated_rectangle


def getAngle(a, b, c):
    ang = math.degrees(math.atan2(c[1] - b[1], c[0] - b[0]) - math.atan2(a[1] - b[1], a[0] - b[0]))
    return ang + 360 if ang < 0 else ang


def isDuplicate(a, b, zoneUnique):
    length = len(zoneUnique)
    # print("    unique zone unique length {}".format(length))
    for i in range(length):
        # print("           compare {} with zone unique {}".format(a, zoneUnique[i]))
        ang = getAngle(a, b, zoneUnique[i])

        if (ang < 45) | (ang > 315):
            return None

    zoneUnique += [a]


def get_non_near_connectors(all_cc, taz_N_list, maz_N_list, node_two_geometry_id_list):
    all_cc_link_gdf = all_cc.copy()

    all_cc_link_gdf = all_cc_link_gdf[all_cc_link_gdf.B.isin(taz_N_list + maz_N_list)].copy()

    all_cc_link_gdf = all_cc_link_gdf[["A", "B", "id", "geometry"]]

    all_cc_link_gdf["ld_point"] = all_cc_link_gdf["geometry"].apply(lambda x: list(x.coords)[0])
    all_cc_link_gdf["c_point"] = all_cc_link_gdf["geometry"].apply(lambda x: list(x.coords)[1])

    all_cc_link_gdf["ld_point_tuple"] = all_cc_link_gdf["ld_point"].apply(lambda x: tuple(x))

    all_cc_link_gdf["good_point"] = np.where(all_cc_link_gdf.A.isin(node_two_geometry_id_list),
                                             1,
                                             0)

    keep_cc_gdf = pd.DataFrame()

    for c in all_cc_link_gdf.B.unique():

        zone_cc_gdf = all_cc_link_gdf[all_cc_link_gdf.B == c].copy()

        centroid = zone_cc_gdf.c_point.iloc[0]

        # if the zone has less than 4 cc, keep all
        if len(zone_cc_gdf) <= 4:
            keep_cc_gdf = keep_cc_gdf.append(zone_cc_gdf, sort=False, ignore_index=True)

        # if the zone has more than 4 cc
        else:

            zoneUnique = []

            zoneCandidate = zone_cc_gdf["ld_point"].to_list()
            # print("zone candidate {}".format(zoneCandidate))
            for point in zoneCandidate:
                # print("evaluate: {}".format(point))
                if len(zoneUnique) == 0:
                    zoneUnique += [point]
                else:
                    isDuplicate(point, centroid, zoneUnique)
                # print("zone unique {}".format(zoneUnique))
                if len(zoneUnique) == 4:
                    break

            zone_cc_gdf = zone_cc_gdf[zone_cc_gdf.ld_point_tuple.isin([tuple(z) for z in zoneUnique])]

            keep_cc_gdf = keep_cc_gdf.append(zone_cc_gdf, sort=False, ignore_index=True)
            """
            ## if more than 4 good cc, apply non-near method
            if zone_cc_gdf.good_point.sum() > 4:
                
                zone_cc_gdf = zone_cc_gdf[zone_cc_gdf.good_point == 1].copy()
                
                zoneUnique = []
                
                zoneCandidate = zone_cc_gdf["B_point"].to_list()
                #print("zone candidate {}".format(zoneCandidate))
                for point in zoneCandidate:
                    #print("evaluate: {}".format(point))
                    if len(zoneUnique) == 0:
                        zoneUnique += [point]
                    else:
                        isDuplicate(point, centroid, zoneUnique)
                    #print("zone unique {}".format(zoneUnique))
                    if len(zoneUnique) == 4:
                        break
                
                zone_cc_gdf = zone_cc_gdf[zone_cc_gdf.B_point_tuple.isin([tuple(z) for z in zoneUnique])]
                
                keep_cc_gdf = keep_cc_gdf.append(zone_cc_gdf, sort = False, ignore_index = True)
    
            ## if less than 4 good cc, keep good cc, apply non-near to pick additional connectors
            else:
                non_near_zone_cc_gdf = zone_cc_gdf[zone_cc_gdf.good_point == 1].copy()
                
                ## keep good cc, get non near based on good cc
                
                zoneUnique = non_near_zone_cc_gdf["B_point"].to_list()
                
                zoneCandidate = zone_cc_gdf[zone_cc_gdf.good_point == 0]["B_point"].to_list()
                
                for point in zoneCandidate:
                    #print("evaluate: {}".format(point))
                    isDuplicate(point, centroid, zoneUnique)
                    #print("zone unique {}".format(zoneUnique))
                    if len(zoneUnique) == 4:
                        break
                        
                zone_cc_gdf = zone_cc_gdf[zone_cc_gdf.B_point_tuple.isin([tuple(z) for z in zoneUnique])]
                
                keep_cc_gdf = keep_cc_gdf.append(zone_cc_gdf, ignore_index = True)
            """
    return keep_cc_gdf
