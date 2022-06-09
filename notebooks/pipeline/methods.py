import errno, glob, math, os, sys
import pandas as pd
import numpy as np
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point, shape, LineString, Polygon
from shapely.ops import transform
import pyproj
from pyproj import CRS
from scipy.spatial import cKDTree
from functools import partial
from network_wrangler import WranglerLogger
import geofeather

# World Geodetic System 1984 (WGS84) used py GPS (latitude/longitude)
# https://epsg.io/4326
LAT_LONG_EPSG = 4326
# Planar CRS that can be used to measure distances in meters
# NAD83 / UTM zone 15N https://epsg.io/26915
NEAREST_MATCH_EPSG = 26915
# number of polygons used for SharedStreet extraction
# == number of rows in step0 INPUT_POLYGON
# == number of geojson files in step0 OUTPUT_BOUNDARY_DIR
NUM_SHST_BOUNDARIES = 14
# for ShSt docker work
DOCKER_SHST_IMAGE_NAME = 'shst:latest'
# Bay Area Counties
BayArea_COUNTIES = ['San Francisco', 'Santa Clara', 'Sonoma', 'Marin', 'San Mateo',
                    'Contra Costa', 'Solano', 'Napa', 'Alameda']

# way (link) tags we want from OpenStreetMap (OSM)
# osmnx defaults are viewable here: https://osmnx.readthedocs.io/en/stable/osmnx.html?highlight=util.config#osmnx.utils.config
# and configurable as useful_tags_way
# These are used in step2_osmnx_extraction.py
TAG_NUMERIC = 1
TAG_STRING  = 2
OSM_WAY_TAGS = {
    'highway'            : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:highway
    'tunnel'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:tunnel
    'bridge'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:bridge
    'junction'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:junction
    'oneway'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:oneway
    'name'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:name
    'ref'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:ref
    'width'              : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:width
    'est_width'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:est_width
    'access'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:access
    'area'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:area
    'service'            : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:service
    'maxspeed'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:maxspeed
    # lanes accounting
    'lanes'              : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes
    'lanes:backward'     : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:forward'      : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:both_ways'    : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'bus'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:bus
    'lanes:bus'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:forward'  : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:backward' : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'hov'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:hov
    'hov:lanes'          : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:hov
    'taxi'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:hov'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:hov
    'shoulder'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:shoulder
    'turn'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn
    'turn:lanes'         : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:forward' : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:backward': TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    # active modes
    'sidewalk'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:sidewalk
    'cycleway'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:cycleway
}

def docker_path(non_docker_path):
    """
    Simple script to transform a non docker path to a docker path for use with the docker container
    created by create_docker_container(); returns that path

    Supported non_docker_paths are in C:/Users/[USERNAME] or E:
    Raises NotImplementedError otherwise
    """
    # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
    non_docker_path_list = non_docker_path.split(os.path.sep)  # e.g. ['E:','tm2_network_version13']

    if non_docker_path.startswith('E:'):
        output_mount_target = '/usr/e_volume'
        # drop the E: part only
        non_docker_path_list = non_docker_path_list[1:]
    elif non_docker_path.startswith('C:/Users/{}'.format(os.environ['USERNAME'])):
        output_mount_target = '/usr/home'
        # drop the C:/Users/[USERRNAME]
        non_docker_path_list = non_docker_path_list[4:]
    else:
        WranglerLogger.error("docker_path() doesn't support non_docker_path {}".format(non_docker_path))
        raise NotImplementedError

    # prepare the path to cd into (OUTPUT_DATA_DIR) -- [output_mount_target]\[rest of OUTPUT_DATA_DIR]
    non_docker_path_list.insert(0, output_mount_target)
    WranglerLogger.debug('non_docker_path_list: {}'.format(non_docker_path_list))

    LINUX_SEP = '/'
    return LINUX_SEP.join(non_docker_path_list)

def get_docker_container(docker_container_name):
    """
    Attempts to fetch the named docker container.  Returns client and docker container instance.
    Raises an exception on failure.
    """
    import docker
    client = docker.from_env()
    container = client.containers.get(docker_container_name)
    WranglerLogger.info('Docker container named {} found; status: {}'.format(docker_container_name, container.status))
    if container.status != 'running':
        container.restart()
    # note: I have had difficulty reusing a container when the mount fails because my IP address (which is included in the volume) has changed
    return (client, container)

def create_docker_container(mount_e: bool, mount_home: bool):
    """
    Uses docker python package to:
    1) If it doesn't already exist, create docker image from Dockerfile in local directory named DOCKER_SHST_IMAGE_NAME
    2) If mount_e is True, creates mount for E: so that it is accessible at /usr/e_volume
    3) If mount_home is True, creates mount for C:\\Users\\{USERNAME} so that it is acceessible at /usr/home
    3) Starts docker container from the given image with given mounts

    Returns (docker.Client instance,
             running docker.models.containers.Container instance)

    See https://docker-py.readthedocs.io/en/stable/containers.html?highlight=prune#docker.models.containers.ContainerCollection.prune
    """
    import docker
    client = docker.from_env()

    # check if the docker image exists
    shst_image = None
    try:
        shst_image = client.images.get('shst:latest')
        WranglerLogger.info('shst image {} found; skipping docker image build'.format(shst_image))
    except docker.errors.ImageNotFound:
        # if not, create one using the local Dockerfile
        dockerfile_dir = os.path.abspath(os.path.dirname(__file__))
        WranglerLogger.info('Creating image using dockerfile dir {}'.format(dockerfile_dir))
        shst_image = client.images.build(path=dockerfile_dir, tag='shst', rm=True)
        WranglerLogger.info('Created docker image {}'.format(shst_image))

    docker_mounts = []
    if mount_e:
        # check if the docker volume exists
        try:
            E_volume = client.volumes.get('E_volume')
            WranglerLogger.info('E_volume volume {} found; skipping docker volume create'.format(E_volume))
        except docker.errors.NotFound:
            # if not, create one
            # first we need our IP address
            import socket
            hostname = socket.gethostname()
            IPAddr   = socket.gethostbyname(hostname)

            # and the Windows username, password
            import getpass
            username = getpass.getuser()
            password = getpass.getpass(prompt='To create a docker volume for your E drive, please enter your password: ')
            # print('username={} password={}'.format(username,password))

            # create the docker volume
            E_volume = client.volumes.create(
                name        = 'E_volume',
                driver      = 'local',
                driver_opts = {'type'  :'cifs',
                               'device':'//{}/e'.format(IPAddr),
                               'o':'user={},password={},file_mode=0777,dir_mode=0777'.format(username,password)
                              })
            WranglerLogger.info('Created docker volume {}'.format(E_volume))

        e_mount = docker.types.Mount(target='/usr/e_volume', source='E_volume', type='volume')
        docker_mounts.append(e_mount)

    if mount_home:
        # mount Users home dir
        WranglerLogger.info('Mouting C:/Users/{} as /usr/home'.format(os.environ['USERNAME']))
        output_mount_target = '/usr/home'
        home_mount = docker.types.Mount(target=output_mount_target, source=os.environ['USERPROFILE'], type='bind')
        docker_mounts.append(home_mount)


    # docker create
    container = client.containers.create(
        image       = 'shst:latest',
        command     = '/bin/bash',
        tty         = True,
        stdin_open  = True,
        auto_remove = False,
        mounts      = docker_mounts)
    WranglerLogger.info('docker container {} named {} created'.format(container, container.name))
    container.start()
    WranglerLogger.info('docker container {} started; status: '.format(container.name, container.status))

    return (client, container)


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
        waySections_order = 1
        for osm_way in metadata.get('osmMetadata').get('waySections'):
            osm_dict = osm_way
            osm_dict['name'] = name
            osm_dict['waySections_len'] = waySections_len
            osm_dict['waySection_ord'] = waySections_order
            osm_dict['geometryId'] = geometryId
            osm_from_shst_link_list.append(osm_dict)
            waySections_order = waySections_order + 1
    WranglerLogger.debug("osm_from_shst_link_list has length {}".format(len(osm_from_shst_link_list)))

    osm_from_shst_link_df = pd.DataFrame.from_records(osm_from_shst_link_list)
    # convert wayId to numeric and waySections_len to int8
    osm_from_shst_link_df["wayId"]           = osm_from_shst_link_df["wayId"].astype(int)
    osm_from_shst_link_df["waySections_len"] = osm_from_shst_link_df["waySections_len"].astype(np.int8)
    osm_from_shst_link_df["waySection_ord"] = osm_from_shst_link_df["waySection_ord"].astype(np.int8)

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

    # the merge is based on "wayId" in osm_ways_from_shst_gdf and "osmid" in osmnx_link_gdf, so first examine duplicated 'osmid'
    osmnx_link_gdf['osmid_cnt'] = osmnx_link_gdf.groupby(['osmid'])['length'].transform('size')
    WranglerLogger.debug('stats on osmid occurances: {}'.format(osmnx_link_gdf['osmid_cnt'].value_counts()))
    # export some examples with duplicated osmid to check on a map
    chk_osmid_dup_gdf = osmnx_link_gdf.loc[(osmnx_link_gdf['osmid_cnt'] > 1) & \
                                            osmnx_link_gdf['lanes:backward'].notnull() & \
                                            osmnx_link_gdf['lanes:forward'].notnull()].sort_values('osmid')
    chk_osmid_dup_gdf.reset_index(drop=True, inplace=True)
    OSMID_DUP_DEBUG_FILE = os.path.join(OUTPUT_DIR, 'osmnx_osmid_dup.feather')
    geofeather.to_geofeather(chk_osmid_dup_gdf, OSMID_DUP_DEBUG_FILE)
    WranglerLogger.debug('Wrote chk_osmid_dup_gdf to {}'.format(OSMID_DUP_DEBUG_FILE))    
                            
    # Two reasons for duplicated osmid:
    # 1. when osmnx generates a graph, it adds edges in both directions for two-way links, tags the reversed link in the
    # boolean field "reversed", and copies link attributes to both edges. Since our osmnx extraction method already includes 
    # direction-dependent attributes, e.g. "lanes:forward", "lanes:backward", "turn:lanes:forward", "turn:lanes:backward",
    # osm way links with "reversed==False" contain link attributes of reversed links, and are consistent with the direction
    # of osm ways in sharedstreets metadata, therefore, drop reversed osm ways links before merging with shst.
    osmnx_link_gdf = osmnx_link_gdf.loc[osmnx_link_gdf['reversed'] == False]
    osmnx_link_gdf.drop(columns=['osmid_cnt', 'reversed'], inplace=True)

    # 2. OSM way links can be chopped up into many nodes, presumably to give it shape
    # for example, this link has a single osmid but 10 nodes:
    # https://www.openstreetmap.org/way/5149900
    # consolidate these -- we expect all the columns to be the same except for length, u, v, key and the geometry
    osm_way_match_cols = list(osmnx_link_gdf.columns.values)
    for remove_col in ['length', 'u', 'v', 'key', 'geometry']:
        osm_way_match_cols.remove(remove_col)

    # Log some debug info about this
    # commented this out since it's not very useful; shows that only length/geometry/u/v are changing
    # osmnx_link_gdf['dupes'] = osmnx_link_gdf.duplicated(subset=osm_way_match_cols, keep=False)
    # WranglerLogger.debug("duplicates in osmnx_link_gdf based on {}: {} rows; " \
    #    "head(50):\n{}".format(osm_way_match_cols, osmnx_link_gdf['dupes'].sum(), 
    #    osmnx_link_gdf.loc[ osmnx_link_gdf['dupes'] == True].head(50)))

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
    # rename and recode indicator to be more clear
    osmnx_shst_gdf.rename(columns={'_merge':'osmnx_shst_merge'}, inplace=True)
    osmnx_shst_gdf['osmnx_shst_merge'] = osmnx_shst_gdf['osmnx_shst_merge'].cat.rename_categories({
        'both'      : 'both',
        'left_only' : 'shst_only',
        'right_only': 'osmnx_only'
    })

    WranglerLogger.debug("osmnx_shst_gdf type {}, len {:,}, dtypes:\n{}".format(
        type(osmnx_shst_gdf), len(osmnx_shst_gdf), osmnx_shst_gdf.dtypes
    ))
    WranglerLogger.debug("osmnx_shst_gdf.head():\n{}".format(osmnx_shst_gdf.head()))

    # stats on merge results
    #   - "shst_only" rows: osm ways in the sharedstreets extracts only. I believe they are "private" ways since we
    #     pass network_type='all' rather than 'all_private' to osmnx.graph.graph_from_polygon() in step2.
    #   - "osmnx_only" rows: osm links in the osmnx extracts only, mostly likely roads added to the OSM network after
    #      the sharedstreets network was built. They also have geometry as None.
    WranglerLogger.debug("merge indicator statistics:\n{}".format(osmnx_shst_gdf['osmnx_shst_merge'].value_counts()))

    #   Log rows with geometry as None (row count should be the same as 'osmnx_only') and remove
    null_shst_geom_df = osmnx_shst_gdf.loc[pd.isnull(osmnx_shst_gdf.geometry)].copy()
    WranglerLogger.debug("osmnx_shst_gdf has {:,} rows with null geometry; head:\n{}".format(
        len(null_shst_geom_df), null_shst_geom_df.head()
    ))
    WranglerLogger.debug('null_shst_geom_df.osmnx_shst_merge.value_counts():\n{}'.format(null_shst_geom_df.osmnx_shst_merge.value_counts()))
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
    # TODO: This is throwing away OSM data for about 180k links right now (out of 1.1M) because they don't correspond with
    # the wayIds listed in the metadata from SharedStreets, leaving 96k SharedStreets links without OSM data.
    # This is because some of the OSM way IDs have changed since the 2018 snapshot was made for SharedStreets.
    # Rather than throwing away this data, we could try to bring it back by doing a sharedstreet match based on the link geometry
    # between these two link sets.
    osmnx_shst_gdf = osmnx_shst_gdf.loc[pd.notnull(osmnx_shst_gdf.geometry)]
    # double check 'osmnx_shst_merge' indicator should only have 'both' and 'shst_only', not 'osmnx_only'
    WranglerLogger.debug(
        'Double check osmnx_shst_merge indicator - should only have "both" and "shst_only":\n{}'.format(
            osmnx_shst_gdf['osmnx_shst_merge'].value_counts()
        ))
    osmnx_shst_gdf.reset_index(drop=True, inplace=True)

    # (temporary) QAQC links where 'oneway_shst' and 'oneway_osmnx' have discrepancies, export to check on the map
    WranglerLogger.debug('QAQC discrepancy between oneway_shst and oneway_osm:\n{}\n{}\n{}\n{}'.format(
        'oneway_shst value counts', osmnx_shst_gdf.oneway_shst.value_counts(dropna=False),
        'oneway_osmnx value counts', osmnx_shst_gdf.oneway_osmnx.value_counts(dropna=False)
    ))
    oneway_diff = osmnx_shst_gdf.loc[osmnx_shst_gdf.oneway_shst.notnull() & osmnx_shst_gdf.oneway_osmnx.notnull() & (
                                     osmnx_shst_gdf.oneway_shst != osmnx_shst_gdf.oneway_osmnx)]
    oneway_diff.reset_index(drop=True, inplace=True)
    WranglerLogger.debug('export {} links with different oneway_shst and oneway_osm for debugging'.format(
        oneway_diff.shape[0]))
    ONEWAY_DEBUG_FILE = os.path.join(OUTPUT_DIR, 'shst_osmnx_oneway_diff.feather')
    geofeather.to_geofeather(oneway_diff, ONEWAY_DEBUG_FILE)
    WranglerLogger.debug('Wrote oneway_diff to {}'.format(ONEWAY_DEBUG_FILE))

    return osmnx_shst_gdf


def recode_osmnx_highway_tag(osmnx_shst_gdf):
    """"
    OSMnx 'highway' tags have a multitude of values that are too detailed for us;
    Simplify the tag to a new column, 'roadway'
    Additionally, add boolean columns 'drive_access', 'walk_access', 'bike_access' representing
    whether these links have this type of access.
    """
    HIGHWAY_TO_ROADWAY = [
        # highway               # roadway           # hierarchy
        ('bridleway',           'cycleway',         13),
        ('closed:path',         'cycleway',         13),
        ('cycleway',            'cycleway',         13),
        ('other',               'cycleway',         13), # ?
        ('path',                'cycleway',         13),
        ('socail_path',         'cycleway',         13),
        ('track',               'cycleway',         13),
        ('corridor',            'footway',          14),
        ('footpath',            'footway',          14),
        ('footway',             'footway',          14),
        ('pedestrian',          'footway',          14),
        ('steps',               'footway',          14),
        ('motorway',            'motorway',          1),
        ('motorway_link',       'motorway_link',     2),
        ('primary',             'primary',           5),
        ('primary_link',        'primary_link',      6),
        ('access',              'residential',      11),
        ('junction',            'residential',      11),
        ('residential',         'residential',      11),
        ('road',                'residential',      11),
        ('unclassified',        'residential',      11),
        ('unclassified_link',   'residential',      11),
        ('secondary',           'secondary',         7),
        ('secondary_link',      'secondary_link',    8),
        ('busway',              'service',          12),
        ('living_street',       'service',          12),
        ('service',             'service',          12),
        ('tertiary',            'tertiary',          9),
        ('tertiary_link',       'tertiary_link',    10),
        ('traffic_island',      'primary',          12),
        ('trunk',               'trunk',             3),
        ('trunk_link',          'trunk_link',        4),
    ]
    # OSMnx 'highway' tags have a multitude of values that are too detailed for us;
    # Simplify the tag to a new column, 'roadway'
    WranglerLogger.info('4a. Converting OSM highway variable into standard roadway variable')
    highway_to_roadway_df = pd.DataFrame.from_records(HIGHWAY_TO_ROADWAY, columns=['highway','roadway','hierarchy'])
    osmnx_shst_gdf = pd.merge(
        left      = osmnx_shst_gdf, 
        right     = highway_to_roadway_df,
        how       = 'left',
        on        = 'highway',
        indicator = True,
    )
    osmnx_shst_gdf.fillna(value={'roadway':'unknown'}, inplace=True)
    WranglerLogger.debug('osmnx_shst_gdf.dtypes:\n{}'.format(osmnx_shst_gdf.dtypes))
    WranglerLogger.debug('osmnx_shst_gdf[["highway","roadway","_merge"]].value_counts():\n{}'.format(
        osmnx_shst_gdf[['highway','roadway','_merge']].value_counts(dropna=False)))
    osmnx_shst_gdf.drop(columns="_merge", inplace=True)

    ROADWAY_TO_ACCESS = [
        # roadway,          drive_access,   walk_access,    bike_access
        ('cycleway',        False,          True,           True ),
        ('footway',         False,          True,           False),
        ('motorway',        True,           False,          False),
        ('motorway_link',   True,           True,           True ),
        ('primary',         True,           True,           True ),
        ('primary_link',    True,           True,           True ),
        ('residential',     True,           True,           True ),
        ('secondary',       True,           True,           True ),
        ('secondary_link',  True,           True,           True ),
        ('service',         True,           True,           True ),
        ('tertiary',        True,           True,           True ),
        ('tertiary_link',   True,           True,           True ),
        ('trunk',           True,           True,           True ),
        ('trunk_link',      True,           True,           True ),
        ('unknown',         True,           True,           True ), # default to true to err on the side of granting more access
    ]
    # add network type variables "drive_access", "walk_access", "bike_access" based on roadway
    WranglerLogger.info('Adding network type variables "drive_access", "walk_access", "bike_access"')
    network_type_df = pd.DataFrame.from_records(ROADWAY_TO_ACCESS, columns=['roadway','drive_access','walk_access','bike_access'])
    osmnx_shst_gdf = pd.merge(
        left  = osmnx_shst_gdf,
        right = network_type_df,
        how   = 'left',
        on    = 'roadway')
    WranglerLogger.debug('osmnx_shst_gdf.drive_access.value_counts():\n{}'.format(osmnx_shst_gdf.drive_access.value_counts(dropna=False)))
    WranglerLogger.debug('osmnx_shst_gdf.walk_access.value_counts():\n{}'.format(osmnx_shst_gdf.drive_access.value_counts(dropna=False)))
    WranglerLogger.debug('osmnx_shst_gdf.bike_access.value_counts():\n{}'.format(osmnx_shst_gdf.drive_access.value_counts(dropna=False)))

    return osmnx_shst_gdf

def modify_osmway_lane_accounting_field_type(osmnx_shst_gdf):
    """
    For all fields related to lane accounting, convert numeric attributes to field type = numeric, and clean up the
    mixture of None and non (both numeric and string attributes).

    Does not return anything; modifies the passed DataFrame.
    """
    WranglerLogger.info('Clean up fields type for attributes related to lane accounting')

    for col in sorted(OSM_WAY_TAGS.keys()):
        # this one is special and has been renamed to oneway_osmnx and it's a bool already
        if col=='oneway': continue

        if OSM_WAY_TAGS[col] == TAG_NUMERIC:
            osmnx_shst_gdf[col] = pd.to_numeric(osmnx_shst_gdf[col], errors='coerce')
            WranglerLogger.debug('converted {} to numeric, with value_counts:\n{}'.format(col, osmnx_shst_gdf[col].value_counts(dropna=False)))

        elif OSM_WAY_TAGS[col] == TAG_STRING:
            osmnx_shst_gdf[col].fillna('', inplace=True)
            WranglerLogger.debug('fillna for {}, with unique values_counts:\n{}'.format(col, osmnx_shst_gdf[col].value_counts(dropna=False)))


def tag_osm_ways_oneway_twoway(osmnx_shst_gdf):
    """
    Adds column, osm_dir_tag; set to 1 for one-way links and 2 for two-way links

    Does not return anything; modifies the passed DataFrame.
    """
    WranglerLogger.info('Add "osm_dir_tag" to label 2 (for two-way) and 1 (for one-way) OSM ways')

    # default to 1-way
    osmnx_shst_gdf['osm_dir_tag'] = np.int8(1)

    # Label 'two-way' links.
    # Generally speaking, we'll defer to the SharedStreets version of oneway because the link geometry comes from SharedStreets and
    # so it's more accurate generally since the oneway-ness is typically driven by geometry.  For example, in situations where there's
    # a partially divided street, SharedStreets will represent the divided part as two one-way shapes and the undivided part as one 
    # two-way geometry.
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.oneway_shst == False) & 
                       (osmnx_shst_gdf.forwardReferenceId != osmnx_shst_gdf.backReferenceId) & 
                       (osmnx_shst_gdf.u != osmnx_shst_gdf.v), 'osm_dir_tag'] = np.int8(2)
    # However, there are some places where SharedStreets got it wrong, or there were two-way conversions.
    # Having lanes:backward > 1 is a strong signal that the link is actually two way so override here
    # Note: these links are labelled as two-way but without 'backReferenceId', therefore, after adding reverse links
    # for two-way links, they will be missing 'shstReferenceId'.
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.oneway_osmnx == False) & (osmnx_shst_gdf['lanes:backward'] > 0), 'osm_dir_tag'] = np.int8(2)
    
    WranglerLogger.debug('osmnx_shst_gdf has {:,} links: \n{}'.format(
        osmnx_shst_gdf.shape[0], (osmnx_shst_gdf.osm_dir_tag.value_counts())))


def impute_num_lanes_each_direction_from_osm(osmnx_shst_gdf, OUTPUT_DIR):
    """
    In OSM data, 'lanes' represents the total number of lanes of a given road, so for links representing two-way roads,
    lanes = lanes:forward + lanes:backward + lanes:both_ways, with 'lanes:forward' and 'lane:backward' representing lane
    counts of each direction, and 'lanes:both_ways' (1 or None) representing middle turn lane shared by both directions.

    This step:
      - creates additional columns to explicitly represent lane count of each direction
      - impute values when lanes:forward or lanes:backward is missing

    For two-way links, 12 cases were identified based on data availabilities and imputation method. A 'lane_count_type' of
    case type is also added to the link_gdf for QAQC. For cases without sufficient data to impute, skip for now.
    For one-way links, use 'lanes'; if 'lanes' is missing, use 'lanes:forward' if available

    The passed dataframe is returned with 4 additional columns; these may be -1 if unset
    - lane_count_type    = a code indicating what imputation rules were used
    - forward_tot_lanes  = number of total lanes in the forward direction
    - backward_tot_lanes = number of total lanes in the backward direction
    - bothways_tot_lanes = number of total lanes in bothways direction (max 1)
    """

    # let's tally the permutation of numeric lane columns (for drive_access links only)
    osmnx_lane_tag_permutations_df = pd.DataFrame(osmnx_shst_gdf.loc[ osmnx_shst_gdf.drive_access == True]. \
        value_counts(subset=['osm_dir_tag','lanes','lanes:forward','lanes:backward','lanes:both_ways',
                             'forward_bus_lane','backward_bus_lane','forward_hov_lane'], dropna=False)).reset_index(drop=False)
    osmnx_lane_tag_permutations_df.rename(columns={0:'lane_count_type_numrows'},inplace=True)  # the count column is named 0 by default
    # give it a new index and write it
    osmnx_lane_tag_permutations_df['lane_count_type'] = osmnx_lane_tag_permutations_df.index
    WranglerLogger.debug('osmnx_lane_permutations_df:\n{}'.format(osmnx_lane_tag_permutations_df))
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'osmnx_lane_tag_permutations.csv')
    osmnx_lane_tag_permutations_df.to_csv(OUTPUT_FILE, header=True, index=False)
    WranglerLogger.debug('Wrote {}'.format(OUTPUT_FILE))

    # join to the geodataframe and write that
    osmnx_lane_tag_permutations_df['drive_access'] = True
    osmnx_shst_temp_gdf = pd.merge(
        left  = osmnx_shst_gdf,
        right = osmnx_lane_tag_permutations_df,
        on    = ['drive_access','osm_dir_tag','lanes','lanes:forward','lanes:backward','lanes:both_ways','forward_bus_lane','backward_bus_lane','forward_hov_lane'],
        how   = 'left'
    )
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'osmnx_shst_lane_tag_permutations.feather')
    geofeather.to_geofeather(osmnx_shst_temp_gdf, OUTPUT_FILE)
    WranglerLogger.debug('Wrote {}'.format(OUTPUT_FILE))

    # these are the new columns we'll be setting; initialize them now to be the right type.  -1 mean unset
    osmnx_shst_gdf['lane_count_type']      = np.int8(-1) # unset
    osmnx_shst_gdf['forward_tot_lanes']    = np.int8(-1) # total lanes, forward direction
    osmnx_shst_gdf['backward_tot_lanes']   = np.int8(-1) # total lanes, backward direction 
    osmnx_shst_gdf['bothways_tot_lanes']   = np.int8(-1) # total lanes, both directions

    WranglerLogger.info('Impute lanes of each direction for two-way links')
    # CASE 1: links missing 'lanes' but have either 'lanes:backward' or 'lanes:forward' or both; no 'lanes:both_way'
    type1_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.isnull() & \
                ((osmnx_shst_gdf['lanes:forward'].notnull()) | (osmnx_shst_gdf['lanes:backward'].notnull())) & \
                (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type1:\n{}'.format(type1_idx.sum(), osmnx_shst_gdf.loc[type1_idx]))
    # add tag
    osmnx_shst_gdf.loc[type1_idx, 'lane_count_type'] = np.int8(1)

    if type1_idx.sum() > 0:
        # Impute: tot_lanes_forward = lanes:forward, tot_lanes_backward = lanes:backward
        osmnx_shst_gdf.loc[type1_idx, 'forward_tot_lanes' ] = osmnx_shst_gdf['lanes:forward']
        osmnx_shst_gdf.loc[type1_idx, 'backward_tot_lanes'] = osmnx_shst_gdf['lanes:backward']

    # CASE 2: links missing 'lanes' but have either 'lanes:backward' or 'lanes:forward' or both; have 'lanes:both_way'
    type2_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.isnull() & \
                ((osmnx_shst_gdf['lanes:forward'].notnull()) | (osmnx_shst_gdf['lanes:backward'].notnull())) & \
                (osmnx_shst_gdf['lanes:both_ways'].notnull())
    WranglerLogger.debug('{:,} links of type2:\n{}'.format(type2_idx.sum(), osmnx_shst_gdf.loc[type2_idx]))
    # add tag
    osmnx_shst_gdf.loc[type2_idx, 'lane_count_type'] = np.int8(2)

    if type2_idx.sum() > 0:
        # cannot impute
        pass

    # CASE 3: links missing 'lanes', 'lanes:backward' and 'lanes:forward'; no 'lanes:both_way'
    type3_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.isnull() & \
                (osmnx_shst_gdf['lanes:forward'].isnull()) & \
                (osmnx_shst_gdf['lanes:backward'].isnull()) & \
                (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type3:\n{}'.format(type3_idx.sum(), osmnx_shst_gdf.loc[type3_idx]))
    # add tag
    osmnx_shst_gdf.loc[type3_idx, 'lane_count_type'] = np.int8(3)

    if type3_idx.sum() > 0:
        # cannot impute
        pass

    # CASE 4: links missing 'lanes', 'lanes:backward' and 'lanes:forward'; have 'lanes:both_way'
    # do nothing
    type4_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.isnull() & \
                (osmnx_shst_gdf['lanes:forward'].isnull()) & \
                (osmnx_shst_gdf['lanes:backward'].isnull()) & \
                (osmnx_shst_gdf['lanes:both_ways'].notnull())
    WranglerLogger.debug('{:,} links of type4:\n{}'.format(type4_idx.sum(), osmnx_shst_gdf.loc[type4_idx]))
    # add tag
    osmnx_shst_gdf.loc[type4_idx, 'lane_count_type'] = np.int8(4)

    if type4_idx.sum() > 0:
        # cannot impute
        pass

    # CASE 5: links have 'lanes' but are missing either 'lanes:backward' or 'lanes:forward'; no 'lanes:both_way'
    type5_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.notnull() & \
                ((osmnx_shst_gdf['lanes:forward'].isnull() & osmnx_shst_gdf['lanes:backward'].notnull()) |
                 (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].isnull())) & \
                (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type5:\n{}'.format(type5_idx.sum(), osmnx_shst_gdf.loc[type5_idx]))
    # add tag
    osmnx_shst_gdf.loc[type5_idx, 'lane_count_type'] = np.int8(5)

    if type5_idx.sum() > 0:
        # Impute: assign forward/backward lanes with available data, calculate lanes for the missing direction
        # if lanes:forward not missing, lanes:backward is missing
        osmnx_shst_gdf.loc[type5_idx & osmnx_shst_gdf['lanes:forward'].notnull(), 'forward_tot_lanes' ] = osmnx_shst_gdf['lanes:forward']
        osmnx_shst_gdf.loc[type5_idx & osmnx_shst_gdf['lanes:forward'].notnull(), 'backward_tot_lanes'] = osmnx_shst_gdf['lanes'] - osmnx_shst_gdf['lanes:forward']
        # if lanes:backward not missing, lanes:forward is missing
        osmnx_shst_gdf.loc[type5_idx & osmnx_shst_gdf['lanes:backward'].notnull(), 'backward_tot_lanes'] = osmnx_shst_gdf['lanes:backward']
        osmnx_shst_gdf.loc[type5_idx & osmnx_shst_gdf['lanes:backward'].notnull(), 'forward_tot_lanes' ] = osmnx_shst_gdf['lanes'] - osmnx_shst_gdf['lanes:backward']

    # CASE 6: links have 'lanes' but are missing either 'lanes:backward' or 'lanes:forward'; have 'lanes:both_way'
    type6_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.notnull() & \
                ((osmnx_shst_gdf['lanes:forward'].isnull() & osmnx_shst_gdf['lanes:backward'].notnull()) |
                 (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].isnull())) & \
                (osmnx_shst_gdf['lanes:both_ways'].notnull())
    WranglerLogger.debug('{:,} links of type6:\n{}'.format(type6_idx.sum(), osmnx_shst_gdf.loc[type6_idx]))
    # add tag
    osmnx_shst_gdf.loc[type6_idx, 'lane_count_type'] = np.int8(6)

    if type6_idx.sum() > 0:
        # cannot impute
        pass

    # CASE 7: links have 'lanes' but are missing both 'lanes:backward' and 'lanes:forward'; no 'lanes:both_way'
    type7_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                (osmnx_shst_gdf['lanes:forward'].isnull() & osmnx_shst_gdf['lanes:backward'].isnull()) & \
                (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type7:\n{}'.format(type7_idx.sum(), osmnx_shst_gdf.loc[type7_idx]))
    WranglerLogger.debug('lanes count stats:\n{}'.format(osmnx_shst_gdf.loc[type7_idx].lanes.value_counts()))
    # add tag
    osmnx_shst_gdf.loc[type7_idx, 'lane_count_type'] = np.int8(7)

    if type7_idx.sum() > 0:
        # if 'lanes' is an even number, split 'lanes' equally between 'lanes:backward' and 'lanes:forward'
        osmnx_shst_gdf.loc[type7_idx & (osmnx_shst_gdf.lanes % 2 == 0), 'forward_tot_lanes' ] = osmnx_shst_gdf['lanes'] / 2
        osmnx_shst_gdf.loc[type7_idx & (osmnx_shst_gdf.lanes % 2 == 0), 'backward_tot_lanes'] = osmnx_shst_gdf['lanes'] / 2
        # if 'lanes' is an odd number, cannot impute

    # CASE 8: links have 'lanes' but are missing both 'lanes:backward' and 'lanes:forward'; have 'lanes:both_way'
    type8_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.notnull() & \
                (osmnx_shst_gdf['lanes:forward'].isnull() & osmnx_shst_gdf['lanes:backward'].isnull()) & \
                (osmnx_shst_gdf['lanes:both_ways'].notnull())
    WranglerLogger.debug('{:,} links of type8:\n{}'.format(type8_idx.sum(), osmnx_shst_gdf.loc[type8_idx]))
    WranglerLogger.debug('lanes count stats:\n{}'.format(osmnx_shst_gdf.loc[type8_idx].lanes.value_counts()))
    # add tag
    osmnx_shst_gdf.loc[type8_idx, 'lane_count_type'] = np.int8(8)
    if type8_idx.sum() > 0:
        # if 'lanes' is an odd number, plus 1 (bothways_tot_lanes) and then split equally between 'forward' and 'backward'
        osmnx_shst_gdf.loc[type8_idx & (osmnx_shst_gdf.lanes % 2 != 0), 'forward_tot_lanes'] = (osmnx_shst_gdf['lanes']-1)/2
        osmnx_shst_gdf.loc[type8_idx & (osmnx_shst_gdf.lanes % 2 != 0), 'backward_tot_lanes'] = (osmnx_shst_gdf['lanes']-1)/2
        # also, create middle turn lane
        osmnx_shst_gdf.loc[type8_idx & (osmnx_shst_gdf.lanes % 2 != 0), 'bothways_tot_lanes'] = 1
        # if "lanes" is an even number, cannot impute

    # CASE 9: links have 'lanes', 'lanes:backward' and 'lanes:forward', and 'lanes:forward'+'lanes:backward'=='lanes';
    # no 'lanes:both_way'
    type9_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                osmnx_shst_gdf.lanes.notnull() & \
                (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].notnull()) & \
                (osmnx_shst_gdf['lanes:forward'] + osmnx_shst_gdf['lanes:backward'] == osmnx_shst_gdf['lanes']) &  \
                (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type9:\n{}'.format(type9_idx.sum(), osmnx_shst_gdf.loc[type9_idx]))
    # add tag
    osmnx_shst_gdf.loc[type9_idx, 'lane_count_type'] = np.int8(9)

    if type9_idx.sum() > 0:
        # Impute: tot_lanes_forward = lanes:forward, tot_lanes_backward = lanes:backward
        osmnx_shst_gdf.loc[type9_idx, 'forward_tot_lanes']  = osmnx_shst_gdf['lanes:forward']
        osmnx_shst_gdf.loc[type9_idx, 'backward_tot_lanes'] = osmnx_shst_gdf['lanes:backward']

    # CASE 10: links have 'lanes', 'lanes:backward' and 'lanes:forward', no 'lanes:both_way',
    # but 'lanes:forward' + 'lanes:backward' != 'lanes'
    type10_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                 osmnx_shst_gdf.lanes.notnull() & \
                 (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].notnull()) & \
                 (osmnx_shst_gdf['lanes:forward'] + osmnx_shst_gdf['lanes:backward'] != osmnx_shst_gdf['lanes']) & \
                 (osmnx_shst_gdf['lanes:both_ways'].isnull())
    WranglerLogger.debug('{:,} links of type10:\n{}'.format(type10_idx.sum(), osmnx_shst_gdf.loc[type10_idx]))
    # add tag
    osmnx_shst_gdf.loc[type10_idx, 'lane_count_type'] = np.int8(10)

    # cannot impute
    if type10_idx.sum() > 0:
        # cannot impute
        pass

    # CASE 11: links have 'lanes', 'lanes:backward' and 'lanes:forward', have 'lanes:both_way', and lane counts add up
    type11_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                 osmnx_shst_gdf.lanes.notnull() & \
                 (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].notnull()) & \
                 (osmnx_shst_gdf['lanes:both_ways'].notnull()) & \
                 (osmnx_shst_gdf['lanes:forward'] + osmnx_shst_gdf['lanes:backward'] + osmnx_shst_gdf['lanes:both_ways'] == osmnx_shst_gdf['lanes']) # lane counts add up
    WranglerLogger.debug('{:,} links of type11:\n{}'.format(type11_idx.sum(), osmnx_shst_gdf.loc[type11_idx]))
    # add tag
    osmnx_shst_gdf.loc[type11_idx, 'lane_count_type'] = np.int8(11)

    if type11_idx.sum() > 0:
        # Impute: add 1 lane to each direction, and create middle turn lane
        osmnx_shst_gdf.loc[type11_idx, 'forward_tot_lanes' ] = osmnx_shst_gdf['lanes:forward'] + 1
        osmnx_shst_gdf.loc[type11_idx, 'bothways_tot_lanes'] = 1
        osmnx_shst_gdf.loc[type11_idx, 'backward_tot_lanes'] = osmnx_shst_gdf['lanes:backward'] + 1

    # CASE 12: links have 'lanes', 'lanes:backward', 'lanes:forward', 'lanes:both_way', but lane counts don't add up
    type12_idx = (osmnx_shst_gdf['osm_dir_tag'] == 2) & \
                 osmnx_shst_gdf.lanes.notnull() & \
                 (osmnx_shst_gdf['lanes:forward'].notnull() & osmnx_shst_gdf['lanes:backward'].notnull()) & \
                 (osmnx_shst_gdf['lanes:both_ways'].notnull()) & \
                 (osmnx_shst_gdf['lanes:forward'] + osmnx_shst_gdf['lanes:backward'] + osmnx_shst_gdf['lanes:both_ways'] != osmnx_shst_gdf['lanes']) # lane counts don't add up
    WranglerLogger.debug('{:,} links of type12:\n{}'.format(type12_idx.sum(), osmnx_shst_gdf.loc[type12_idx]))
    # add tag
    osmnx_shst_gdf.loc[type12_idx, 'lane_count_type'] = np.int8(12)

    if type12_idx.sum() > 0:
        # cannot impute
        pass

    WranglerLogger.info('Finished imputing lane counts for two-way links, lane counts stats:\n{}\n{}\n{}\n{}'.format(
        'forward_tot_lanes', osmnx_shst_gdf['forward_tot_lanes'].value_counts(dropna=False),
        'backward_tot_lanes', osmnx_shst_gdf['backward_tot_lanes'].value_counts(dropna=False)))

    WranglerLogger.info('Impute lanes for one-way links')
    WranglerLogger.info('osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag==1].value_counts:\n{}'.format(
        osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag==1].value_counts(
            subset=['lanes','lanes:forward','lanes:both_ways'], dropna=False)))

    # when 'lanes' data available
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.osm_dir_tag==1) & \
                        osmnx_shst_gdf.lanes.notnull(), 'forward_tot_lanes'] = osmnx_shst_gdf['lanes']
    # when 'lanes' data is missing, use 'lanes:forward' when available
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.osm_dir_tag==1) & \
                        osmnx_shst_gdf.lanes.isnull() & \
                        osmnx_shst_gdf['lanes:forward'].notnull(),
                'forward_tot_lanes'] = osmnx_shst_gdf['lanes:forward']
    # add tag
    WranglerLogger.info('Finished imputing lane counts for one-way links, lane counts stats:\n{}\n{}'.format(
        'forward_tot_lanes', osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag==1, 'forward_tot_lanes'].value_counts(dropna=False)))

    WranglerLogger.debug('lane_count_type value_counts:\n{}'.format(osmnx_shst_gdf['lane_count_type'].value_counts()))

def count_bus_lanes(osmnx_shst_gdf, OUTPUT_DIR):
    """
    Imputes presence of bus-only lane based on OSM 'bus','lanes:bus','lanes:bus:forward','lanes:bus:backward' attributes.
    Adds numeric columns, 'forward_bus_lane' and 'backward_bus_lane' to the given dataframe.
    """
    WranglerLogger.info('Count bus-only lanes')

    # initialize new columns we'll be setting
    osmnx_shst_gdf['forward_bus_lane' ] = np.int8(-1)  # unset
    osmnx_shst_gdf['backward_bus_lane'] = np.int8(-1)  # unset

    # for any link, bus = 'designated' indicates one bus-only lane
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.bus == 'designated'), 'forward_bus_lane'] = 1

    # if 'bus' is na, but 'lanes:bus' or 'lanes:bus:forward' has value (1), set forward_bus_lane as 1
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.bus == '') & (osmnx_shst_gdf['lanes:bus'] == 1), 'forward_bus_lane'] = 1
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.bus == '') & osmnx_shst_gdf['lanes:bus'].isnull() & (osmnx_shst_gdf['lanes:bus:forward'] == 1), 'forward_bus_lane'] = 1

    # for two-way links, bus = 'designated' indicating one bus-only lane for each direction
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.osm_dir_tag==2) & (osmnx_shst_gdf.bus == 'designated'), 'backward_bus_lane'] = 1

    # if 'bus' is na, but 'lanes:bus:forward' or 'lanes:bus:backward' has value (1), set bus lane for both direction
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.osm_dir_tag==2) & (osmnx_shst_gdf.bus == '') & (osmnx_shst_gdf['lanes:bus:forward' ] == 1), 'forward_bus_lane' ] = 1
    osmnx_shst_gdf.loc[(osmnx_shst_gdf.osm_dir_tag==2) & (osmnx_shst_gdf.bus == '') & (osmnx_shst_gdf['lanes:bus:backward'] == 1), 'backward_bus_lane'] = 1

    # save summary to csv and to log
    bus_lane_permutations_df = pd.DataFrame(osmnx_shst_gdf.value_counts(subset=[
        'drive_access','osm_dir_tag','bus','lanes:bus','lanes:bus:forward','lanes:bus:backward','forward_bus_lane','backward_bus_lane'], dropna=False)).reset_index(drop=False)
    bus_lane_permutations_df.rename(columns={0:'bus_count_type_numrows'},inplace=True)  # the count column is named 0 by default
    WranglerLogger.debug('bus_lane_permutations_df:\n{}'.format(bus_lane_permutations_df))
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'osmnx_bus_permutations.csv')
    bus_lane_permutations_df.to_csv(OUTPUT_FILE, header=True, index=False)
    WranglerLogger.debug('Wrote {}'.format(OUTPUT_FILE))

    WranglerLogger.debug('Bus lane imputation summary: \n{}'.format(bus_lane_permutations_df))


def count_hov_lanes(osmnx_shst_gdf):
    """
    Add hov-only lane based on OSM attributes 'hov:lanes' and 'hov'.
    If 'hov:lanes' available (e.g. 'designated|yes|yes'), count the occurrence of 'designated' or 'lane' in the string;
    if 'hov' not available, use 'hov': if hov = 'designated' or 'lane', set 1 hov-only lane.

    Does not return anything; modifies the passed DataFrame by adding the column 'forward_hov_lane'
    """
    WranglerLogger.info('Count hov-only lanes')

    # initialize new columns we'll be setting
    # it appears that in our data, 'hov:lanes' and 'hov' are only available for one-way links
    WranglerLogger.debug('one-way or two-way stats for links with hov info:\n{}'.format(
        osmnx_shst_gdf[['drive_access','osm_dir_tag','hov:lanes','hov']].value_counts(dropna=False)
    ))
    osmnx_shst_gdf['forward_hov_lane'] = np.int8(-1)  # unset; two-way links will have 'NaN' in hov lane count, whereas
                                                      # one-way links with no hov info will have '-1' in hov lane count.

    # count occurrences of 'designated' or 'lane' in 'hov:lanes'
    osmnx_shst_gdf['cnt_occur'] = osmnx_shst_gdf['hov:lanes'].apply(lambda x: x.count('designated') + x.count('lane'))
    # set 'forward_hov_lane' for one-way links with 'hov:lanes' info
    osmnx_shst_gdf.loc[(osmnx_shst_gdf['hov:lanes'] != ''),'forward_hov_lane'] = osmnx_shst_gdf['cnt_occur']
    # when 'hov:lanes' is missing, use 'hov'
    osmnx_shst_gdf.loc[(osmnx_shst_gdf['hov:lanes'] == '') & \
                       ((osmnx_shst_gdf['hov'] == 'designated') | (osmnx_shst_gdf['hov'] == 'lane')),
                       'forward_hov_lane'] = 1
    # drop 'cnt_occur'
    osmnx_shst_gdf.drop(columns=['cnt_occur'], inplace=True)

    WranglerLogger.debug('HOV lane imputation summary: \n{}'.format(
       osmnx_shst_gdf[['drive_access','osm_dir_tag','hov:lanes','hov','forward_hov_lane']].value_counts(dropna=False)
    ))

def reverse_geometry(to_reverse_gdf):
    """
    Reverses the geometry of the given LineString GeoDataFrame and returns it as an array of LineStrings
    """
    geom_types = to_reverse_gdf.geom_type.value_counts(dropna=False)  # this is a series
    WranglerLogger.debug('reverse_geometry: to_reverse_gdf.geom_types:\n{}'.format(geom_types))

    # for MultiLineString, do special processing
    # https://shapely.readthedocs.io/en/stable/manual.html?highlight=MultiLineString#MultiLineString
    if 'MultiLineString' in geom_types.index.values:
        # if they're all MultiLineStrings

        # geometry is a MultiLineString, geoms is a list of LineString instances
        # Get lengths (e.g. number of LineStrings per MultiLineString)
        to_reverse_gdf['multilinestring length'] = -1
        to_reverse_gdf.loc[ to_reverse_gdf.geom_type == 'MultiLineString', 'multilinestring length'] = to_reverse_gdf['geometry'].apply(lambda mls: len(mls.geoms))
        multlinestring_lengths_value_counts = to_reverse_gdf['multilinestring length'].value_counts(dropna=False)
        WranglerLogger.debug('reverse_geometry: MultiLineString lengths:\n{}'.format(multlinestring_lengths_value_counts))

        # handle MultiLineString with count of 1 LineString by converting to LineString
        to_reverse_gdf.loc[ (to_reverse_gdf.geom_type == 'MultiLineString') & \
                            (to_reverse_gdf['multilinestring length'] == 1), 
                            'geometry'] = to_reverse_gdf['geometry'].apply(lambda mls: list(mls.geoms)[0])

        geom_types = to_reverse_gdf.geom_type.value_counts(dropna=False)  # this is a series
        WranglerLogger.debug('reverse_geometry: to_reverse_gdf.geom_types:\n{}'.format(geom_types))
    
    if ('LineString' in geom_types.index.values) and (geom_types['LineString'] == len(to_reverse_gdf)):
        # all geometries are single line strings now
        pass
    else:
        # todo: handle remaining MultiLineStrings or other types of geometries
        raise NotImplementedError
    
    # reverse the geometries themselves, enabling offset and arrows to work when this is drawn in GIS
    forward_linestrings = to_reverse_gdf['geometry'].tolist()
    WranglerLogger.debug('reverse_geometry: forward_linestrings len={} type(forward_linestrings[0])={} first 5={}'.format(
        len(forward_linestrings), type(forward_linestrings[0]), forward_linestrings[:5]
    ))
    reverse_linestrings = []
    for forward_linestring in forward_linestrings:
        # forward_linstring is a shapely.geometry.LineString object
        reverse_coordinates = list(forward_linestring.coords)[::-1]
        reverse_linestrings.append(LineString(reverse_coordinates))
    return reverse_linestrings

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
    WranglerLogger.debug('osmnx_shst_gdf has {:,} links: \n{}'.format(osmnx_shst_gdf.shape[0], (osmnx_shst_gdf.osm_dir_tag.value_counts())))
    # get two-way links; basing our judgement on what these are using the SharedStreet assesment
    # of oneway because the geometries are from SharedStreets, which combines some link+reverse
    # links into a single bi-directional link sometimes, and we want to create the reverse of those
    reverse_osmnx_shst_gdf = osmnx_shst_gdf.loc[osmnx_shst_gdf['osm_dir_tag'] == 2].copy()

    WranglerLogger.debug('osmnx_shst_gdf has {:,} two-way OSM ways, which contain {:,} geometries'.format(
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
    reverse_osmnx_shst_gdf['geometry'] = reverse_geometry(reverse_osmnx_shst_gdf)

    # also reverse nodeIds
    forward_nodeIds = reverse_osmnx_shst_gdf['nodeIds'].tolist()
    WranglerLogger.debug('forward_nodeIds len={} type(forward_nodeIds[0])={} first 5={}'.format(
        len(forward_nodeIds), type(forward_nodeIds[0]), forward_nodeIds[:5]
    ))
    reverse_nodeIds = []
    for forward_nodes in forward_nodeIds:
        # forward_linstring is a shapely.geometry.LineString object
        reverse_nodes = list(forward_nodes)[::-1]
        reverse_nodeIds.append(reverse_nodes)
    reverse_osmnx_shst_gdf['nodeIds'] = reverse_nodeIds

    # add variables to represent imputed lanes for each direction and turns for each direction
    # for reversed osm links, use 'backward_tot_lanes', 'turn:lanes:backward', 'backward_bus_lane', 'bothways_tot_lanes'
    reverse_osmnx_shst_gdf['lanes_osmSplit'          ] = reverse_osmnx_shst_gdf['backward_tot_lanes']
    reverse_osmnx_shst_gdf['turns:lanes_osmSplit'    ] = reverse_osmnx_shst_gdf['turn:lanes:backward']
    reverse_osmnx_shst_gdf['busOnly_lane_osmSplit'   ] = reverse_osmnx_shst_gdf['backward_bus_lane']
    reverse_osmnx_shst_gdf['bothways_lane_osmSplit'  ] = reverse_osmnx_shst_gdf['bothways_tot_lanes'] # x 0.5 ?

    # for the initial rows for two-way links, use 'forward_tot_lanes', 'turn:lanes:forward', 'forward_bus_lane', 'bothways_tot_lanes'
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 2, 'lanes_osmSplit'          ] = osmnx_shst_gdf['forward_tot_lanes']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 2, 'turns:lanes_osmSplit'    ] = osmnx_shst_gdf['turn:lanes:forward']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 2, 'busOnly_lane_osmSplit'   ] = osmnx_shst_gdf['forward_bus_lane']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 2, 'bothways_lane_osmSplit'  ] = osmnx_shst_gdf['bothways_tot_lanes'] # x 0.5?

    # for one-way links, use 'forward_tot_lanes', 'turn:lanes', 'forward_hov_lane', 'forward_bus_lane'
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 1, 'lanes_osmSplit'          ] = osmnx_shst_gdf['forward_tot_lanes']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 1, 'turns:lanes_osmSplit'    ] = osmnx_shst_gdf['turn:lanes']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 1, 'busOnly_lane_osmSplit'   ] = osmnx_shst_gdf['forward_bus_lane']
    osmnx_shst_gdf.loc[osmnx_shst_gdf.osm_dir_tag == 1, 'hov_lane_osmSplit'       ] = osmnx_shst_gdf['forward_hov_lane']

    # add variable to note that it's a reverse that we've created
    osmnx_shst_gdf["reverse"] = False
    reverse_osmnx_shst_gdf["reverse"] = True

    # concatenate the reversed links and the initial links
    link_all_gdf = pd.concat( [osmnx_shst_gdf, reverse_osmnx_shst_gdf], sort=False, ignore_index=True)

    # update "forwardReferenceId" and "backReferenceId": rename the former to shstReferenceId because the link now only
    # represents the 'forward' direction; drop the latter because the opposite link is represented by another row
    link_all_gdf.rename(columns={"forwardReferenceId": "shstReferenceId",
                                 "geometryId": "shstGeometryId"},
                        inplace=True)
    link_all_gdf.drop(columns=["backReferenceId"], inplace=True)

    WranglerLogger.debug(
        'after adding the opposite link of two-way OSM Ways, the ShSt-derived OSM Ways have {:,} OSM links, {:,} geometries, {:,} ShSt references'.format(
            link_all_gdf.shape[0],
            link_all_gdf.shstGeometryId.nunique(),
            link_all_gdf.groupby(["shstReferenceId", "shstGeometryId"]).count().shape[0]
        )
    )

    WranglerLogger.debug('of these links, {:,} are missing OSM extracts info, due to shst extracts (default tile 181224) containing ' \
        '{:,} osmids that are not included in the latest OSM extracts, e.g. private streets, closed streets.'.format(
            link_all_gdf.loc[link_all_gdf.osmid.isnull()].shape[0],
            link_all_gdf.loc[link_all_gdf.osmid.isnull()].wayId.nunique()
        )
    )
    WranglerLogger.debug("add_two_way_osm returning link_all_gdf with type(link_all_gdf): {}".format(type(link_all_gdf)))
    return link_all_gdf


def cleanup_turns_attributes(osmnx_shst_gdf):
    """
    Cleans up values from OSM extract's turn-related attributes:
        - typos:
            "throught" -> "through"
            "revese" -> "reverse"
            "revesre" -> "reverse"
            "mege" -> "merge"
            "sligth" -> "slight"
            "3"      -> "through"
        - non-turn values: 'none' or (empty) can be used for lanes with no turn indication (e.g. '||right' or
          'none|none|right' represents a 3-lane road (one-way or two-way), with '|' as the lane divider mark).
          Replace 'none' or (empty) with 'non_turn', so 'non_turn|non_turn|right'.

    Since this function is run after add_two_way_osm(), only need to clean up field 'turns:lanes_osmSplit'.
    If run before that step, need to clean up 'turn:lanes', 'turn:lanes:forward', 'turn:lanes:backward'.

    Does not return anything; modifies the passed DataFrame by updating the column, 'turns:lanes_osmSplit'

    """
    WranglerLogger.info('Clean up turn-related attributes')
    WranglerLogger.debug('osmnx_shst_gdf["turns:lanes_osmSplit"].value_counts():\n{}'.format(
        osmnx_shst_gdf['turns:lanes_osmSplit'].value_counts(dropna=False)))

    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('throught', 'through'))
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('revese', 'reverse'))
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('revesre', 'reverse'))
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('mege', 'merge'))
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('sligth', 'slight'))
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('3', 'through'))

    WranglerLogger.info('...clean up non-turn values')
    # first, replace 'none' with 'non_turn'
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(
        lambda x: x.replace('none', 'non_turn'))
    # second, replace (empty) with 'non_turn'
    osmnx_shst_gdf['turns:lanes_osmSplit'] = osmnx_shst_gdf['turns:lanes_osmSplit'].apply(lambda x: _fill_non_turn(x))

    WranglerLogger.info('...completed turns attributes cleanup.')
    WranglerLogger.debug('osmnx_shst_gdf["turns:lanes_osmSplit"].value_counts():\n{}'.format(
        osmnx_shst_gdf['turns:lanes_osmSplit'].value_counts(dropna=False)))

def _fill_non_turn(turn_str):
    """
    cleans up strings in OSM extract's turns:lanes-related attributes.
    """
    # if not turn lanes value, pass
    if len(turn_str) == 0:
        pass
    else:
        # fill in all (empty) between each pair of lane divider marks
        while '||' in turn_str:
            turn_str = turn_str.replace('||', '|non_turn|')
        # fill in (empty) of the first lane
        if turn_str[0] == '|':
            turn_str = 'non_turn' + turn_str
        # fill in (empty) of the last lane
        if turn_str[-1] == '|':
            turn_str = turn_str + 'non_turn'
    return turn_str


def turn_lane_accounting(osmnx_shst_gdf, OUTPUT_DIR):
    """
    Generate lane counts by lane's turn type based on OSM attributes related to turn lanes.

    Adds columns:
    - through_turn          = count of lanes where through movement or left turn movement is allowed
    - merge_only            = count of lanes which are merging into the other lanes (e.g. they'll disappear)
    - through_only          = count of lanes which are through only
    - turn_only             = count of lanes which are turn only
    - lane_count_from_turns = sum of the above (does not include middle_turn)
    - middle_turn           = count of middle turn (e.g. both way) lanes 
    """
    WranglerLogger.info('Turn lane accounting')

    # get all links with 'turns:lanes_osmSplit'
    link_with_turns = osmnx_shst_gdf.loc[osmnx_shst_gdf['turns:lanes_osmSplit'] != ''].reset_index(drop=True)
    WranglerLogger.info('{:,} links have turn info'.format(link_with_turns.shape[0]))

    # convert lane values (e.g. 'through|right') into a list (e.g. ['through', 'right'])
    link_with_turns['turns_list'] = link_with_turns['turns:lanes_osmSplit'].apply(lambda x: x.split('|'))

    # get a list of all available 'turn' values from the OSMnx data, primarily for debug purposes
    turn_values_list_raw = [item for sublist in list(link_with_turns['turns_list']) for item in sublist]
    turn_values_list = list(set(turn_values_list_raw))
    WranglerLogger.debug('OSMnx data has the following values to represent through/turn movements: {}'.format(sorted(turn_values_list)))

    # turn value recode crosswalk to simplify turn values
    # these refer to a single lane
    turn_recode_simple_dict = {
        'designated'                : 'turn_only',
        'left'                      : 'turn_only',
        'left;left;right'           : 'turn_only',
        'left;merge_to_left'        : 'turn_only',
        'left;right'                : 'turn_only',
        'left;slight_left'          : 'turn_only',
        'left;slight_left;through'  : 'through_turn',
        'left;slight_right'         : 'turn_only',
        'left;through'              : 'through_turn',
        'left;through;right'        : 'through_turn',
        'merge_to_left'             : 'merge_only',
        'merge_to_left;right'       : 'turn_only',
        'merge_to_left;slight_right': 'turn_only',
        'merge_to_right'            : 'merge_only',
        'non_turn;slight_right'     : 'turn_only',
        'reverse'                   : 'turn_only',
        'reverse;left'              : 'turn_only',
        'reverse;through'           : 'through_turn',
        'right'                     : 'turn_only',
        'right;through'             : 'through_turn',
        'sharp_left'                : 'turn_only',
        'sharp_left;left'           : 'turn_only',
        'slight_left'               : 'turn_only',
        'slight_left;left'          : 'turn_only',
        'slight_left;merge_to_left' : 'turn_only',
        'slight_left;right'         : 'turn_only',
        'slight_left;slight_right'  : 'turn_only',
        'slight_left;through'       : 'through_turn',
        'slight_right'              : 'turn_only',
        'slight_right;merge_to_left': 'turn_only',
        'slight_right;right'        : 'turn_only',
        'through'                   : 'through_only',
        'through;left'              : 'through_turn',
        'through;right'             : 'through_turn',
        'through;slight_right'      : 'through_turn',
        'non_turn'                  : 'through_only',
        '3'                         : 'through_only'
    }
    # debug step: ensure all turn values in OSMnx are included in the recode crosswalk
    for turn_value in turn_values_list:
        if turn_value not in turn_recode_simple_dict:
            WranglerLogger.debug('add {} to turn_recode_simple_dict'.format(turn_value))

    # get a list of simplified turn values
    recoded_turn_values_list = list(set(list(turn_recode_simple_dict.values())))
    WranglerLogger.debug('OSMnx turn values are recoded to: {}'.format(recoded_turn_values_list))

    # convert turn values into lane count by recoded turn type
    def _count_lanes_by_turn_type(turns_list):
        """
        A function to first recode the raw turn value based on 'turn_recode_simple_dict',
        then count lanes by turn type and save the result in a dictionary with turn type as keys and lane count as value
        e.g. [left, non_turn, non_turn] -> {'through_turn': 0,
                                            'turn_only': 1,
                                            'through_only': 2,
                                            'merge_only': 0}
        """
        # recode turn values
        turns_list_recode = [turn_recode_simple_dict.get(item) for item in turns_list]
        # count lanes and save in a dict
        turn_counts = dict()
        for recoded_turn_value in recoded_turn_values_list:
            turn_counts[recoded_turn_value] = turns_list_recode.count(recoded_turn_value)
        return turn_counts

    # apply the function to each row
    link_with_turns['turns_dict'] = link_with_turns['turns_list'].apply(lambda x: _count_lanes_by_turn_type(x))
    WranglerLogger.debug("link_with_turns.head(50):\n{}".format(link_with_turns.head(50)))

    # explode the dictionary into multiple columns with turn types as column names
    lane_counts_by_turn_type = pd.json_normalize(link_with_turns['turns_dict'])
    # merge it back with the link gdf so now link_with_turns has additional columns: through_turn, merge_only, through_only , turn_only     
    link_with_turns = pd.concat([link_with_turns, lane_counts_by_turn_type], axis=1)
    WranglerLogger.debug('turn lane accounting has been added to {:,} links'.format(
        link_with_turns.shape[0]))

    # debug: inconsistency between implied total lane count from 'turn' values and from osm 'lanes' values,
    # including 'lane_count_from_turns' != 'lanes_osmSplit', and lanes_osmSplit is missing
    # first, calculate implied lane counts from turns data
    link_with_turns['lane_count_from_turns'] = link_with_turns['turns_list'].apply(lambda x: len(x))
    # note that the 'turns' values in OSM doesn't consider middle turn lane, therefore, when there is middle turn lane,
    # 'lane_count_from_turns' should +1
    link_with_turns.loc[link_with_turns['bothways_lane_osmSplit'] == 1,
                        'lane_count_from_turns'] = link_with_turns['lane_count_from_turns'] + 1
    
    WranglerLogger.debug("link_with_turns.head(50):\n{}".format(link_with_turns.head(50)))

    # we're done with turns_list and turns_dict; drop them (since they're complex objects)
    link_with_turns.drop(columns=['turns_list','turns_dict'], inplace=True)

    lane_count_debug = link_with_turns.loc[
        link_with_turns['lane_count_from_turns'] != link_with_turns['lanes_osmSplit']]
    # export to inspect on a map
    lane_count_debug.reset_index(drop=True, inplace=True)
    WranglerLogger.debug(
        'export {} links with different total lane counts from "lanes" and "turns" for debugging'.format(
            lane_count_debug.shape[0]))
    LANE_COUNT_DEBUG_FILE = os.path.join(OUTPUT_DIR, 'lane_turn_count_diff.feather')
    geofeather.to_geofeather(lane_count_debug, LANE_COUNT_DEBUG_FILE)
    WranglerLogger.debug('Wrote lane_count_diff to {}'.format(LANE_COUNT_DEBUG_FILE))

    # merge it with links with no turn info
    # LMZ: this makes me nervous -- what's it merging on?
    osmnx_shst_gdf_new = pd.concat([link_with_turns,
                                    osmnx_shst_gdf.loc[osmnx_shst_gdf['turns:lanes_osmSplit'] == '']])
    osmnx_shst_gdf_new.reset_index(drop=True, inplace=True)
    WranglerLogger.debug('osmnx_shst_gdf_new.head(100):\n{}'.format(osmnx_shst_gdf_new.head(100)))

    # finally, there are links with 'bothways_lane_osmSplit' = 1 but are missing 'turns' info, set 'middle_turn' = 1
    osmnx_shst_gdf_new.loc[(osmnx_shst_gdf_new['turns:lanes_osmSplit'] == '') & \
                           (osmnx_shst_gdf_new['bothways_lane_osmSplit'] == 1), 'middle_turn'] = 1
    # set NA version of new columns to -1
    osmnx_shst_gdf_new.fillna(value={
        'through_turn'          :-1, 
        'merge_only'            :-1, 
        'through_only'          :-1, 
        'turn_only'             :-1, 
        'lane_count_from_turns' :-1, 
        'middle_turn'           :-1}, 
        inplace=True)
    # and convert to np.int8
    osmnx_shst_gdf_new = osmnx_shst_gdf_new.astype({
        'through_turn'          :np.int8, 
        'merge_only'            :np.int8,
        'through_only'          :np.int8,
        'turn_only'             :np.int8,
        'lane_count_from_turns' :np.int8,
        'middle_turn'           :np.int8})

    # save summary to csv and to log
    turn_lane_permutations_df = pd.DataFrame(osmnx_shst_gdf_new.value_counts(subset=[
        'drive_access','osm_dir_tag','turns:lanes_osmSplit', 'through_turn', 'merge_only', 'through_only', 'turn_only', 'lane_count_from_turns', 'middle_turn'], dropna=False)).reset_index(drop=False)
    turn_lane_permutations_df.rename(columns={0:'turn_count_type_numrows'},inplace=True)  # the count column is named 0 by default
    WranglerLogger.debug('turn_lane_permutations_df:\n{}'.format(turn_lane_permutations_df))
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'osmnx_turn_permutations.csv')
    turn_lane_permutations_df.to_csv(OUTPUT_FILE, header=True, index=False)
    WranglerLogger.debug('Wrote {}'.format(OUTPUT_FILE))

    WranglerLogger.debug('Turn lane summary: \n{}'.format(turn_lane_permutations_df))
    WranglerLogger.debug('osmnx_shst_gdf_new.dtypes:\n{}'.format(osmnx_shst_gdf_new.dtypes))

    WranglerLogger.info('Finished turn lane accounting, return {:,} links with following fields: {}'.format(
        osmnx_shst_gdf_new.shape[0],
        list(osmnx_shst_gdf_new)))
    return osmnx_shst_gdf_new


def reconcile_lane_count_inconsistency(osmnx_shst_gdf):
    """
    Resolve two cases:
    - Some links are missing 'lanes_osmSplit' data (either the data not available in OSMnx, or there was no sufficient
    infomation to imputate lane count by direction in step 'impute_num_lanes_each_direction_from_osm(osmnx_shst_gdf)'),
    but have 'lane_count_from_turns'. Set 'lanes_osmSplit' = 'lane_count_from_turns'.
    - links with both 'lanes_osmSplit' and 'lane_count_from_turns', but the values differ.

    Does not return anything; modifies the passed DataFrame.
    """
    WranglerLogger.info('Reconciling lane count inconsistency')

    # links missing 'lanes_osmSplit'
    WranglerLogger.debug('...{} links are missing lanes_osmSplit but have lane_count_from_turns'.format(
        ((osmnx_shst_gdf['lanes_osmSplit'] == -1) & (osmnx_shst_gdf['lane_count_from_turns'].notnull())).sum()
    ))
    osmnx_shst_gdf.loc[(osmnx_shst_gdf['lanes_osmSplit'] == -1) & osmnx_shst_gdf['lane_count_from_turns'].notnull(),
                       'lanes_osmSplit'] = osmnx_shst_gdf['lane_count_from_turns']

    # checked a few 'lanes_osmSplit' != 'lane_count_from_turns' examples, 'lane_count_from_turns' tends to be more accurate
    osmnx_shst_gdf.loc[
            (osmnx_shst_gdf['lanes_osmSplit'] != -1) & \
            osmnx_shst_gdf['lane_count_from_turns'].notnull() & \
            (osmnx_shst_gdf['lanes_osmSplit'] != osmnx_shst_gdf['lane_count_from_turns']),
        'lanes_osmSplit'] = osmnx_shst_gdf['lane_count_from_turns']


def consolidate_lane_accounting(osmnx_shst_gdf):
    """
    Consolidates data on lane accounting:
    'lanes_tot' =  'lanes_gp' (general purpose)
                 + 'lanes_hov' (high occupancy vehicle only)
                 + 'lanes_bus' (bus-only)
                 + 'lanes_turn'(left turn, right turn)
                 + 'lanes_aux' (auxiliary, could be on freeway typically between two interchanges to facilititate merging, but could also be merge lane segment on arterial; has less capacity than a full GP lane)
                 + 'lanes_mix' (mix of general purpose and turn/aux)

    Does not return anything; modifies the passed DataFrame by adding columns: lanes_non_gp, lanes_gp
    """
    WranglerLogger.info('Consolidating lane accounting')

    lane_accounting_fields = ['lanes_osmSplit', 'busOnly_lane_osmSplit', 'hov_lane_osmSplit', 'turn_only',
                              'through_turn', 'merge_only', 'through_only', 'middle_turn']
    WranglerLogger.debug('lane accounting fields value count: ')
    for field in lane_accounting_fields:
        WranglerLogger.debug('{}:\n{}'.format(field, osmnx_shst_gdf[field].value_counts(dropna=False)))

    # fill -1 and na with 0:
    for column_name in ['busOnly_lane_osmSplit', 'hov_lane_osmSplit', 'turn_only',
              'through_turn', 'merge_only', 'middle_turn']:
        osmnx_shst_gdf[column_name].fillna(0, inplace=True)
        if column_name in ['busOnly_lane_osmSplit', 'hov_lane_osmSplit']:
            osmnx_shst_gdf.loc[osmnx_shst_gdf[column_name] == -1, column_name] = 0
        # convert to int8
        osmnx_shst_gdf[column_name] = osmnx_shst_gdf[column_name].astype(np.int8)
        WranglerLogger.debug('after fill -1 and na with 0, {} value counts:\n{}'.format(
            column_name, osmnx_shst_gdf[column_name].value_counts(dropna=False)
        ))

    # rename fields
    osmnx_shst_gdf.rename(columns={'lanes_osmSplit'       : 'lanes_tot',
                                   'busOnly_lane_osmSplit': 'lanes_bus',
                                   'hov_lane_osmSplit'    : 'lanes_hov',
                                   'turn_only'            : 'lanes_turn',
                                   'through_turn'         : 'lanes_through_turn',
                                   'merge_only'           : 'lanes_aux',
                                   'middle_turn'          : 'lanes_middleturn'}, inplace=True)

    # calculate GP lane ('through_only' value only available for links with turn info, so cannot represent all GP lanes
    osmnx_shst_gdf.loc[osmnx_shst_gdf['lanes_tot'] != -1, 'lanes_non_gp'] = \
        osmnx_shst_gdf[['lanes_bus', 'lanes_hov', 'lanes_turn', 'lanes_through_turn',
                        'lanes_aux', 'lanes_middleturn']].sum(axis=1)
    
    osmnx_shst_gdf.loc[osmnx_shst_gdf['lanes_non_gp'].notnull(), 'lanes_gp'] = \
        osmnx_shst_gdf['lanes_tot'] - osmnx_shst_gdf['lanes_non_gp']
    osmnx_shst_gdf['lanes_gp'].fillna(-1, inplace=True)

    # convert to int8
    osmnx_shst_gdf['lanes_tot'] = osmnx_shst_gdf['lanes_tot'].astype(np.int8)
    osmnx_shst_gdf['lanes_gp' ] = osmnx_shst_gdf['lanes_gp' ].astype(np.int8)
    WranglerLogger.debug('consolidate_lane_accounting complete; dtypes=\n{}'.format(osmnx_shst_gdf.dtypes))


def update_attributes_based_on_way_length(osmnx_shst_gdf, attrs_longest, groupby_cols):
    """
    When multiple OSM ways are matched to a same shst link, update certain attributes to use the values of the longest
     OSM way.
    """
    # sort by shstReferenceId and length
    osmnx_shst_gdf_sorted = osmnx_shst_gdf.sort_values(['shstReferenceId', 'length'], ascending=False)

    # group by 'shstReferenceId' and keep the first (longest OSM way) of each group
    WranglerLogger.debug('......osmnx_shst_gdf has {:,} unique sharedstreets links'.format(
        osmnx_shst_gdf.drop_duplicates(subset=groupby_cols).shape[0]))
    osmnx_new_values_by_shst = osmnx_shst_gdf_sorted.groupby(
        groupby_cols).first().reset_index()[groupby_cols + attrs_longest]
    # check the row count of osmnx_new_values_by_shst == unique shst link count
    WranglerLogger.debug('......groupby resulted in {:,} rows of shst-link-level attributes'.format(
        osmnx_new_values_by_shst.shape[0]))

    # join the updated value back to the dataframe
    osmnx_shst_other_attrs_gdf = osmnx_shst_gdf.loc[:, ~osmnx_shst_gdf.columns.isin(attrs_longest)]
    osmnx_shst_gdf_updated = osmnx_shst_other_attrs_gdf.merge(osmnx_new_values_by_shst,
                                                              on=groupby_cols,
                                                              how='left')
    WranglerLogger.debug('......finished updating attributes based on osm way length')

    return osmnx_shst_gdf_updated


def aggregate_osm_ways_back_to_shst_link(osmnx_shst_gdf):
    """
    when multiple OSM Ways comprise one sharedstreets link:
    """
    WranglerLogger.info('Starting aggregated osm ways back to shst links')
    WranglerLogger.debug('...osmnx_shst_gdf field types:\n{}'.format(osmnx_shst_gdf.dtypes))

    # 1, separate the attributes into different groups based on what consolidation methodology to apply
    WranglerLogger.debug('... osmnx_shst_gdf link attributes are grouped into the following sets:')
    # 'attrs_shst_level' fields: already represent the values of the entire sharedstreet link, no change;
    # though 'reverse' is created at OSM way level, but all OSM ways of the same shst link have the same 'reverse' value
    attrs_shst_level = ['id', 'fromIntersectionId', 'toIntersectionId', 'shstReferenceId', 'shstGeometryId', 'geometry', 'reverse']
    # 'attrs_sum' fields: sum the values of each OSM way
    attrs_sum = ['length']
    # 'attrs_max' fields: use the largest value of all OSM ways in the same shst link
    attrs_max = ['waySections_len']
    # 'attrs_concat' fields: concatenate
    attrs_concat = ['wayId', 'waySection_ord', 'osmid', 'osmnx_shst_merge', 'index']
    # 'attrs_merge' fields: node-related merge
    attrs_merge = ['nodeIds', 'u', 'v']
    # 'attrs_longest' fields: use the values of the longest OSM way
    attrs_longest = list(
        set(list(osmnx_shst_gdf)) - set(attrs_shst_level) - set(attrs_sum) - set(attrs_max) - set(attrs_merge) - set(
            attrs_concat))
    WranglerLogger.debug('no need to aggregate: {}'.format(attrs_shst_level))
    WranglerLogger.debug('aggregate using sum: {}'.format(attrs_sum))
    WranglerLogger.debug('aggregate using max: {}'.format(attrs_max))
    WranglerLogger.debug('aggregate using concatenation: {}'.format(attrs_concat))
    WranglerLogger.debug('aggregate by merging nodes: {}'.format(attrs_merge))
    WranglerLogger.debug('aggregate by using the value of the longest osm way: {}'.format(attrs_longest))

    # 2, for 'attrs_longest', set the value of all osm ways of the same shst to be the same as the longest osm way
    groupby_cols = ['id', 'fromIntersectionId', 'toIntersectionId', 'shstReferenceId', 'shstGeometryId']
    WranglerLogger.debug('...updating values for osm-way-length-based link attribute')
    osmnx_shst_gdf_longest_update = update_attributes_based_on_way_length(osmnx_shst_gdf, attrs_longest, groupby_cols)

    # 3, create an agg dictionary for grouping by osm ways by shst link
    def _concat_way_values(x):
        """
        Aggregation function for concatenating values of OSM ways into a string, e.g. one ShSt link contains two OSM
        ways with 'wayId' 394112485 and 393899244, the aggregated ShSt link 'wayId' = '394112485,393899244'
        """
        values = x.str.cat(sep=",")
        return values

    def _merge_nodeIds(x):
        """
        Aggregation function for merging nodeIds, e.g. two OSM ways of the same ShSt link have nodeIds:
        ['4717667145', '4717667151'], ['4717667151', '4717667176', '4717667189', '4717667193', '4717667196'],
        the merged nodeIds is ['4717667145', '4717667151', '4717667176', '4717667189', '4717667193', '4717667196']
        """
        # get all series into a nested list of series
        nodeIds_ls = list(x)
        # expand to one list
        nodeIds_ls_expand = [item for sublist in nodeIds_ls for item in sublist]
        # remove duplicates while keep node order; duplicated nodes appear at connecting osm ways
        nodeIds_ls_expand_nodup = list(dict.fromkeys(nodeIds_ls_expand))
        return nodeIds_ls_expand_nodup

    # set initial agg dictionary
    agg_dict = {
        'geometry'       : 'first',
        'reverse'        : 'first',
        'u'              : 'first',         # 'u' of the shst link is the u of the first osm way
        'v'              : 'last',          # 'v' of the shst link is the v of the last osm way
        'nodeIds'        : _merge_nodeIds,  # 'nodeIds' apply the aggregation function
        'length'         : 'sum',           # 'length' of the shst link is the sum of all osm ways
        'waySections_len': 'max'            # 'waySections_len' of the shst link is the max of all osm ways
    }
    # add other fields
    # fields that need to concatenate into tuples
    for c in attrs_concat:
        agg_dict[c] = _concat_way_values
    # fields whose values are already updated
    for c in attrs_longest:
        agg_dict[c] = 'first'

    WranglerLogger.debug('...aggregation dict:\n{}'.format(agg_dict))

    # TODO: decide if 4. fill NaN is needed
    # 4, fill NaN and modify field type before aggregating. This fills NAs for ShSt-derived OSM Ways that do not have
    # complete osm info, so that these osm ways won't be omitted when aggregating back to ShSt links using aggregation
    # rules such as 'first' (will keep the first non-na value). However, if not all OSM Ways have info, we may want to
    # use the next non-na value instead of having no value, in this case, skip this step.
    WranglerLogger.debug('...fill na for numeric and string fields')
    _fill_na(osmnx_shst_gdf_longest_update)

    # 5, convert 'attrs_concat' fields to string so they can be concatenated into a string
    WranglerLogger.debug('...modify concatenation fields to type string')
    for col_name in attrs_concat:
        # 'osmnx_shst_merge' is categorical value
        if col_name == 'osmnx_shst_merge':
            pass
        else:
            # for 'wayId', 'waySection_ord', 'osmid', 'index', make sure it is integer, not float
            osmnx_shst_gdf_longest_update[col_name].fillna(0, inplace=True)
            osmnx_shst_gdf_longest_update[col_name] = osmnx_shst_gdf_longest_update[col_name].astype(int)
        # convert all to string
        osmnx_shst_gdf_longest_update[col_name] = osmnx_shst_gdf_longest_update[col_name].astype(str)

    # 6, apply the aggregation method to forward links and backward links separately. This is because 'waySection_ord'
    # represents the order of an OSM way in a shst link in the initial shst metadata; after adding two-way OSM ways (
    # 'reverse=True' links), the order should be reversed, in other words, descending.
    forward_link_gdf = osmnx_shst_gdf_longest_update.loc[osmnx_shst_gdf_longest_update['reverse'] == False].copy()
    forward_link_gdf.sort_values(['shstReferenceId', 'waySection_ord'], inplace=True)
    WranglerLogger.debug('...aggregating forward links')
    forward_link_gdf_agg = forward_link_gdf.groupby(groupby_cols).agg(agg_dict).reset_index()

    backward_link_gdf = osmnx_shst_gdf_longest_update.loc[osmnx_shst_gdf_longest_update['reverse'] == True].copy()
    backward_link_gdf.sort_values(['shstReferenceId', 'waySection_ord'], ascending=False, inplace=True)
    WranglerLogger.debug('...aggregating backward links')
    backward_link_gdf_agg = backward_link_gdf.groupby(groupby_cols).agg(agg_dict).reset_index()

    # put them together
    shst_link_gdf = pd.concat([forward_link_gdf_agg, backward_link_gdf_agg],
                              sort=False,
                              ignore_index=True)

    # assign EPSG
    shst_link_gdf = gpd.GeoDataFrame(shst_link_gdf, crs=LAT_LONG_EPSG)

    return shst_link_gdf


def _fill_na(df):
    """
    fill str NaN with ""
    fill numeric NaN with 0
    TODO: what about boolean fields?

    Does not return anything; modifies the passed DataFrame by filling in na.
    """

    num_col = list(df.select_dtypes([np.number]).columns)
    WranglerLogger.debug('numeric columns: {}'.format(num_col))

    object_col = list(df.select_dtypes(['object']).columns)
    # remove 'oneway_shst', 'oneway_osmnx' from 'object_col' because they should be boolean fields
    for remove_col in ['oneway_shst', 'oneway_osmnx']:
        if remove_col in object_col:
            object_col.remove(remove_col)
    WranglerLogger.debug('str columns: {}'.format(object_col))

    for x in list(df.columns):
        if x in num_col:
            df[x].fillna(0, inplace=True)
        elif x in object_col:
            df[x].fillna("", inplace=True)


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

    shst_link_gdf = gpd.GeoDataFrame(shst_link_gdf, crs=LAT_LONG_EPSG)

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
    # don't waste time doing reversed links; this is sufficient for pulling nodes
    forward_link_gdf = link_gdf[link_gdf.reverse == False].copy()

    # create point geometry from shst linestring
    forward_link_gdf["u_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[0]), axis=1)
    forward_link_gdf["v_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[-1]), axis=1)

    # we want the u and v points for each link
    point_gdf = pd.concat([
        forward_link_gdf[["u", "fromIntersectionId", "u_point"]].rename(columns={
            "u"                 : "osm_node_id",
            "fromIntersectionId": "shst_node_id",
            "u_point"           : "geometry"}),
        forward_link_gdf[["v", "toIntersectionId", "v_point"]].rename(columns={
            "v"                 : "osm_node_id",
            "toIntersectionId"  : "shst_node_id",
            "v_point"           : "geometry"})],
        sort=False,
        ignore_index=True)

    # drop duplicates
    point_gdf.drop_duplicates(subset=["osm_node_id", "shst_node_id"], inplace=True)

    point_gdf = gpd.GeoDataFrame(point_gdf, crs=LAT_LONG_EPSG)

    return point_gdf


def tag_nodes_links_by_county_name(node_gdf, link_gdf, counties_gdf):
    """
    Tag network nodes and links by county.
    Nodes: first spatially join nodes with counties shapes; for nodes failed to join (e.g. in the Bay), 
           match to the nearest joined node based on cKDTree. cKDTree requires meter-based CRS. 
    Links: first spatially join links with counties shapes based on link centroids. For links failed to join, 
           match to the nearest nodes which already have county tags.
    """

    ###### tag nodes 
    WranglerLogger.info('tagging nodes with county names')

    WranglerLogger.debug('...spatially joining nodes with county shape')
    node_county_gdf = gpd.sjoin(node_gdf, counties_gdf, how='left', predicate='intersects')

    # some nodes may get joined to more than one county, e.g. geometry on the boundary. Drop duplicates
    WranglerLogger.debug('# of unique nodes: {}'.format(node_gdf.shape[0]))
    WranglerLogger.debug('# of nodes in spatial join result: {}'.format(node_county_gdf.shape[0]))
    WranglerLogger.debug('# of unique nodes in spatial join result: {}'.format(node_county_gdf.shst_node_id.nunique()))
    WranglerLogger.info('...drop duplicates due to nodes on county boundaries getting more than one math')
    node_county_gdf.drop_duplicates(subset=['shst_node_id'], inplace=True)

    # use nearest match to fill in names for nodes that did not get county match (e.g. in the Bay)
    # Note: (also see https://app.asana.com/0/0/1202393240187598/f)
    # - WSP's initial method uses cKDTree, first, construct a k-dimensional tree from matched points, then iterate through
    #   each unmatched node to find its nearest neighbor. A slightly revised version of this method is here:
    #   https://github.com/BayAreaMetro/travel-model-two-networks/blob/2f990f5438965389fd814fabf747413aea2a8d4c/notebooks/pipeline/methods.py#L1777)
    # - A faster approach that also uses cKDTree is to do the second part in a dataframe instead of iterating through. Example:
    #   "def ckdnearest(gdA, gdB)" at https://gis.stackexchange.com/questions/222315/finding-nearest-point-in-other-geodataframe-using-geopandas
    # - geopandas.sjoin_nearest() usually is slower than cKDTree for large dataset. However, in this case, node_county_unmatched 
    #   is small (less than 300 nodes) while node_county_matched is large, therefore the "overhead" of constructing the tree seems
    #   unnecessarily large. geopandas.sjoin_nearest() is actually faster.

    node_county_matched_gdf = node_county_gdf.loc[node_county_gdf.NAME.notnull()]
    node_county_unmatched_gdf = node_county_gdf.loc[node_county_gdf.NAME.isnull()]
    WranglerLogger.debug('{:,} nodes got a county join'.format(node_county_matched_gdf.shape[0]))
    WranglerLogger.debug('{:,} nodes failed to get county join'.format(node_county_unmatched_gdf.shape[0]))
    WranglerLogger.debug('filling in county name by looking for nearest nodes that already got a county join')

    # convert to meter-based EPSG
    node_county_matched_gdf = node_county_matched_gdf.to_crs(CRS('epsg:{}'.format(str(NEAREST_MATCH_EPSG))))
    node_county_unmatched_gdf = node_county_unmatched_gdf.to_crs(CRS('epsg:{}'.format(str(NEAREST_MATCH_EPSG))))

    # spatial join
    node_county_rematch_gdf = gpd.sjoin_nearest(node_county_unmatched_gdf[['shst_node_id', 'geometry']],
                                                node_county_matched_gdf[['geometry', 'NAME']], how='left')
    WranglerLogger.debug('found nearest nodes for {} out of {} previously unmatched nodes'.format(
        node_county_rematch_gdf.loc[node_county_rematch_gdf.NAME.notnull()].shape[0],
        node_county_unmatched_gdf.shape[0]
    ))

    # merge with nodes got a county join
    node_county_matched_gdf = pd.concat([node_county_matched_gdf[['shst_node_id', 'NAME']],
                                         node_county_rematch_gdf[['shst_node_id', 'NAME']]])

    # merge county name into node_gdf
    node_with_county_gdf = pd.merge(
        node_gdf,
        node_county_matched_gdf,
        how='left',
        on='shst_node_id')

    ###### tag links 
    WranglerLogger.info('tagging links with county names')

    # spatial join links with county shape based on link centroids
    WranglerLogger.debug('...spatially joining links with county shape using link centroids')

    # first, create a temporary unique link identifier for convenience in merging; at this point, link_gdf unique
    # link identifier is based on ['fromIntersectionId', 'toIntersectionId', 'shstReferenceId', 'shstGeometryId']
    link_gdf.loc[:, 'temp_link_id'] = range(len(link_gdf))

    # get link centroids
    link_centroid_gdf = link_gdf.copy()
    link_centroid_gdf["geometry"] = link_centroid_gdf["geometry"].centroid

    # spatial join
    link_centroid_gdf_join = gpd.sjoin(link_centroid_gdf, counties_gdf, how="left", predicate="intersects")
    WranglerLogger.debug('{} unique links'.format(link_gdf.shape[0]))
    WranglerLogger.debug('{} links in spatial join result, representing {} unique links'.format(
        link_centroid_gdf_join.shape[0],
        link_centroid_gdf_join['temp_link_id'].nunique()))
    if link_centroid_gdf_join.shape[0] != link_centroid_gdf_join['temp_link_id'].nunique():
        WranglerLogger.debug('...drop duplicates due to links on county boundaries getting more than one match')
        link_centroid_gdf_join.drop_duplicates(subset=['temp_link_id'], inplace=True)

    # use nearest match to fill in names for links that did not get county match (e.g. in the Bay)

    link_centroid_county_matched_gdf = link_centroid_gdf_join.loc[link_centroid_gdf_join['NAME'].notnull()]
    link_centroid_county_unmatched_gdf = link_centroid_gdf_join.loc[link_centroid_gdf_join['NAME'].isnull()]
    # for some reason, the output of sjoin "link_centroid_gdf_join" is a dataframe instead of a geodataframe, convert.  
    link_centroid_county_unmatched_gdf = gpd.GeoDataFrame(link_centroid_county_unmatched_gdf,
                                                          crs=link_centroid_gdf.crs,
                                                          geometry='geometry')

    WranglerLogger.debug('{:,} links got a county join'.format(link_centroid_county_matched_gdf.shape[0]))
    WranglerLogger.debug('{:,} links failed to get a county join'.format(link_centroid_county_unmatched_gdf.shape[0]))
    WranglerLogger.debug('filling in county name by looking for nearest nodes that already got a county join')

    node_county_matched_gdf = node_with_county_gdf.loc[node_with_county_gdf['NAME'].notnull()][['geometry', 'NAME']]
    # convert to meter-based EPSG
    node_county_matched_gdf = node_county_matched_gdf.to_crs(CRS('epsg:{}'.format(str(NEAREST_MATCH_EPSG))))
    link_centroid_county_unmatched_gdf = \
        link_centroid_county_unmatched_gdf.to_crs(CRS('epsg:{}'.format(str(NEAREST_MATCH_EPSG))))
    
    # spatial join
    link_centroid_county_rematch_gdf = gpd.sjoin_nearest(link_centroid_county_unmatched_gdf[['temp_link_id', 'geometry']],
                                                         node_county_matched_gdf[['geometry', 'NAME']], how='left')
    WranglerLogger.debug('found nearest nodes for {} out of {} previously unmatched nodes'.format(
        link_centroid_county_rematch_gdf.loc[link_centroid_county_rematch_gdf.NAME.notnull()].shape[0],
        link_centroid_county_unmatched_gdf.shape[0]
    ))

    # merge with links got a county join
    link_centroid_county_matched_gdf = pd.concat([link_centroid_county_matched_gdf[['temp_link_id', 'NAME']], 
                                                  link_centroid_county_rematch_gdf[['temp_link_id', 'NAME']]])

    # merge county name into link_gdf
    link_with_county_gdf = pd.merge(
        link_gdf,
        link_centroid_county_matched_gdf,
        how='left',
        on='temp_link_id')

    # drop temporary unique link id
    link_with_county_gdf.drop(['temp_link_id'], axis=1, inplace=True)
    
    # rename 'NAME' to 'county'
    node_with_county_gdf.rename(columns={'NAME': 'county'}, inplace=True)
    link_with_county_gdf.rename(columns={'NAME': 'county'}, inplace=True)

    return node_with_county_gdf, link_with_county_gdf


def remove_out_of_region_links_nodes(link_gdf, node_gdf):
    """
    Remove out-of-region links and nodes used by out-of-region links; 
    for nodes that are out-of-region but used by cross-region links (therefore not removed), re-label them to Bay Area counties.
    """

    # first, remove out-of-region links
    WranglerLogger.debug('...dropping out-of-region links')
    link_BayArea_gdf = link_gdf.loc[link_gdf['county'].isin(BayArea_COUNTIES)]
    link_BayArea_gdf.reset_index(drop=True, inplace=True)
    
    # then, remove nodes not use by BayArea links
    WranglerLogger.debug('...dropping nodes not used by Bay Area links')
    node_BayArea_gdf = node_gdf[node_gdf.shst_node_id.isin(link_BayArea_gdf.fromIntersectionId.tolist() +
                                                           link_BayArea_gdf.toIntersectionId.tolist())]
    
    # for nodes that are outside the Bay Area but used by BayArea links, need to give them the
    # internal county names:
    WranglerLogger.debug('...updating county names of out-of-region nodes that are used by Bay Area links')
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

    return link_BayArea_gdf, node_BayArea_gdf


def conflate(third_party: str, third_party_gdf: gpd.GeoDataFrame, id_columns, third_party_type,
             THIRD_PARTY_OUTPUT_DIR, OUTPUT_DATA_DIR, CONFLATION_SHST, BOUNDARY_DIR, docker_container_name=None):
    """
    Generic conflation method.  Does the following:
    1) Creates conflation directory (if it doesn't exist)
    2) If the dataset is large, does the following by partitioning the dataset by the subregion boundaries established in step1_extract_shst
       and iterating through them, doing the following steps.  Otherwise, this is done in one go
       a) If this partition is already conflated (input and output files exist) skip to step d)
       b) Writes the third_party_gdf (id_columns and geometry) for conflation
       c) Runs the conflation in a docker container
       d) Reads the resulting matched and unmatched results, concat with previous partition results (if relevant)
    3) Merges the full results back with the full third_party_gdf and writes them for debugging
    4) Returns the resulting GeoDataFrames (matched and unmatched)
    
    Args:
        - third_party: string name of the data source, need to be consistent with folder name.
        - third_party_gdf: geodataframe of third-party network data, already convert to CRS, with (one or more) id column(s). 
        - id_columns: unique identifier of each row.
        - third_party_type: 'roadway_link' or 'transit', as different shst match commands apply.
        - THIRD_PARTY_OUTPUT_DIR: where all the third-party datasets' shst match outputs will be written to.
        - OUTPUT_DATA_DIR: the top level of output data dir, where the Docker contained will be created.
        - CONFLATION_SHST: sub-folder for the current third-party dataset's shst match outputs will be written to.
        - BOUNDARY_DIR: dir of th boundary files used to split the third-party dataset before shst matching if it is too large.
        - docker_container_name: name of docker container to use; otherwise creates a new one.
    
    """
    WranglerLogger.info('Running conflate() for {} with id_columns {}; third_party_gdf.dtypes:\n{}'.format(
        third_party, id_columns, third_party_gdf.dtypes))

    # create conflation directory
    if third_party_type == 'roadway_link':
        conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, third_party, CONFLATION_SHST)
    elif third_party_type == 'transit':
        conflation_dir = os.path.join(THIRD_PARTY_OUTPUT_DIR, CONFLATION_SHST)
    else:
        error_message = "conflate: third_party_type must be one of ['roadway_link','transit']. Received {}".format(third_party_type)
        WranglerLogger.error(error_message)
        raise ValueError(error_message)

    WranglerLogger.debug("conflate: conflation_dir = {}".format(conflation_dir))

    if not os.path.exists(conflation_dir):
        WranglerLogger.info('creating conflation folder: {}'.format(conflation_dir))
        os.makedirs(conflation_dir)

    # In order to conflate, the dataset needs to be in ESPG WGS 84 (latitude/longitude)
    prev_crs_str = str(third_party_gdf.crs)
    third_party_gdf = third_party_gdf.to_crs(epsg=LAT_LONG_EPSG)
    WranglerLogger.info('Converted third_party_gdf from crs {} to crs {}'.format(prev_crs_str, third_party_gdf.crs))

    # For smaller datasets, partitioning by the boundaries is not necessary with NODE_OPTIONS==--max_old_space_size=8192
    # For larger datasets, I'm not getting crashing but the 'optimizing graph...' step takes many hours; I gave up at 20
    # So far, 50k (ACTC, CCTA) has been fine without partitioning and 950k (TomTom) has not.
    # Whether "shst match" will run out of memory depends on the number of target links (or nodes) in the underlying
    # shst network, not the side of the third-party dataset, but the latter is a good approximation in this case.
    # For the same reason, the partitioning slipts the dataset by geographic boundaries, not by some criteria such as FT.
    boundary_partitions = ['']
    if len(third_party_gdf) > 800000:
        boundary_partitions = ['_{:02d}'.format(boundary_num) for boundary_num in range(1, NUM_SHST_BOUNDARIES+1)]
    WranglerLogger.debug('boundary_partitions = {}'.format(boundary_partitions))
    
    client        = None
    matched_gdf   = gpd.GeoDataFrame()
    unmatched_gdf = gpd.GeoDataFrame()

    # verify that the id columns serve as unique identifiers
    duplicated_id = third_party_gdf.duplicated(subset=id_columns)
    assert(duplicated_id.sum() == 0)

    # write input file to single geofeather for debugging
    debug_input_file    = os.path.join(conflation_dir, '{}.in.feather'.format(third_party))
    geofeather.to_geofeather(third_party_gdf.reset_index(), debug_input_file)
    WranglerLogger.debug('Wrote input file as geofeather {} for debugging'.format(debug_input_file))

    # boundary_partition is either '' or '_01','_02', etc
    for boundary_partition in boundary_partitions:
        # input / output file for this boundary_partition
        shst_input_file     = os.path.join(conflation_dir, '{}{}.in.geojson'.format(third_party, boundary_partition))
        shst_matched_file   = os.path.join(conflation_dir, '{}{}.out.matched.geojson'.format(third_party, boundary_partition))
        shst_unmatched_file = os.path.join(conflation_dir, '{}{}.out.unmatched.geojson'.format(third_party, boundary_partition))

        # NOTE: skip shst conflation of this partition if it was already done and if the files are in place since this process is slow....
        if os.path.exists(shst_input_file) and os.path.exists(shst_matched_file):
            WranglerLogger.info('{} conflation files {} and {} exist -- skipping conflation'.format(
                third_party, shst_input_file, shst_matched_file))

        else:
            # do this onely once, not for every partition
            if (client == None) and (docker_container_name != None):
                try:
                    WranglerLogger.info('Attempting to get docker container named {}'.format(docker_container_name))
                    (client, container) = get_docker_container(docker_container_name)
                    WranglerLogger.info('Docker container found')
                except Exception as error:
                    WranglerLogger.error('Could not get docker container named {}; error:{}'.format(docker_container_name, error))

            if (client == None):
                # create docker container to do the shst matchting
                (client, container) = create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

            # we're going to need to cd into OUTPUT_DATA_DIR -- create that path (on UNIX)
            docker_output_path = docker_path(OUTPUT_DATA_DIR)

            # no partitioning
            if boundary_partition == '':
                # export the whole dataset
                WranglerLogger.info('Exporting {:,} rows of {} data to {}'.format(len(third_party_gdf), third_party, shst_input_file))
                third_party_gdf[id_columns + ['geometry']].to_file(shst_input_file, driver="GeoJSON") # write id columns and geometry
            else:
                # export the partition dataset
                boundary_gdf = gpd.read_file(os.path.join(BOUNDARY_DIR, 'boundary{}.geojson'.format(boundary_partition)))
                third_party_partition_gdf = third_party_gdf.loc[third_party_gdf.intersects(boundary_gdf.geometry.unary_union)]
                WranglerLogger.info('Exporting {:,} rows of {} data to {}'.format(len(third_party_partition_gdf), third_party, shst_input_file))
                third_party_partition_gdf[id_columns + ['geometry']].to_file(shst_input_file, driver="GeoJSON")

            # run the conflation
            if third_party_type == 'roadway_link':
                command = ("/bin/bash -c 'cd {path}; shst match step4_third_party_data/{third_party}/conflation_shst/{third_party}{boundary_partition}.in.geojson "
                            "--out=step4_third_party_data/{third_party}/conflation_shst/{third_party}{boundary_partition}.out.geojson "
                            "--tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction'".format(
                                path=docker_output_path, third_party=third_party, boundary_partition=boundary_partition))
            elif third_party_type == 'transit':
                command = ("/bin/bash -c 'cd {path}; shst match step6_gtfs/output/conflation_shst/{third_party}{boundary_partition}.in.geojson "
                            "--out=step6_gtfs/output/conflation_shst/{third_party}{boundary_partition}.out.geojson "
                            "--tile-hierarchy=8 --follow-line-direction'".format(
                                path=docker_output_path, third_party=third_party, boundary_partition=boundary_partition))
                
            WranglerLogger.info('Executing docker command: {}'.format(command))
        
            (exec_code,exec_output) = container.exec_run(command, stdout=True, stderr=True, stream=True)
            while True:
                try:
                    line = next(exec_output)
                    # note: this looks a little funny because it's a byte string
                    WranglerLogger.debug(line.strip())
                except StopIteration:
                    # done
                    WranglerLogger.info('...Completed command. exec_code: {}'.format(exec_code))
                    break
            
        # read the results
        if os.path.exists(shst_matched_file):
            matched_partition_gdf   = gpd.read_file(shst_matched_file)
            WranglerLogger.debug('Read {:,} rows from {}; dtypes:\n{}'.format(len(matched_partition_gdf), 
                shst_matched_file, matched_partition_gdf.dtypes))
            
            # combine with previous
            matched_gdf = pd.concat([matched_gdf, matched_partition_gdf], axis='index')
            WranglerLogger.debug('matched_gdf has {:,} rows and columns:{}'.format(len(matched_gdf), 
                list(matched_gdf.columns)))

        if os.path.exists(shst_unmatched_file):
            unmatched_partition_gdf = gpd.read_file(shst_unmatched_file)
            WranglerLogger.debug('Read {:,} rows from {}; dtypes:\n{}'.format(len(unmatched_partition_gdf), 
                shst_unmatched_file, unmatched_partition_gdf.dtypes))

            # combine with previous
            unmatched_gdf = pd.concat([unmatched_gdf, unmatched_partition_gdf], axis='index')
            WranglerLogger.debug('unmatched_gdf has {:,} rows and columns:{}'.format(len(unmatched_gdf), 
                list(unmatched_gdf.columns)))

    # all possible conflation is done
    if (client != None):
        pass
        # don't stop the container; reusing it for subsequent shst matching will be more efficient since it'll use cached shst tiles
        # WranglerLogger.info('stopping container {}'.format(container.name))
        # container.stop()
        # client.containers.prune()

    if len(matched_gdf) > 0:
        # rename id columns back to original; shst match will lowercase and prepend with pp_
        rename_columns = {
            'shstFromIntersectionId': 'fromIntersectionId',
            'shstToIntersectionId'  : 'toIntersectionId',
        }
        for id_column in id_columns:
            rename_columns['pp_{}'.format(id_column.lower())] = id_column
        WranglerLogger.debug('Renaming columns: {}'.format(rename_columns))
        matched_gdf.rename(columns=rename_columns, inplace=True)

        # merge back with original
        matched_gdf = pd.merge(
            left     = matched_gdf,
            right    = third_party_gdf.drop(columns=['geometry']),  # we already have this
            how      = 'left',
            on       = id_columns)
        WranglerLogger.debug('After join, matched_gdf.dtypes:\n{}'.format(matched_gdf.dtypes))

        # output for debugging
        if third_party_type == 'roadway_link':
            matched_geofeather = os.path.join(THIRD_PARTY_OUTPUT_DIR, third_party, CONFLATION_SHST, 'matched.feather')
        elif third_party_type == 'transit':
            matched_geofeather = os.path.join(CONFLATION_SHST, '{}_matched.feather'.format(third_party))
        
        geofeather.to_geofeather(matched_gdf, matched_geofeather)
        WranglerLogger.info('Wrote {:,} lines to {}'.format(len(matched_gdf), matched_geofeather))

        WranglerLogger.info('Sharedstreets matched {:,} out of {:,} total unique ids'.format(
            len(matched_gdf.drop_duplicates(subset=id_columns)), len(third_party_gdf)))

    
    else:
        matched_gdf = None
        WranglerLogger.debug('No matched file(s) found')        

    if len(unmatched_gdf) > 0:
        # shst lowercases columns -- rename back if needed
        rename_columns = {}
        for id_column in id_columns:
            if id_column.lower() != id_column: rename_columns[id_column.lower()] = id_column
        if len(rename_columns) > 0:
            WranglerLogger.debug('Renaming columns: {}'.format(rename_columns))
            unmatched_gdf.rename(columns=rename_columns, inplace=True)

        unmatched_gdf = pd.merge(
            left     = unmatched_gdf,
            right    = third_party_gdf.drop(columns=['geometry']),  # we already have this
            how      = 'left',
            on       = id_columns)
        WranglerLogger.debug('After join, unmatched_gdf.dtypes:\n{}'.format(unmatched_gdf.dtypes))

        # output for debugging
        if third_party_type == 'roadway_link':
            unmatched_geofeather = os.path.join(THIRD_PARTY_OUTPUT_DIR, third_party, CONFLATION_SHST, 'unmatched.feather')
        elif third_party_type == 'transit':
            unmatched_geofeather = os.path.join(CONFLATION_SHST, '{}_unmatched.feather'.format(third_party))
        geofeather.to_geofeather(unmatched_gdf, unmatched_geofeather)
        WranglerLogger.info('Wrote {:,} lines to {}'.format(len(unmatched_gdf), unmatched_geofeather))

        WranglerLogger.info('Sharedstreets failed to match {:,} out of {:,} total unique ids'.format(
            len(unmatched_gdf.drop_duplicates(subset=id_columns)), len(third_party_gdf)))
    else:
        unmatched_gdf = None
        WranglerLogger.debug('No unmatched file(s) found')

    return (matched_gdf, unmatched_gdf)


def pems_station_sheild_dir_nearest_match(pems_gdf, link_gdf, pems_type_roadway_crosswalk):
    """

    """
    WranglerLogger.info('Start matching PEMS stations to links using nearest sheildnum+direction match')
    offset = 100

    pems_route_list = pems_gdf.route.unique().tolist()
    pems_match_gdf = gpd.GeoDataFrame()

    # iterate through each route
    for route in pems_route_list:
        WranglerLogger.debug('...pems route id {}'.format(route))
        pems_route_gdf = pems_gdf.loc[pems_gdf.route == route]
        dir_list = pems_route_gdf.direction.unique().tolist()

        # iterate through each direction
        for direction in dir_list:
            WranglerLogger.debug('\t ...pems direction {}'.format(direction))
            pems_route_dir_gdf = pems_route_gdf.loc[pems_route_gdf.direction == direction]
            type_list = pems_route_dir_gdf['type'].unique().tolist()

            # iterate through each road type
            for ptype in type_list:
                WranglerLogger.debug('\t\t ...pems type {}'.format(ptype))
                pems_route_dir_ptype_gdf = pems_route_dir_gdf.loc[pems_route_dir_gdf['type'] == ptype]
                # create a bounding box around each station with the set offset;
                # bbox is a dataframe with columns 'minx', 'miny', 'maxx', 'maxy'
                bbox = pems_route_dir_ptype_gdf.bounds + [-offset, -offset, offset, offset]

                # get links with the same shieldnum, rtedir, roadway
                # str(route) because 'route' is numeric in PEMS data but 'tomtom_shieldnum' is string
                link_candidates = link_gdf.loc[(link_gdf.tomtom_shieldnum == str(route)) &
                                               (link_gdf.tomtom_rtedir == direction) &
                                               (link_gdf.roadway.isin(pems_type_roadway_crosswalk[ptype]))]
                # if no link, use links with the same roadway type (regardless of name/direction) as candidates instead
                if link_candidates.shape[0] == 0:
                    WranglerLogger.debug('\t\t ...there is no link with tomtom label direction {}, route {}, roadway {},'
                                        'matching to the closest {}'.format(
                        direction, route, ptype, pems_type_roadway_crosswalk[ptype]))
                    link_candidates = link_gdf.loc[link_gdf.roadway.isin(pems_type_roadway_crosswalk[ptype])]

                # use GeoPandas R-tree spatial indexing (sindex) to intersect each PEMS point's bbox with all candidate
                # links; the result "hit" has the same length as bbox, the values are the index of candidate link(s)
                # that are intersect with each bbox row (each PEMS point's bbox)
                hits = bbox.apply(lambda row: list(link_candidates.sindex.intersection(row)),
                                  axis=1)
                # convert hits into a dataframe with two columns: 'pt_idx' and 'link_i'
                tmp = pd.DataFrame({
                    # index of points table 'pems_route_dir_ptype_gdf' (also bbox)
                    "pt_idx": np.repeat(hits.index, hits.apply(len)),
                    # ordinal position of link - access via iloc later
                    "link_i": np.concatenate(hits.values)
                })

                # set pt_idx as index and join with pems_route_dir_ptype_gdf
                tmp.set_index(["pt_idx"], inplace=True)
                tmp = tmp.join(
                    pems_route_dir_ptype_gdf[[
                        'station', 'longitude', 'latitude', 'route', 'direction', 'type', 'geometry'
                    ]].rename(
                        columns={"geometry": "point"}),
                    how='left')

                # rest link_condidates index and join tmp to it
                tmp.set_index(['link_i'], inplace=True)
                tmp = tmp.join(
                    link_candidates[[
                        'shstReferenceId', 'roadway', 'tomtom_shieldnum', 'tomtom_rtedir', 'geometry'
                    ]].reset_index(drop=True),
                    how='left')

                # find closest link to point
                # 1st, convert it to geodataframe with link's geometry as geometry
                tmp_gdf = gpd.GeoDataFrame(tmp, geometry=tmp['geometry'], crs=pems_gdf.crs)
                # 2nd, calculate the snap distance between each point (PEMS station) and the hitted link
                tmp_gdf['snap_distance'] = tmp_gdf.geometry.distance(gpd.GeoSeries(tmp_gdf.point))
                # 3rd, sort by snap_distance
                tmp_gdf.sort_values(by=['snap_distance'], inplace=True)
                # 4th, for each station, keep the link with the shortest snap distance
                closest_gdf = tmp.groupby(["station", "longitude", "latitude"]).first().reset_index()

                # add it to the matching result gdf
                pems_match_gdf = pd.concat([pems_match_gdf, closest_gdf],
                                           sort=False,
                                           ignore_index=True)

    WranglerLogger.debug(
        'finished matching PEMS stations to links using nearest sheildnum+direction match, '
        '{} rows with following fields:\n{}'.format(
            pems_match_gdf.shape[0], list(pems_match_gdf)
        ))

    return pems_match_gdf


def _geodesic_point_buffer(lat, lon, buffer_radius):
    """
    Create a buffer of point, based on point lat/lon
    """
    # Azimuthal equidistant projection
    proj_wgs84 = pyproj.Proj('+proj=longlat +datum=WGS84')
    aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)),
        proj_wgs84)
    buf = Point(0, 0).buffer(buffer_radius)  # distance in meters
    return Polygon(transform(project, buf).exterior.coords[:])


def _links_within_point_buffer(link_gdf, point_df, buffer_radius=25):
    """
    find the links that intersect within the buffer of each point
    """

    point_buffer_gdf = point_df.copy()

    # set the geometry to be the buffered area of each point and convert the data into a geodataframe
    point_buffer_gdf['geometry'] = point_buffer_gdf.apply(
        lambda x: _geodesic_point_buffer(x.latitude, x.longitude, buffer_radius),
        axis=1)
    point_buffer_gdf = gpd.GeoDataFrame(point_buffer_gdf,
                                        geometry=point_buffer_gdf['geometry'],
                                        crs=LAT_LONG_EPSG)
    point_buffer_gdf = point_buffer_gdf.to_crs(epsg=LAT_LONG_EPSG)
    link_gdf = link_gdf.to_crs(epsg=LAT_LONG_EPSG)
    point_buffer_link_df = gpd.sjoin(link_gdf,
                                     point_buffer_gdf[['geometry', 'type']],
                                     how='left',
                                     op='intersects')

    point_buffer_link_df = point_buffer_link_df.loc[point_buffer_link_df['type'].notnull()]

    return point_buffer_link_df['shstReferenceId'].tolist()


def pems_match_ft(pems_station_df, link_gdf, pems_type_roadway_crosswalk):
    """
    match each PEMS station to a base roadway link of the same facility type

    pems_station_df has to have fields 'latitude' and 'longitude'
    link_gdf has to be in lat/lon epsg (4326)
    """
    WranglerLogger.info('Starting matching PEMS stations to links of the same facility type')

    pems_stations_ft_matched_gdf = gpd.GeoDataFrame()

    for i in range(len(pems_station_df)):
        row_df = pems_station_df.iloc[[i]]
        station_type = row_df['type'].iloc[0]
        station_num = row_df['station'].iloc[0]
        WranglerLogger.debug('...process station {}, type {}'.format(station_num, station_type))
        # get all links with the same facility types
        links = link_gdf.loc[(link_gdf.roadway.isin(pems_type_roadway_crosswalk[station_type]))]
        # get all links within the buffer of the station
        links_within_buffer = _links_within_point_buffer(links, row_df, 100)

        # add other link attributes to the links within the buffer
        links_within_buffer_gdf = link_gdf.loc[link_gdf.shstReferenceId.isin(links_within_buffer)]
        links_within_buffer_gdf.loc[:, 'station'] = station_num
        links_within_buffer_gdf.loc[:, 'type'] = station_type
        links_within_buffer_gdf.loc[:, 'latitude'] = row_df['latitude'].iloc[0]
        links_within_buffer_gdf.loc[:, 'longitude'] = row_df['longitude'].iloc[0]
        row_df.loc[:, 'geometry'] = row_df.apply(lambda x: Point(x.longitude, x.latitude), axis=1)
        links_within_buffer_gdf['point'] = row_df['geometry'].iloc[0]

        # calculate the distance between the station point and each matched link
        links_within_buffer_gdf['snap_distance'] = links_within_buffer_gdf.geometry.distance(
            gpd.GeoSeries(links_within_buffer_gdf.point)
        )
        # sort by distance, and groupby to keep the link with the shortest distance
        links_within_buffer_gdf.sort_values(by=['snap_distance'], inplace=True)
        closest_link_gdf = links_within_buffer_gdf.groupby(['station', 'longitude', 'latitude']).first().reset_index()

        pems_stations_ft_matched_gdf = pd.concat([pems_stations_ft_matched_gdf, closest_link_gdf],
                                                 sort=False,
                                                 ignore_index=True)

    WranglerLogger.info('finished matching PEMS stations')
    return pems_stations_ft_matched_gdf


def gtfs_point_shapes_to_link_gdf(gtfs_shape_file, gtfs_name):

    """
    Create a transit line geodataframe from 'shapes.txt' in GTFS data, which contains lat/lon of transit stops/stations:
        shape_id         : line name
        shape_pt_sequence: sequence of stop/station in the line
        shape_pt_lat     : stop/station latitude
        shape_pt_lon     : stop/station longitude

    """
    WranglerLogger.debug('read shape.txt')
    gtfs_shape_df = pd.read_csv(gtfs_shape_file)

    try:
        # get station/stop points from lat/lon
        gtfs_geometry = [Point(xy) for xy in zip(gtfs_shape_df.shape_pt_lon, gtfs_shape_df.shape_pt_lat)]
        gtfs_shape_gdf = gpd.GeoDataFrame(gtfs_shape_df, geometry = gtfs_geometry)

        # group stations/stops to get line 
        line_df = gtfs_shape_gdf.groupby(['shape_id'])['geometry'].apply(lambda x:LineString(x.tolist())).reset_index()
        line_gdf = gpd.GeoDataFrame(line_df, geometry = 'geometry')

        return line_gdf  

    except:
        WranglerLogger.info('failed to create line geojson')
        return None


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
    read all shst extraction geofeather file
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


def read_shst_matched(path, suffix):
    """
    read shst match results
    """

    shst_matched_gdf = pd.DataFrame()

    shst_matched_files = glob.glob(path + "/**/" + suffix, recursive=True)

    # raise an error if no files are found
    if len(shst_matched_files) == 0:
        raise FileNotFoundError(errno.ENOENT, path + "/**/" + suffix)

    WranglerLogger.debug('...start reading shst matching result data')
    for shst_matched in shst_matched_files:
        WranglerLogger.debug('......reading shst matching result {}'.format(shst_matched))
        new_gdf = gpd.read_file(shst_matched)
        new_gdf['source'] = shst_matched
        shst_matched_gdf = pd.concat([shst_matched_gdf, new_gdf],
                                     ignore_index=True,
                                     sort=False)
    WranglerLogger.debug('...finished reading shst matching result')
    WranglerLogger.debug('shst matched head: {}\n'.format(shst_matched_gdf.head(10)))

    return shst_matched_gdf


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

    new_cc_gdf.set_crs(epsg = 26915)
    new_cc_gdf = new_cc_gdf.to_crs(epsg=4326)
    new_centroid_gdf.set_crs(epsg = 26915)
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
        gdf_proj = gdf.to_crs(epsg = LAT_LONG_EPSG)
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
