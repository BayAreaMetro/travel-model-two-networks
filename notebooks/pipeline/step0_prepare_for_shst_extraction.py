USAGE = """

Partitions the region geographically into 14 sub-regions based on the INPUT_POLYGON
SharedStreets extraction requires a polygon boundary as input. 
Running extraction with the entire Bay Area as the boundary will run out of space.

Subsequently uses the docker python package to create a sharedstreets docker image (from the Dockerfile in this directory)
and runs the shared street extraction for each subregion.

set INPUT_DATA_DIR, OUTPUT_DATA_DIR environment variable
Input:  [INPUT_DATA_DIR]/external/step0_boundaries/cb_2018_us_county_5m_BayArea.shp: polygons of Bay Area counties
Output: [OUTPUT_DATA_DIR]/external/step0_boundaries/boundary_[01-14].json
        [OUTPUT_DATA_DIR]/external/step1_shst_extracts/mtc_[01-14].out.geojson
"""
import methods
import geopandas as gpd
import os, sys
from pyproj import CRS
from network_wrangler import WranglerLogger, setupLogging
from datetime import datetime

# input/output directory and files
INPUT_DATA_DIR         = os.environ['INPUT_DATA_DIR']
OUTPUT_DATA_DIR        = os.environ['OUTPUT_DATA_DIR']
INPUT_POLYGON          = os.path.join(INPUT_DATA_DIR, 'external', 'step0_boundaries', 'cb_2018_us_county_5m_BayArea.shp')
OUTPUT_BOUNDARY_DIR    = os.path.join(OUTPUT_DATA_DIR, 'external', 'step0_boundaries')
OUTPUT_SHSTEXTRACT_DIR = os.path.join(OUTPUT_DATA_DIR, 'external', 'step1_shst_extracts')

# EPSG requirement
lat_lon_epsg_str = 'epsg:{}'.format(str(methods.LAT_LONG_EPSG))
WranglerLogger.info('standard ESPG: ', lat_lon_epsg_str)

if __name__ == '__main__':
    # create output folder if it does not exist
    if not os.path.exists(OUTPUT_BOUNDARY_DIR):
        WranglerLogger.info('create output folder: {}'.format(OUTPUT_BOUNDARY_DIR))
        os.makedirs(OUTPUT_BOUNDARY_DIR)

    # setup logging
    LOG_FILENAME = os.path.join(
        OUTPUT_BOUNDARY_DIR,
        "step0_prepare_for_shst_extraction_{}.info.log".format(datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME, LOG_FILENAME.replace('info', 'debug'))

    # read polygon boundary
    county_polys_gdf = gpd.read_file(INPUT_POLYGON)

    WranglerLogger.info('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(CRS(lat_lon_epsg_str))
    WranglerLogger.info('converted to projection: ' + str(county_polys_gdf.crs))
    WranglerLogger.debug('county_polys_gdf: {}'.format(county_polys_gdf))

    # verify this variable is correct
    assert(len(county_polys_gdf) == methods.NUM_SHST_BOUNDARIES)
        
    # export polygon to geojson for shst extraction
    for row_index, row in county_polys_gdf.iterrows():
        boundary_gdf = gpd.GeoDataFrame({"geometry": gpd.GeoSeries(row['geometry'])})

        output_file = os.path.join(OUTPUT_BOUNDARY_DIR, 'boundary_{:02d}.geojson'.format(row_index+1))
        WranglerLogger.info('Creating boundary file  {}'.format(output_file))
        boundary_gdf.to_file(output_file, driver="GeoJSON")

    # set these based on create_docker_container_with_directory()
    if OUTPUT_DATA_DIR.startswith('E:'):
        output_mount_target = '/usr/e_volume'
    elif OUTPUT_DATA_DIR.startswith('C:/Users/{}'.format(os.environ['USERNAME'])):
        output_mount_target = '/usr/home'
    else:
        WranglerLogger.error('Only USERPROFILE dir and E: are currently supported for OUTPUT_DATA_DIR')
        sys.exit(1)

    # use docker python package (https://docker-py.readthedocs.io/en/stable/index.html) to run SharedStreets extraction
    (client, container) = methods.create_docker_container(mount_e=OUTPUT_DATA_DIR.startswith('E:'), mount_home=True)

    # run these and check exec_log[1] to if test mounts were successful
    # exec_log = container.exec_run("/bin/bash -c 'cd /usr/e_volume; ls -l'", stdout=True, stderr=True, stream=True)
    # exec_log = container.exec_run("/bin/bash -c 'cd /usr/home; ls -l'", stdout=True, stderr=True, stream=True)

    # we're going to need to cd into OUTPUT_DATA_DIR\external -- create that path (on UNIX)
    output_data_dir_list = OUTPUT_DATA_DIR.split(os.path.sep)  # e.g. ['E:','tm2_network_version13']
    if OUTPUT_DATA_DIR.startswith('E:'):
        # drop the E: part only
        output_data_dir_list = output_data_dir_list[1:]
    elif OUTPUT_DATA_DIR.startswith('C:/Users/{}'.format(os.environ['USERNAME'])):
        # drop the C:/Users/[USERRNAME]
        output_data_dir_list = output_data_dir_list[4:]

    # prepare the path to cd into (OUTPUT_DATA_DIR\external) -- [output_mount_target]\[rest of OUTPUT_DATA_DIR]\external
    output_data_dir_list.insert(0, output_mount_target)
    output_data_dir_list.append('external')
    WranglerLogger.info('output_data_dir_list: {}'.format(output_data_dir_list))

    LINUX_SEP = '/'
    docker_external_path = LINUX_SEP.join(output_data_dir_list)

    # create output folder if it does not exist
    if not os.path.exists(OUTPUT_SHSTEXTRACT_DIR):
        WranglerLogger.info('create output folder: {}'.format(OUTPUT_SHSTEXTRACT_DIR))
        os.makedirs(OUTPUT_SHSTEXTRACT_DIR)

    # do the shared street extraction
    for boundary_num in range(1, methods.NUM_SHST_BOUNDARIES+1):
        command = ("/bin/bash -c 'cd {}; shst extract step0_boundaries/boundary_{:02d}.geojson "
                   "--out=step1_shst_extracts/mtc_{:02d}.geojson --metadata --tile-hierarchy=8 --tiles'".format(
                       docker_external_path,boundary_num,boundary_num))
        WranglerLogger.info('Executing docker command: {}'.format(command))
        
        (exec_code,exec_output) = container.exec_run(command, stdout=True, stderr=True, stream=True)
        while True:
            try:
                line = next(exec_output)
                # note: this looks a little funny because it's a byte string
                WranglerLogger.debug(line.strip())
            except StopIteration:
                # done
                WranglerLogger.info('...Completed command')
                break
    
    WranglerLogger.info('stopping container {}'.format(container.name))
    container.stop()
    client.containers.prune()
    WranglerLogger.info('complete')
