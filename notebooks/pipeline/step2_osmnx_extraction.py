USAGE = """

Extracts complete OSM attributes using OSMNX.
Input: polygon boundary file for the region
Output: geojson files from OSMNX
"""

import geopandas as gpd
import json
import networkx as nx
import pandas as pd
import osmnx as ox
import os
import time

# from methods import point_df_to_geojson
# from methods import link_df_to_geojson
# import methods
from methods import *

# input/output directory and files
NETWORKS_DIR = r'M:\Development\Travel Model Two\Networks\TM2_network_v13'
INPUT_DIR = os.path.join(NETWORKS_DIR, 'external', 'step0_boundaries')
COUNTY_POLYGON = 'county_5m - Copy.shp'
OUTPUT_DIR = os.path.join(NETWORKS_DIR, 'external', 'step2_osmnx_extraction')

if __name__ == '__main__':

    # read polygon boundary
    county_polys_gdf = gpd.read_file(os.path.join(INPUT_DIR, COUNTY_POLYGON))
    print('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(epsg=LAT_LONG_EPSG)
    print('converted to projection: ' + str(county_polys_gdf.crs))

    # dissolve into one polygon (osmnx extraction doesn't have the area limitation as ShSt extraction)
    print('dissolve into one polygon')
    boundary = county_polys_gdf.geometry.unary_union

    # OSM extraction
    print('starting osmnx extraction')
    # starting time
    start = time.time()

    G_drive = ox.graph_from_polygon(boundary, network_type='all', simplify=False)

    # end time
    end = time.time()
    print('finished osmnx extraction, tool {} minutes'.format((end - start)/60))

    print('getting links and nodes')
    link_gdf = ox.graph_to_gdfs(G_drive, nodes=False, edges=True)
    print('link_gdf has {} records, with columns: \n{}'.format(link_gdf.shape[0],
                                                               list(link_gdf)))
    node_gdf = ox.graph_to_gdfs(G_drive, nodes=True, edges=False)
    print('node_gdf has {} records, with columns: \n{}'.format(node_gdf.shape[0],
                                                               list(node_gdf)))

    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        print('create output folder')
        os.makedirs(OUTPUT_DIR)

    print('writing out OSM links and nodes to geojson')
    start = time.time()

    # writing out OSM link data to geojson
    link_prop = link_gdf.drop("geometry", axis=1).columns.tolist()
    link_geojson = link_df_to_geojson(link_gdf, link_prop)
    with open(os.path.join(OUTPUT_DIR, 'link.geojson'), "w") as f:
        json.dump(link_geojson, f)

    # writing out OSM node data to geojson
    node_prop = node_gdf.drop("geometry", axis=1).columns.tolist()
    node_geojson = point_df_to_geojson(node_gdf, node_prop)
    with open(os.path.join(OUTPUT_DIR, 'node.geojson'), "w") as f:
        json.dump(node_geojson, f)

    end = time.time()
    print('finished writing out OSM links and nodes, took {} minutes'.format((end - start)/60))
