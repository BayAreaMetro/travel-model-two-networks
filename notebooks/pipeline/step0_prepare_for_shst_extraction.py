USAGE = """

Partitions the region geographically into 14 sub-regions. 
SharedStreets extraction requires a polygon boundary as input. 
Running extraction with the entire Bay Area as the boundary will run out of space.

Input: county_5m - Copy.shp: polygons of Bay Area counties
Output: 14 geojson files
"""

from methods import *
import geopandas as gpd
import os

# input/output directory and files
NETWORKS_DIR = r'M:\Development\Travel Model Two\Networks\TM2_network_v13'
INPUT_DIR = os.path.join(NETWORKS_DIR, 'external', 'step0_boundaries')
COUNTY_POLYGON = 'county_5m - Copy.shp'
OUTPUT_DIR = os.path.join(NETWORKS_DIR, 'external', 'step0_boundaries', 'modified')

# # EPSG requirement
# TARGET_EPSG = 4326


if __name__ == '__main__':

    # read polygon boundry
    county_polys_gdf = gpd.read_file(os.path.join(INPUT_DIR, COUNTY_POLYGON))
    # "../../data/external/county_boundaries/county_5m - Copy.shp")

    print('Input county boundary file uses projection: ' + str(county_polys_gdf.crs))

    # project to lat-long
    county_polys_gdf = county_polys_gdf.to_crs(epsg=LAT_LONG_EPSG)
    print('converted to projection: ' + str(county_polys_gdf.crs))
    # print(county_polys_gdf)

    # create output folder if not exist
    if not os.path.exists(OUTPUT_DIR):
        print('create output folder')
        os.makedirs(OUTPUT_DIR)

    # export polygon to geojson for shst extraction
    i = 1
    for g in county_polys_gdf.geometry:
        print('exporting boundary file number: ' + str(i))

        boundary_gdf = gpd.GeoDataFrame({"geometry": gpd.GeoSeries(g)})

        boundary_gdf.to_file(os.path.join(OUTPUT_DIR, 'boundary_' + str(i) + '.geojson'),
                             driver="GeoJSON")
        i += 1
