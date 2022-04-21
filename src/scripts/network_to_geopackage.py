import argparse
import pandas as pd
import geopandas as gpd
from itertools import compress


USAGE = """"
    Reads shape geojson and attribute json of the travel model network and outputs a combined geopackage
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("shape_path",               help="Location of the shape geojson file")
    parser.add_argument("attribute_path",           help="Location of the attribute json file")
    parser.add_argument("output_dir",               help="Directory for the output geodatabase")
    args = parser.parse_args()

    #Data Reads
    tm_shape_gdf = gpd.read_file(args.shape_path, driver='GeoJSON')
    print('-- Shape load successful')
    tm_attrs_df = pd.read_json(args.attribute_path)
    print('-- Attribute load successful')

    #Merge Attributes to Shapes
    tm_attrs_gdf = pd.merge(tm_shape_gdf, tm_attrs_df, how="left", on="id", indicator=True)
    
    matches = tm_attrs_gdf[tm_attrs_gdf['_merge'] == 'both'].shape[0]
    non_matches = tm_attrs_gdf[tm_attrs_gdf['_merge'] == 'left_only'].shape[0]

    print('-- ' + str(matches) + ' matching links and ' + str(non_matches) + ' non-matching links.')

    #Filter List Elements from Attributes
    def has_list(x):
        return str(any(isinstance(i, list) for i in x))

    list_cols = tm_attrs_gdf.apply(has_list)
    list_cols = list_cols.values
    list_cols = [sub.replace('False','') for sub in list_cols]
    list_cols = list(map(bool, list_cols))
    list_cols = list(compress(list(tm_attrs_gdf.columns), list_cols))

    for i in list_cols:
        tm_attrs_gdf[i] = tm_attrs_gdf[i].apply(lambda x: str(x))
        
    print("-- " + str(list_cols) + " converted to string.")
    tm_attrs_gdf = tm_attrs_gdf.drop(['_merge'], axis=1)

    #Export to GeoPackage
    tm_attrs_gdf.to_file(args.output_dir + "tm_attrs.gpkg", driver="GPKG", layer='network_links')
    print('Network exported to geopackage.')