USAGE = """
  Converts geojson (e.g. sharedstreets extracts) to geopackage
  Specify a directory with one or more .geojson files
  As well as an output geopackage file

"""

import argparse, glob, os, sys
import geopandas

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input_geojson_dir', help='Input directory for geojson files')
    parser.add_argument('output_gpkg',   help='Output GeoPackage file')

    args = parser.parse_args()

    # check input files exist
    file_list = glob.glob(os.path.join(args.input_geojson_dir,"*.geojson"))
    if len(file_list)==0:
        error_str = 'No .geojson files found in [{}]'.format(args.input_geojson_dir)
        print(error_str)
        sys.exit(error_str)

    # check output geopackage doesn't exist
    if os.path.exists(args.output_gpkg):
        error_str = 'output_gpkg [{}] exists already'.format(args.output_gpkg)
        print(error_str)
        sys.exit(error_str)

    # read geojson files and output them to geopackage
    for full_filename in file_list:
        print("Reading {}".format(full_filename))
        input_gdf = geopandas.read_file(full_filename)
        print("dtypes:\n{}".format(input_gdf.dtypes))

        # layer name is the filename without ".geojson" and "_" instead of "."
        (dir,filename) = os.path.split(full_filename)
        layer_name = filename.replace(".geojson","")
        layer_name = layer_name.replace(".","_")
        input_gdf.to_file(args.output_gpkg, layer=layer_name, driver="GPKG")
        print("Wrote {} to {}".format(layer_name, args.output_gpkg))

    print("Done")