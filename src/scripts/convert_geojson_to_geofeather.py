USAGE = """
  Converts geojson (e.g. sharedstreets extracts) to geofeather
  Specify a directory with one or more .geojson files
  Will output X.geojson to X.feather

  Creates a log to make timing clear
"""

import argparse, datetime, glob, os, sys
import geopandas
from network_wrangler import WranglerLogger, setupLogging

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input_geojson_dir', help='Input directory for geojson files')

    args = parser.parse_args()

    # setup logging
    LOG_FILENAME = os.path.join(
        args.input_geojson_dir,
        "convert_geojson_to_geofeather_{}.info.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M")),
    )
    setupLogging(LOG_FILENAME)

    # check input files exist
    file_list = glob.glob(os.path.join(args.input_geojson_dir,"*.geojson"))
    if len(file_list)==0:
        error_str = 'No .geojson files found in [{}]'.format(args.input_geojson_dir)
        WranglerLogger.fatal(error_str)
        sys.exit(error_str)

    # read geojson files and output them to geofeather
    for full_filename in file_list:
        WranglerLogger.info("Reading {}".format(full_filename))
        input_gdf = geopandas.read_file(full_filename)
        WranglerLogger.info("dtypes:\n{}".format(input_gdf.dtypes))
        output_geofeather = full_filename.replace('.geojson','.feather')

        input_gdf.to_feather(output_geofeather)
        WranglerLogger.info("Wrote to {}".format(output_geofeather))

    WranglerLogger.info("Done")