USAGE = """
  Converts geofeather (intermediate format that is fast to read/right) to geopackage (for visualization)
  Specify a directory with one or more .feather/.crs files
  As well as an output geopackage file.

  As part of the conversion, a couple extra steps are performed:
  1) Columns that have data types that will cause the geopackage layer write to fail are
     converted to strrings.  Currrently these are: categorical and numpy.ndarrrays
  2) Columns are renamed to play well with geopackages

"""

import argparse, datetime, glob, os, sys
import geopandas
import numpy
import geofeather
import pandas as pd
from network_wrangler import WranglerLogger, setupLogging

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input_geofeather_dir', help='Input directory for geofeather files')
    parser.add_argument('output_gpkg',   help='Output GeoPackage file')

    # setup logging
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 50000)
    LOG_FILENAME = "convert_geofeather_to_geopackage_{}.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M"))
    setupLogging(LOG_FILENAME)

    args = parser.parse_args()

    # check input files exist
    file_list = glob.glob(os.path.join(args.input_geofeather_dir,"*.feather"))
    if len(file_list)==0:
        error_str = 'No .feather files found in [{}]'.format(args.input_geofeather_dir)
        WranglerLogger.fatal(error_str)
        sys.exit(error_str)

    # check output geopackage doesn't exist
    if os.path.exists(args.output_gpkg):
        error_str = 'output_gpkg [{}] exists already'.format(args.output_gpkg)
        WranglerLogger.fatal(error_str)
        sys.exit(error_str)

    # read geofeather files and output them to geopackage
    for full_filename in file_list:
        WranglerLogger.info("Reading {}".format(full_filename))
        input_gdf = geofeather.from_geofeather(full_filename)
        WranglerLogger.info("Initial dtypes:\n{}".format(input_gdf.dtypes))

        # downcast num types
        gdf_downcast = input_gdf.select_dtypes(include=[numpy.number]).apply(pd.to_numeric, downcast='signed')
        gdf_downcast = gdf_downcast.select_dtypes(include=[numpy.number]).apply(pd.to_numeric, downcast='unsigned')
        input_gdf[gdf_downcast.columns] = gdf_downcast
        del gdf_downcast
        WranglerLogger.info("After numeric downcast dtypes:\n{}".format(input_gdf.dtypes))
        WranglerLogger.info("Head:\n{}".format(input_gdf.head()))

        # Categorical columns are invalid for geopackages so convert to string.
        # Additionally, some object columns are interpretted as lists or dicts and will fail to write
        # so try to figurer out what these are and cast those columns to strings
        first_row = input_gdf.iloc[0]
        for col in sorted(list(input_gdf.columns)):
            if str(input_gdf.dtypes[col])=="category":
                WranglerLogger.info("Converting category column {} to string".format(col))
                # convert to string for output
                input_gdf[col] = input_gdf[col].astype(str)
            elif str(input_gdf.dtypes[col])=="object":
                # some of these are already strings; don't waste time/memory on all those columns
                # rather, try to figurer out which ones will error
                WranglerLogger.info("object-dtype column {} first_row val is {}; type is {}".format(
                    col, first_row[col], type(first_row[col])
                ))
                if type(first_row[col]) == numpy.ndarray:
                    WranglerLogger.info(" => Converting array column {} to string".format(col))
                    # input_gdf[col] = input_gdf[col].apply(str) # this is very slow
                    input_gdf[col] = input_gdf[col].astype(str)

        # Note: For maximum interoperability, start your database identifiers (table names, column names, etc.) with a 
        # lowercase character and only use lowercase characters, numbers 0-9, and underscores (_).
        prev_col_name = ""
        for col in sorted(list(input_gdf.columns)):
            col_new = col
            # leave this one
            if col=="geometry": continue

            WranglerLogger.info("Column {:<20} has type {}".format(col, input_gdf.dtypes[col]))            
            # fix column names for geopackage
            if ':' in col:   # rename columns with ":"
                col_new = col.replace(':','_')
                input_gdf.rename(columns={col:col_new}, inplace=True)
                WranglerLogger.info("    => renamed to {}".format(col_new))
            elif col.startswith("_"): # don't start with _; prepend alpha character 'x'
                col_new = 'x' + col
                input_gdf.rename(columns={col:col_new}, inplace=True)
                WranglerLogger.info("    => renamed to {}".format(col_new))
            elif col == 'id':
                col_new = 'id_orig'
                input_gdf.rename(columns={col:col_new}, inplace=True)
                WranglerLogger.info("    => renamed to {}".format(col_new))
            # todo fix this upstream. Also this isn't robust to more than 2
            elif col.lower() == prev_col_name:
                col_new = col+"_2"
                input_gdf.rename(columns={col:col_new}, inplace=True)
                WranglerLogger.info("    => DUPLICATE COL: renamed to {}".format(col_new))

            prev_col_name = col_new.lower()

        WranglerLogger.info("dtypes:\n{}".format(input_gdf.dtypes))

        # layer name is the filename without ".feather" and "_" instead of "."
        (dir,filename) = os.path.split(full_filename)
        layer_name = filename.replace(".feather","")
        layer_name = layer_name.replace(".","_")
        input_gdf.to_file(args.output_gpkg, layer=layer_name, driver="GPKG")
        WranglerLogger.info("Wrote {} to {}".format(layer_name, args.output_gpkg))

        del input_gdf

    WranglerLogger.info("Done")