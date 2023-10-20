import argparse, os, sys

# this requires the user to run this using a python environment with arcpy installed
# typically this means setting your PATH to C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
import arcpy

USAGE = """
  Converts GeoPackage (https://www.geopackage.org) vector features to a File Geodatabase with those features.

  See this article on GeoPackages
  https://www.esri.com/arcgis-blog/products/product/data-management/how-to-use-ogc-geopackages-in-arcgis-pro/

  GeoPackages are a helpful intermediate step because they are
  * writeable by Fiona (e.g. network_wrangler/lasso)
    unlike GeoDatabases which involve installing an ESRI driver
  * don't have shapefile limitations (e.g. file size limit, attribute field count, name width limit,
    lack of null value support for most field types)

  However, they have these drawbacks:
  * ArcGIS Pro won't publish them to web
  * Tableau won't read them

  This utility therefore converts an GeoPackage to a File Geodatabase.
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input_gpkg',   help='Input GeoPackage file')
    parser.add_argument('output_gdb',   help='Output Geodatabase')
    parser.add_argument('feature',      help='Feature(s) to copy from input_gpkg to output_gdb. If none specified, will do all.',
                                        nargs='*')
    args = parser.parse_args()

    # verify input_gpkg file exists
    if not os.path.exists(args.input_gpkg):
        error_str = 'input_gpkg [{}] doesn\'t exist'.format(args.input_gpkg)
        print(error_str)
        sys.exit(error_str)
    
    # and output_gdb does not
    if os.path.exists(args.output_gdb):
        error_str = 'output_gdb [{}] exists already'.format(args.output_gdb)
        print(error_str)
        sys.exit(error_str)

    # arcpy likes forward slashes instead of back slashes
    input_geopackage = args.input_gpkg
    input_geopackage = input_geopackage.replace('\\', '/')

    # check the contents of input_geopackage
    gpkg_features = []
    walk = arcpy.da.Walk(input_geopackage)
    for path, names, filenames in walk:
        for fname in filenames:
            d = arcpy.Describe(os.path.join(path, fname))
            if str(d.dataType) == 'FeatureClass':
                gpkg_features.append(fname)

    # copy specified features, or all if none are specified
    copy_features = []
    if args.feature:
        copy_features = args.feature
    else:
        copy_features = gpkg_features
    
    if len(gpkg_features) == 0:
        error_str = 'No features found in input_gpkg {}'.format(args.input_gpkg)
        print(error_str)
        sys.exit(error_str)

    print('Features in {}:'.format(args.input_gpkg))
    print(*gpkg_features, sep=' \n')

    # arcpy likes forward slashes instead of back slashes
    output_geodatabase = os.path.abspath(args.output_gdb)
    output_geodatabase_dir, output_geodatabase_file = os.path.split(output_geodatabase)
    output_geodatabase_dir = output_geodatabase_dir.replace('\\', '/')
    try:
        print('Creating geodatabase [{}]'.format(output_geodatabase))
        arcpy.CreateFileGDB_management(output_geodatabase_dir, output_geodatabase_file)

        for feature in copy_features:
            # check if it's there
            if feature not in gpkg_features:
                print("Feature {} not in gpkg_features; skipping".format(feature))
                continue

            # 'main.' is a geopackage thing and it isn't a helpful prefix -- drop it
            dest_feature = feature.replace("main.","")
            dest_feature = arcpy.ValidateTableName(dest_feature)

            print('Copying feature {} to {}'.format(feature, dest_feature)) 
            arcpy.management.CopyFeatures(os.path.join(input_geopackage, feature),
                                          os.path.join(output_geodatabase_dir, output_geodatabase_file, dest_feature))
    
    except Exception as e:
        print("Exception: {}".format(e))

    print('Success')