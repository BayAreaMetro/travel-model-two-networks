USAGE = """
  Tests disk I/O by reading and writing a geopackage and a geojson file since I'm finding it *verrrry slow*
"""

import argparse, datetime, logging, sys
import geopandas as gpd

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('input_file',   help='Input file (gpkg or geojson)')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("test_io_{}.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))),
                  logging.StreamHandler()]
    )

    if args.input_file.endswith('.gpkg'):
        file_copy = args.input_file.replace(".gpkg","_copy.gpkg")

        import fiona
        layers = fiona.listlayers(args.input_file)
        logging.info("Reading gpkg {} with layers {}".format(args.input_file, layers))
        gdfs = {}
        for layer in layers:
            logging.info("Reading layer {}".format(layer))
            gdfs[layer] = gpd.GeoDataFrame.from_file(args.input_file, layer=layer)
            logging.info("... complete; read {} rows".format(len(gdfs[layer])))

        for layer in layers:
            logging.info("Writing layer {} to {}".format(layer, file_copy))
            gdfs[layer].to_file(file_copy, driver='GPKG', layer=layer)
            logging.info("... complete")
    
    elif args.input_file.endswith('.geojson'):
        file_copy = args.input_file.replace(".geojson","_copy.geojson")

        logging.info("Reading geojson file {}".format(args.input_file))
        gdf = gpd.GeoDataFrame.from_file(args.input_file, driver='GeoJSON')
        logging.info("... complete; read {} rows".format(len(gdf)))

        logging.info("Writing layer {} to {}".format(layer, file_copy))
        gdf.to_file(file_copy, driver='GeoJSON')
        logging.info("... complete")

    sys.exit()