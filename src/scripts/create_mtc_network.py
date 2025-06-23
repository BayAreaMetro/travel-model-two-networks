USAGE = """

Create MTC base year networks from OSM.

Tested in June 2025 with:
  * network_wrangler, https://github.com/network-wrangler/network_wrangler/tree/hotfix-pandaspanderacompatibility

References:
  * network_wrangler\\notebook\\Create Network from OSM.ipynb
"""
import argparse
import datetime
import pathlib
import osmnx
import pandas as pd
import geopandas as gpd
import shapely

import network_wrangler
from network_wrangler import WranglerLogger

OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
BAY_AREA_COUNTIES = [
    'Alameda', 
    'Contra Costa', 
    'Marin',
    'Napa',
    'San Francisco',
    'San Mateo',
    'Santa Clara',
    'Solano',
    'Sonoma'
]

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

def get_min_lane_value(lane):
    """
    When multiple values are present, return the minimum value.
    """
    if isinstance(lane, list):
        return min(lane)
    return lane

def standardize_highway_value(links_gdf: gpd.GeoDataFrame):
    """Standardize the highway value in the links GeoDataFrame.

    Standardized values:
    - footway
    - residential

    Args:
        links_gdf (gpd.DataFrame): links from OSMnx with columns, highway, steps, and other OSM tags.

    """
    # make steps an attribute
    links_gdf['steps'] = False

    # steps -> footway, steps=True
    links_gdf.loc[links_gdf.highway == 'steps', 'steps'] = True
    links_gdf.loc[links_gdf.highway == 'steps', 'highway'] = 'footway'

    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 'steps' in x), 'steps'  ] = True
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 'steps' in x), 'highway'] = 'footway'
    return

def get_roadway_value(highway):
    """ 
    When multiple values are present, return the first one.
    """
    if isinstance(highway,list):
        WranglerLogger.debug(f"list: {highway}")
        return highway[0]
    return highway

def write_geodataframe_as_tableau_hyper(in_gdf, filename, tablename):
    """
    Write a GeoDataFrame to a Tableau Hyper file.
    See https://tableau.github.io/hyper-db/docs/guides/hyper_file/geodata

    This is kind of a bummer because it would be preferrable to write to something more standard, like
    geofeather or geoparquet, but Tableau doesn't support those formats yet.
    """
    WranglerLogger.info(f"write_geodataframe_as_tableau_hyper: {filename=}, {tablename=}")
    # make a copy since we'll be messing with the columns
    gdf = in_gdf.copy()

    import tableauhyperapi

    # Check if all entries in the geometry column are valid Shapely geometries
    is_valid_geometry = gdf['geometry'].apply(lambda x: isinstance(x, shapely.geometry.base.BaseGeometry))
    WranglerLogger.info(f"is_valid_geometry: \n{is_valid_geometry.value_counts()}")

    # Convert geometry to WKT format
    gdf['geometry_wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    # drop this column, we don't need it any longer
    gdf.drop(columns='geometry', inplace=True)

    table_def = tableauhyperapi.TableDefinition(tablename)
    # Inserter definition contains the column definition for the values that are inserted
    # The data input has two text values Name and Location_as_text
    inserter_definition = []

    # Specify the conversion of SqlType.text() to SqlType.tabgeography() using CAST expression in Inserter.ColumnMapping.
    # Specify all columns into which data is inserter in Inserter.ColumnMapping list. For columns that do not require any
    # transformations provide only the names
    column_mappings = []
  

    for col in gdf.columns:
        # geometry_wkt to be converted from WKT to geometry via column_mapping
        if col == 'geometry_wkt':
            table_def.add_column('geometry', tableauhyperapi.SqlType.tabgeography())
            # insert as geometry_wkt
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name='geometry_wkt', type=tableauhyperapi.SqlType.text(), nullability=tableauhyperapi.NOT_NULLABLE))
            # convert to geometry
            column_mappings.append(tableauhyperapi.Inserter.ColumnMapping(
                'geometry', f'CAST({tableauhyperapi.escape_name("geometry_wkt")} AS TABLEAU.TABGEOGRAPHY)'))
            continue

        if gdf[col].dtype == bool:
            table_def.add_column(col, tableauhyperapi.SqlType.bool())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.bool()))  
        elif gdf[col].dtype == int:
            table_def.add_column(col, tableauhyperapi.SqlType.int())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.int()))
        elif gdf[col].dtype == float:
            table_def.add_column(col, tableauhyperapi.SqlType.double())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.double()))
        else:
            table_def.add_column(col, tableauhyperapi.SqlType.text())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.text()))
        column_mappings.append(col)

    WranglerLogger.info(f"table_def={table_def}")
    WranglerLogger.info(f"column_mappings={column_mappings}")

    table_name = tableauhyperapi.TableName("Extract", tablename)
    with tableauhyperapi.HyperProcess(telemetry=tableauhyperapi.Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with tableauhyperapi.Connection(endpoint=hyper.endpoint, database=filename, 
                                        create_mode=tableauhyperapi.CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(table_def)

            with tableauhyperapi.Inserter(connection, table_def, columns=column_mappings, inserter_definition=inserter_definition) as inserter:

                inserter.add_rows(rows=gdf.itertuples(index=False, name=None))
                inserter.execute()


    WranglerLogger.info(f"GeoDataFrame written to {filename} as Tableau Hyper file.")

if __name__ == "__main__":

    osmnx.settings.use_cache = True
    osmnx.settings.cache_folder = OUTPUT_DIR / "osmnx_cache"
    osmnx.settings.log_file = True
    osmnx.settings.log_file_name = OUTPUT_DIR / "osmnx.log"
    osmnx.settings.useful_tags_way=OSM_WAY_TAGS.keys()

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("county", type=str, choices=['Bay Area'] + BAY_AREA_COUNTIES)
    args = parser.parse_args()

    INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_{args.county}_{NOW}.info.log"
    DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_{args.county}_{NOW}.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
    )

    counties = [args.county] if args.county != 'Bay Area' else BAY_AREA_COUNTIES
    for county in counties:
        # use network_type='all_public' for all edges
        # Use OXMnx to pull the network graph for a place.
        # See https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.graph.graph_from_place
        #
        # g is a [networkx.MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html#), 
        # a directed graph with self loops and parallel edges (muliple edges can exist between two nodes)
        WranglerLogger.info(f"Creating network for {county}...")
        g = osmnx.graph_from_place(f'{county}, California, USA', network_type='all')

        nodes_gdf, edges_gdf = osmnx.graph_to_gdfs(g)
        WranglerLogger.info(f"Graph has {len(nodes_gdf):,} nodes and {len(edges_gdf):,} edges")

        # When checking for uniqueness in uv, it looks like all of these are loops where
        # it would be fine to delete the longer one for the purposes of routing....so that's what we will do.
        links_gdf = edges_gdf.loc[edges_gdf.groupby(['u', 'v'])['length'].idxmin()].reset_index(drop=False)
        WranglerLogger.info(f"links_gdf has {len(links_gdf):,} links after dropping duplicates")

        # use A,B instead of u,v
        links_gdf.rename(columns={'u': 'A', 'v': 'B'}, inplace=True)

        standardize_highway_value(links_gdf)

        WranglerLogger.debug(f"2 links_gdf:\n{links_gdf}")
        WranglerLogger.debug(f"2 links_gdf.dtypes:\n{links_gdf.dtypes}")

        for col in links_gdf.columns:
            # report on value counts for non-unique columns
            if col not in ['geometry', 'A', 'B', 'name', 'width','osm_link_id', 'length']:
                WranglerLogger.info(f"column {col} has value_counts:\n{links_gdf[col].value_counts(dropna=False)}")

            # convert types
            if col in ['reversed', 'oneway']:
                # replace NaN with -1
                if len(links_gdf.loc[ pd.isnull(links_gdf[col])]) > 0:
                    links_gdf.loc[ pd.isnull(links_gdf[col]), col] = -1

                # for list, choose first value
                links_gdf[col] = links_gdf[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

                # convert to int
                links_gdf[col] = links_gdf[col].astype(int)
            # A, B are too big for int64, so convert to str
            # leave lanes to look at them
            # leave geometry as is
            elif col in ['A', 'B', 'name', 'width', 'osm_link_id', 'length', 'lanes']:
                links_gdf[col] = links_gdf[col].astype(str)
            elif col not in ['geometry']: 
                links_gdf[col] = links_gdf[col].astype(str)

    WranglerLogger.info(f"3 links_gdf:\n{links_gdf}")
    WranglerLogger.info(f"3 links_gdf.dtypes:\n{links_gdf.dtypes}")

    
    write_geodataframe_as_tableau_hyper(
        links_gdf, 
        OUTPUT_DIR/f"{args.county.replace(' ','_').lower()}_links.hyper", 
        f"{args.county.replace(' ','_').lower()}_links"
    )

    for col in nodes_gdf.columns:
        if col in ['highway','ref','railway']:
            nodes_gdf[col] = nodes_gdf[col].astype(str)

    WranglerLogger.info(f"1 nodes_gdf:\n{nodes_gdf}")
    WranglerLogger.info(f"1 nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")
    write_geodataframe_as_tableau_hyper(
        nodes_gdf, 
        OUTPUT_DIR/f"{args.county.replace(' ','_').lower()}_nodes.hyper", 
        f"{args.county}"
    )