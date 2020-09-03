import pandas as pd
import numpy as np
import geopandas as gpd
import glob
from shapely.geometry import Point

shst_link_df_list = []

def extract_osm_link_from_shst_shape(x, shst_link_df_list):
    """
    if len(x.get("metadata").get("osmMetadata").get("waySections")) > 1:
        link_df = pd.DataFrame()
        all_link_df = pd.DataFrame(x.get("metadata").get("osmMetadata").get("waySections"))
        '''
        link_df = pd.Series(data = {"nodeIds" : all_link_df.nodeIds.tolist(),
                               "wayId" : all_link_df.wayId.tolist(),
                               "roadClass" : all_link_df.roadClass.tolist(),
                               "oneWay" : all_link_df.oneWay.tolist(),
                               "name" : all_link_df.name.tolist()})
        link_df = pd.DataFrame(data = link_df)
        print(link_df)
        '''
        for c in all_link_df.columns.tolist():
            attr_list = all_link_df[c].tolist()
            final = []
            if c == "nodeIds":
                attr_list = [item for sublist in attr_list for item in sublist]
            link_df[c] = [attr_list] * 1
                
    else:    
        link_df = pd.DataFrame(x.get("metadata").get("osmMetadata").get("waySections"))
    """
    link_df = pd.DataFrame(x.get("metadata").get("osmMetadata").get("waySections"))
    link_df["geometryId"] = x.get("metadata").get("geometryId")
    
    shst_link_df_list.append(link_df)

"""
def extract_osm_link_from_shst_shape_pdconcat(x):
    
    link_df = pd.DataFrame(x.get("metadata").get("osmMetadata").get("waySections"))
    link_df["geometryId"] = x.get("metadata").get("geometryId")
    
    shst_link_df = pd.concat([shst_link_df, link_df], sort = False, ignore_index = True)
""" 
    
def osm_link_with_shst_info(link_df, shst_gdf):
    """
    get complete osm links with shst info
    1. two way shst to two osm links
    2. add u, v node
    
    Parameters
    --------------
    osm link from shst extraction
    
    return
    --------------
    complete osm link with shst info
    """
    osm_link_gdf = pd.merge(link_df, 
                            shst_gdf.drop(["roadClass", "metadata", "source"], axis = 1),
                           how = "left",
                           left_on = "geometryId",
                           right_on = "id")
    
    return osm_link_gdf
    
    
def add_two_way_osm(link_gdf, osmnx_link):
    """
    for osm with oneway = False, add the reverse direction to complete
    
    Parameters
    ------------
    osm link from shst extraction, plus shst info
    
    return
    ------------
    complete osm link
    """
    osm_link_gdf = link_gdf.copy()
    osm_link_gdf["wayId"] = osm_link_gdf["wayId"].astype(int)
    osm_link_gdf.drop("name", axis = 1, inplace = True)
    
    osmnx_link_gdf = osmnx_link.copy()
    
    """
    osmnx_link_gdf.rename(columns = {"u" : "u_for_osm_join",
                                     "v" : "v_for_osm_join"},
                         inplace = True)
    """
    osmnx_link_gdf.drop_duplicates(subset = ["osmid"], inplace = True)
    osmnx_link_gdf.drop(["length", "u", "v", "geometry"], axis = 1, inplace = True)
    
    print("shst extraction has geometry: ", osm_link_gdf.id.nunique())
    print("osm links from shst extraction: ", osm_link_gdf.shape[0])
    
    osm_link_gdf["u"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[0]))
    osm_link_gdf["v"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[-1]))
    
    print("---joining osm shst with osmnx data---")
    osm_link_gdf = pd.merge(osm_link_gdf,
                            osmnx_link_gdf,
                            left_on = ["wayId"],
                            right_on = ["osmid"],
                            how = "left")
    
    """
    #join on osmid, u, v
    osm_link_gdf["u_for_osm_join"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[0]))
    osm_link_gdf["v_for_osm_join"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[1]))
    
    """
    
    #osm_link_gdf["oneWay"] = osm_link_gdf.apply(lambda x: True if True in [x.oneWay, x.oneway] else x.oneWay,
     #                                          axis = 1)
    
    reverse_osm_link_gdf = osm_link_gdf[(osm_link_gdf.oneWay == False) & 
                                        (osm_link_gdf.forwardReferenceId != osm_link_gdf.backReferenceId) & 
                                        (osm_link_gdf.u != osm_link_gdf.v)].copy()
    
    print("which includes two way links:", reverse_osm_link_gdf.shape[0])
    print("and they are geometrys: ", reverse_osm_link_gdf.id.nunique())
    
    reverse_osm_link_gdf.rename(columns = {"u" : "v",
                                          "v" : "u",
                                           #"u_for_osm_join" : "v_for_osm_join",
                                           #"v_for_osm_join" : "u_for_osm_join",
                                          "forwardReferenceId" : "backReferenceId",
                                          "backReferenceId" : "forwardReferenceId",
                                          "fromIntersectionId" : "toIntersectionId",
                                          "toIntersectionId" : "fromIntersectionId"},
                               inplace = True)
    
    reverse_osm_link_gdf["reverse_out"] = 1
    
    osm_link_gdf = pd.concat([osm_link_gdf, reverse_osm_link_gdf],
                            sort = False,
                            ignore_index = True)
    
    osm_link_gdf.rename(columns = {"forwardReferenceId" : "shstReferenceId",
                                 "geometryId" : "shstGeometryId"},
                      inplace = True)
    
    osm_link_gdf.drop("backReferenceId",
                     axis = 1,
                     inplace = True)
    """
    # join with osmnx
    print("---joining osm shst with osmnx data---")
    #col_before_join = osm_link_gdf.columns.tolist()
    osm_link_gdf = pd.merge(osm_link_gdf,
                            osmnx_link_gdf,
                            left_on = ["wayId"],#, "u_for_osm_join", "v_for_osm_join"],
                            right_on = ["osmid"],#, "u_for_osm_join", "v_for_osm_join"],
                           how = "left")
    """
    """
    succ_osm_link_gdf = osm_link_gdf[osm_link_gdf.osmid.notnull()].copy()
    print("-----number of matched osm------- :", succ_osm_link_gdf.shape[0])
    
    fail_osm_link_gdf = osm_link_gdf[osm_link_gdf.osmid.isnull()].copy()
    fail_osm_link_gdf = fail_osm_link_gdf[col_before_join].copy()
    print("-----number of un-matched osm-------:", fail_osm_link_gdf.shape[0])
    
    fail_osm_link_gdf["u_for_osm_join"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[-1]))
    fail_osm_link_gdf["v_for_osm_join"] = osm_link_gdf.nodeIds.apply(lambda x: int(x[-2]))
    
    fail_osm_link_gdf = pd.merge(fail_osm_link_gdf,
                            osmnx_link_gdf.drop("geometry", axis = 1),
                            left_on = ["wayId", "u_for_osm_join", "v_for_osm_join"],
                            right_on = ["osmid", "u_for_osm_join", "v_for_osm_join"],
                           how = "left")
    
    print("-----number of un-matched osm after rejoining-------:", fail_osm_link_gdf.shape[0])
    
    osm_link_gdf = pd.concat([succ_osm_link_gdf, fail_osm_link_gdf], ignore_index = True, sort = False)
    """
    
    
    print("after join, osm links from shst extraction: ", 
          len(osm_link_gdf), 
          " out of which there are ", 
          len(osm_link_gdf[osm_link_gdf.osmid.isnull()]), 
          " links that do not have osm info, due to shst extraction (default tile 181224) contains ", 
          osm_link_gdf[osm_link_gdf.osmid.isnull()].wayId.nunique(), 
          " osm ids that are not included in latest OSM extraction, e.g. private streets, closed streets.")
    print("after join, there are shst geometry # : ", osm_link_gdf.groupby(["shstReferenceId", "shstGeometryId"]).count().shape[0])
    
    return osm_link_gdf


def consolidate_osm_way_to_shst_link(osm_link):
    """
    if a shst link has more than one osm ways, aggregate info into one, e.g. series([1,2,3]) to cell value [1,2,3]
    
    Parameters
    ----------
    osm link with shst info
    
    return
    ----------
    shst link with osm info
    
    """
    osm_link_gdf = osm_link.copy()

    agg_dict = {"geometry" : lambda x: x.iloc[0],
                "u" : lambda x: x.iloc[0],
                "v" : lambda x: x.iloc[-1]}
    
    for c in ['link', 'nodeIds', 'oneWay', 'roadClass', 'roundabout', 'wayId', 'access', 'area', 'bridge',
              'est_width', 'highway', 'junction', 'key', 'landuse', 'lanes', 'maxspeed', 'name', 'oneway', 'ref', 'service', 
              'tunnel', 'width']:
        agg_dict.update({c : lambda x: list(x) if len(list(x)) > 1 else list(x)[0]})
    
    print("-----start aggregating osm segments to one shst link for forward links----------")
    forward_link_gdf = osm_link_gdf[osm_link_gdf.reverse_out == 0].copy()
    
    if len(forward_link_gdf) > 0:
        forward_link_gdf = forward_link_gdf.groupby(
                                        ["shstReferenceId", "id", "shstGeometryId", "fromIntersectionId", "toIntersectionId"]
                                        ).agg(agg_dict).reset_index()
        forward_link_gdf["forward"] = 1
    else:
        forward_link_gdf = None
    
    print("-----start aggregating osm segments to one shst link for backward links----------")
    
    backward_link_gdf = osm_link_gdf[osm_link_gdf.reverse_out==1].copy()
    
    if len(backward_link_gdf) > 0:
        agg_dict.update({"u" : lambda x: x.iloc[-1],
                     "v" : lambda x: x.iloc[0]})    

        backward_link_gdf = backward_link_gdf.groupby(
                                        ["shstReferenceId", "id", "shstGeometryId", "fromIntersectionId", "toIntersectionId"]
                                        ).agg(agg_dict).reset_index()
    else:
        backward_link_gdf = None
    
    shst_link_gdf = None
    
    if (forward_link_gdf is None):
        print("back")
        shst_link_gdf = backward_link_gdf
        
    if (backward_link_gdf is None):
        print("for")
        shst_link_gdf = forward_link_gdf
        
    if (forward_link_gdf is not None) and (backward_link_gdf is not None):
        print("all")
        shst_link_gdf = pd.concat([forward_link_gdf, backward_link_gdf],
                                  sort = False,
                                  ignore_index = True)
        
    shst_link_gdf = gpd.GeoDataFrame(shst_link_gdf,
                                    crs = {'init': 'epsg:4326'})
    
    return shst_link_gdf


def create_node_gdf(link_gdf):
    """
    create shst node gdf from shst geometry
    
    Paramters
    ---------
    link_gdf:  shst links with osm info
    
    return
    ---------
    shst nodes with osm info
    
    """
    print("-------start creating shst nodes--------")
    # geometry only matches for forward direction
    forward_link_gdf = link_gdf[link_gdf.forward == 1].copy()
    
    # create point geometry from shst linestring
    forward_link_gdf["u_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[0]), axis = 1)
    forward_link_gdf["v_point"] = forward_link_gdf.apply(lambda x: Point(list(x.geometry.coords)[-1]), axis = 1)
    
    # get from points
    point_gdf = forward_link_gdf[["u", "fromIntersectionId", "u_point"]].copy()
    
    point_gdf.rename(columns = {"u" : "osm_node_id",
                      "fromIntersectionId" : "shst_node_id",
                      "u_point" : "geometry"},
                    inplace = True)
    
    # append to points
    point_gdf = pd.concat([point_gdf, forward_link_gdf[["v", "toIntersectionId", "v_point"]].rename(columns = 
                     {"v" : "osm_node_id",
                      "toIntersectionId" : "shst_node_id",
                      "v_point" : "geometry"})],
                     sort = False,
                     ignore_index = True)
    
    # drop duplicates
    point_gdf.drop_duplicates(subset = ["osm_node_id", "shst_node_id"], inplace = True)
    
    point_gdf = gpd.GeoDataFrame(point_gdf,
                                 crs = {'init': 'epsg:4326'})
    
    return point_gdf



def link_df_to_geojson(df, properties):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    geojson = {"type":"FeatureCollection", "features":[]}
    for _, row in df.iterrows():
        feature = {"type":"Feature",
                   "properties":{},
                   "geometry":{"type":"LineString",
                               "coordinates":[]}}
        feature["geometry"]["coordinates"] = [[x, y] for (x,y) in list(row["geometry"].coords)]
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson


def point_df_to_geojson(df: pd.DataFrame, properties: list):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    
    geojson = {"type": "FeatureCollection", "features": []}
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": []},
        }
        feature["geometry"]["coordinates"] = [row["geometry"].x, row["geometry"].y]
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson

def fill_na(df_na):
    """
    fill str NaN with ""
    fill numeric NaN with 0
    """
    df = df_na.copy()
    num_col = list(df.select_dtypes([np.number]).columns)
    print("numeric columns: ", num_col)
    object_col = list(df.select_dtypes(['object']).columns)
    print("str columns: ", object_col)
    
    for x in list(df.columns):
        if x in num_col:
            df[x].fillna(0, inplace = True)
        elif x in object_col:
            df[x].fillna("", inplace = True)
    
    return df

def ox_graph(nodes_df, links_df):
    """
        create an osmnx-flavored network graph
        osmnx doesn't like values that are arrays, so remove the variables
        that have arrays.  osmnx also requires that certain variables
        be filled in, so do that too.
        Parameters
        ----------
        nodes_df : GeoDataFrame
        link_df : GeoDataFrame
        Returns
        -------
        networkx multidigraph
    """
    try:
        graph_nodes = nodes_df.drop(
                ["inboundReferenceId", "outboundReferenceId"], axis=1
            )
    except:
        graph_nodes = nodes_df

    graph_nodes.gdf_name = "network_nodes"
    graph_nodes['id'] = graph_nodes['osm_node_id']

    graph_links = links_df.copy()
    graph_links['id'] = graph_links['osm_link_id']
    graph_links['key'] = str(graph_links['osm_link_id'])+"_"+str(graph_links['model_link_id'])

    G = ox.gdfs_to_graph(graph_nodes, graph_links)

    return G

def identify_dead_end_nodes(links):
    """
    iteratively find the dead end in networks
    """
    
    A_B_df = pd.concat([links,
                       links.rename(columns = {"u":"v", "v":"u"})],
                      ignore_index = True,
                      sort = False)
    
    A_B_df.drop_duplicates(inplace = True)
    
    A_B_df =  A_B_df.groupby(["u"]).count().reset_index()
    
    single_node_list = A_B_df[A_B_df.v == 1].u.tolist()
    
    return single_node_list


def read_shst_extract(path, suffix):
    """
    read all shst extraction geojson file
    """
    shst_gdf = pd.DataFrame()
    
    shst_file = glob.glob(path + "**/" + suffix, recursive = True)
    print("----------start reading shst extraction data-------------")
    for i in shst_file:
        print("reading shst extraction data : ", i)
        new = gpd.read_file(i)
        new['source'] = i
        shst_gdf = pd.concat([shst_gdf, new],
                             ignore_index = True,
                             sort = False)
    print("----------finished reading shst extraction data-------------")
    
    return shst_gdf


def highway_attribute_list_to_value(x, highway_to_roadway_dict, roadway_hierarchy_dict):
    """
    clean up osm highway, and map to standard roadway
    """
    if type(x.highway) == list:
        value_list = list(set([highway_to_roadway_dict[c] for c in x.highway]))
        if len(value_list) == 1:
            if value_list[0] != "":
                return value_list[0]
            else:
                if type(x.roadClass) == list:
                    return highway_to_roadway_dict[x.roadClass[0].lower()]
                else:
                    return highway_to_roadway_dict[x.roadClass.lower()]

        else:
            ret_val = value_list[0]
            ret_val_level = roadway_hierarchy_dict[ret_val]
            for c in value_list:
                val_level = roadway_hierarchy_dict[c]
                if val_level < ret_val_level:
                    ret_val = c
                    ret_val_level = val_level
                else:
                    continue
            return ret_val
    else:
        if x.highway == "":
            return highway_to_roadway_dict[x.roadClass.lower()]
        else:
            return highway_to_roadway_dict[x.highway]