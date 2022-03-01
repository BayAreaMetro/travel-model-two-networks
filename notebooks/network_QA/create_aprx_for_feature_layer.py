# --------------------------------------------------------------------------------
# This script create the ArcGIS Pro project (.aprx) that underlies the TM2 QA web map for each county
# This is not a stand alone script
# I start from manually starting a blank project 
# and run the following code in the python window within ArcGIS Pro
# 
# --------------------------------------------------------------------------------

import arcpy

# county list - note the underscores
county_list = ["Alameda", "Contra_Costa", "Marin",  "Napa",  "San_Francisco",  "San_Mateo",  "Santa_Clara",  "Solano",  "Sonoma"]
#county_list = ["Alameda"]

for CountyName in county_list:

    # This script uses the keyword current - it should be run from the Python window within ArcGIS Pro 
    # To make this a stand alone script, I can probably just specify a project in between the quotes e.g. aprx = arcpy.mp.ArcGISProject(r"C:\Projects\etc\etc.aprx"), but I haven't tested it.
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    aprxMap = aprx.listMaps("Map")[0] 


    # add transit layers
    # ----------------------------
    # transit links
    lyrToAdd = r"M:\Data\GIS layers\TM2_Networks\model_net.gdb\transit_links"
    aprxMap.addDataFromPath(lyrToAdd)

    # import symbology 
    arcpy.management.ApplySymbologyFromLayer("transit_links", r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\Symbology_layers\transit_links.lyrx", None, "MAINTAIN")

    # fare zones
    lyrToAdd = r"M:\Data\GIS layers\TM2_Networks\model_net.gdb\nodes"
    aprxMap.addDataFromPath(lyrToAdd)

    # apply definition query (TANA, or non TANA)
    l = aprxMap.listLayers("nodes")[0]
    l.definitionQuery = "farezone <> 0"
    # rename layer
    for lyr in aprxMap.listLayers():
        print(lyr.name)
        if lyr.name=="nodes":
           layerName = str(lyr.name)
           lyr.name = lyr.name.replace(layerName, "nodes_farezone")
           print(f"Layer renamed to: {lyr.name}")  

    # import symbology 
    arcpy.management.ApplySymbologyFromLayer("nodes_farezone", r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\Symbology_layers\nodes_farezone.lyrx", None, "MAINTAIN")

    # add TM2 links layers - TANA and non TANA
    # ----------------------------
    
    tm2_layer_list = ["TANA", "nonTANA"]
    
    for TANAnonTANA in tm2_layer_list:

        lyrToAdd = r"M:\Data\GIS layers\TM2_Networks\model_net.gdb\links_" + CountyName
        aprxMap.addDataFromPath(lyrToAdd)

        # apply definition query (TANA, or non TANA)
        l = aprxMap.listLayers("links_" + CountyName)[0]
        if TANAnonTANA =="TANA": 
            l.definitionQuery = "cntype = 'TANA'"
        if TANAnonTANA =="nonTANA": 
            l.definitionQuery = "cntype <> 'TANA'"

        # rename layer to TANA or non TANA
        for lyr in aprxMap.listLayers():
            print(lyr.name)
            if lyr.name=="links_" + CountyName:
               layerName = str(lyr.name)
               lyr.name = lyr.name.replace(layerName, "links_" + CountyName + " " + TANAnonTANA)
               print(f"Layer renamed to: {lyr.name}")

        # import symbology
        arcpy.management.ApplySymbologyFromLayer("links_" + CountyName + " " + TANAnonTANA, r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\Symbology_layers\links_" + TANAnonTANA + ".lyrx", None, "MAINTAIN")


    # add maz and taz layers
    # ----------------------------
    # maz
    lyrToAdd = (r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\mazstazs_by_county\mazs_" + CountyName + ".shp") 
    aprxMap.addDataFromPath(lyrToAdd)

    # taz
    lyrToAdd = (r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\mazstazs_by_county\tazs_" + CountyName + ".shp") 
    aprxMap.addDataFromPath(lyrToAdd)


    # import symbology
    arcpy.management.ApplySymbologyFromLayer("tazs_" + CountyName, r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\Symbology_layers\tazs_TM2_v2_2.lyrx", None, "MAINTAIN")

    arcpy.management.ApplySymbologyFromLayer("mazs_" + CountyName, r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\Symbology_layers\mazs_TM2_v2_2.lyrx", None, "MAINTAIN")

    print(f"Done adding the mazs and tazs layers.")

    # re-order the layers
    print(f"Re-ordering the layers...")

    reference_layer = aprxMap.listLayers("links_" + CountyName +" TANA")[0]
    move_layer = aprxMap.listLayers("links_" + CountyName +" nonTANA")[0]
    aprxMap.moveLayer(reference_layer, move_layer, "BEFORE")

    reference_layer = aprxMap.listLayers("links_" + CountyName +" nonTANA")[0]
    move_layer = aprxMap.listLayers("tazs_" + CountyName)[0]
    aprxMap.moveLayer(reference_layer, move_layer, "BEFORE")

    reference_layer = aprxMap.listLayers("tazs_" + CountyName)[0]
    move_layer = aprxMap.listLayers("mazs_" + CountyName)[0]
    aprxMap.moveLayer(reference_layer, move_layer, "BEFORE")
 
    print(f"Finished re-ordering.")

    # to reduce clutter, turn off the layers except for the TANA layer

    layer_to_turn_off = aprxMap.listLayers("links_" + CountyName +" nonTANA")[0]
    layer_to_turn_off.visible = False

    layer_to_turn_off = aprxMap.listLayers("tazs_" + CountyName)[0]
    layer_to_turn_off.visible = False
 
    layer_to_turn_off = aprxMap.listLayers("mazs_" + CountyName)[0]
    layer_to_turn_off.visible = False

    layer_to_turn_off = aprxMap.listLayers("transit_links")[0]
    layer_to_turn_off.visible = False

    layer_to_turn_off = aprxMap.listLayers("nodes_farezone")[0]
    layer_to_turn_off.visible = False

    # change the zoom
    #layer_for_zoomExtent = aprxMap.listLayers("links_" + CountyName +" TANA")[0]
    #lyt = aprx.listLayouts()[0]
    #mf = lyt.listElements("mapframe_element", "*")[0]
    #mf.camera.setExtent(mf.getLayerExtent(layer_for_zoomExtent, False, True)) # or (layer,True,True)?
    #mf.zoomToAllLayers()
    # zoom to layer does not work yet. May need a "layout". See reference here:
    # https://pro.arcgis.com/en/pro-app/latest/arcpy/mapping/mapframe-class.htm

    # save it as a new project
    # aprx.save()
    aprx.saveACopy(r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\TM2_RoadwayQA_" + CountyName + ".aprx")
    print(f"Saved the project for " + CountyName)

    # remove layers in the current project (to get ready for the next county)
    layer_to_remove = aprxMap.listLayers("links_" + CountyName +" TANA")[0]
    aprxMap.removeLayer(layer_to_remove)

    layer_to_remove = aprxMap.listLayers("links_" + CountyName +" nonTANA")[0]
    aprxMap.removeLayer(layer_to_remove)

    layer_to_remove = aprxMap.listLayers("tazs_" + CountyName)[0]
    aprxMap.removeLayer(layer_to_remove)
 
    layer_to_remove = aprxMap.listLayers("mazs_" + CountyName)[0]
    aprxMap.removeLayer(layer_to_remove)

    layer_to_remove = aprxMap.listLayers("transit_links")[0]
    aprxMap.removeLayer(layer_to_remove)

    layer_to_remove = aprxMap.listLayers("nodes_farezone")[0]
    aprxMap.removeLayer(layer_to_remove)

    aprx.save()



# final steps that are done manually
# ----------------------------
# add the CTA's model network layer (reprojecting their map seem necessary or it may not display properly on the web map even though it may display properly on the .aprx). Note that TM2 Marin is proprietary, so don't post it)
# zoom into some place so they can see a close up view of the links when the map is loaded up.

# publish the feature layer manually 
# I started the script publish_web_feature_layer.py but it doesn't configure sharing and editing settings 
# e.g. general: share with everyone
# also we may want to provide "login required" version where we allow editing (feature property configuration: update feature attributes only)
# finally, make the feature layer a web map (save as "TM2 QA – CountyName 2015"; add text to the descriptions)


