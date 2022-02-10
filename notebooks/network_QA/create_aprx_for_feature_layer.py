# --------------------------------------------------------------------------------
# This script create the ArcGIS Pro project (.aprx) that underlies the TM2 QA web map for each county
# This is not a stand alone script
# I start from manually starting a blank project 
# and run the following code in the python window within ArcGIS Pro
# 
# this script is stil in development - there are some issues related to reordering the layers
# --------------------------------------------------------------------------------

import arcpy

# county_list = ["Alameda", "Contra Costa", "Marin",  "Napa",  "San Francisco",  "San Mateo",  "Santa Clara",  "Solano",  "Sonoma"]
county_list = ["Marin",  "Napa",  "San Francisco",  "San Mateo",  "Santa Clara",  "Solano",  "Sonoma"]
#county_list = ["Marin"] 
county_list = ["Napa"]

for CountyName in county_list:

    # This script uses the keyword current - it should be run from the Python window within ArcGIS Pro 
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    aprxMap = aprx.listMaps("Map")[0] 

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

        # add the comment field
        #inFeatures = aprxMap.listLayers("links_" + CountyName + " " + TANAnonTANA)[0]
        #fieldName = "CTA comments"
        #fieldLength = 500
        #arcpy.AddField_management(inFeatures, fieldName, "TEXT", field_length=fieldLength)


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

    # save the project
    # aprx.save()
    aprx.saveACopy(r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\TM2_RoadwayQA_" + CountyName + ".aprx")
    print(f"Saved the project for " + CountyName)


# final steps that are done manually
# ----------------------------
# add the CTA's model network layer (note that TM2 Marin is proprietary, so don't post it)
# in the contents pane, uncheck the layers excep for the TANA layer
# publish using the script publish_web_feature_layer.py
# make the feature layer a web map
# configure the sharing settings and editing settings on ArcGIS Online

