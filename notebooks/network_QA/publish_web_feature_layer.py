# this script publishes a map as a "web feature layer"
# adapted from https://pro.arcgis.com/en/pro-app/2.7/arcpy/sharing/featuresharingdraft-class.htm

import arcpy
import os

counties = ["Alameda", "Contra_Costa", "Marin",  "Napa",  "San_Francisco",  "San_Mateo",  "Santa_Clara",  "Solano",  "Sonoma"]

for x in counties:

    # Sign in to portal
    # arcpy.SignInToPortal('https://www.arcgis.com', 'MyUserName', 'MyPassword')
    # I can bypass this step by running this script within ArcGIS Pro's python window

    # Specify a local path for storing temporary contents to be used for publishing the service definition draft and service definition file
    outdir = r"M:\Development\Travel Model Two\Supply\Network_QA_2022\ArcGISOnline_ServiceDef"
    service = "TM2 network QA " + x + " 2015"
    sddraft_filename = service + ".sddraft"
    sddraft_output_filename = os.path.join(outdir, sddraft_filename)

    # Reference map to publish
    aprx = arcpy.mp.ArcGISProject(r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\TM2_RoadwayQA_" + x + ".aprx")
    m = aprx.listMaps("Map")[0]

    # Create FeatureSharingDraft and set service properties
    sharing_draft = m.getWebLayerSharingDraft("HOSTING_SERVER", "FEATURE", service)
    sharing_draft.summary = "TM2 Roadway Network for QA - " + x
    sharing_draft.tags = "TM2"
    sharing_draft.description = "My Description"
    sharing_draft.credits = "My Credits"
    sharing_draft.useLimitations = "My Use Limitations"

    # Create Service Definition Draft file
    sharing_draft.exportToSDDraft(sddraft_output_filename)

    # Stage Service
    sd_filename = service + ".sd"
    sd_output_filename = os.path.join(outdir, sd_filename)
    arcpy.StageService_server(sddraft_output_filename, sd_output_filename)

    # Share to portal
    print("Uploading Service Definition...")
    arcpy.UploadServiceDefinition_server(sd_output_filename, "My Hosted Services")

    print("Successfully Uploaded service.")