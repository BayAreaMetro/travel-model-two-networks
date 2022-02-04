
###########################
#
# this script is intended for overwriting hosted feature services
# adapted from: https://support.esri.com/en/technical-article/000023164?fbclid=IwAR2rxGGkWPY7zWaOmWpqrneVma5ETNKZJHedtT9dK37DS1_lNU-JigdKqvM
# BUT IT DOES NOT WORK
#
###########################

# Import the necessary modules
import arcpy
import os, sys
from arcgis.gis import GIS

# Specify the location of the project file (.aprx)
prjPath = r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\TM2_RoadwayQA_Contra Costa.aprx"

# Specify the feature service name in ArcGIS Online
sd_fs_name = "TM2 QA Contra Costa"
portal = "http://www.arcgis.com"

# Set the desired sharing options
shrOrg = True
shrEveryone = False
shrGroups = ""

# Specify a local path for storing temporary contents to be used for publishing the service definition draft and service definition file
relPath = r'M:\Staff\Flavia\ArcGISOnline_ServiceDef'
sddraft = os.path.join(relPath, "temporary service name.sddraft")
sd = os.path.join(relPath, "temporary service name.sd")

print("Creating SD file")
arcpy.env.overwriteOutput = True
prj = arcpy.mp.ArcGISProject(prjPath)
mp = prj.listMaps()[0]

sharing_draft = mp.getWebLayerSharingDraft("HOSTING_SERVER", "FEATURE", sd_fs_name)
sharing_draft.summary = "My Summary"
sharing_draft.tags = "TM2"
sharing_draft.description = "My Description"
sharing_draft.credits = "My Credits"
sharing_draft.useLimitations = "My Use Limitations"

sharing_draft.exportToSDDraft(sddraft)
arcpy.StageService_server(sddraft, sd)

print("Connecting to {}".format(portal))
#user = "USERNAME"
#password = "PASSWORD"
# gis = GIS(portal, user, password)
# user name and password not needed, because I'm running this script within Pro where I am already connected.
gis = GIS("Pro")

# Find the SD, update it, publish /w overwrite and set sharing and metadata
print("Search for original SD on portal…")
print(f"Query: {sd_fs_name}")
sdItem = gis.content.search(query=sd_fs_name, item_type="Service Definition")

i=0
while sdItem[i].title != sd_fs_name:
            i += 1
print('Item Found')
print(f'item[i].title = {sdItem[i].title}, sd_fs_name = {sd_fs_name}')
item = sdItem[i] 
item.update(data=sd)

print("Overwriting existing feature service…")
fs = item.publish(overwrite=True)

if shrOrg or shrEveryone or shrGroups:
    print("Setting sharing options…")
    fs.share(org=shrOrg, everyone=shrEveryone, groups=shrGroups)

print("Finished updating: {} – ID: {}".format(fs.title, fs.id))