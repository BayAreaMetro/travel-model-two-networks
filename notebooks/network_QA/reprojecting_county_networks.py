# this script reproject county model networks to the same coordinate system as TM2's

import arcpy

# TM2's coordinate system
out_coordinate_system = arcpy.SpatialReference('NAD 1983 StatePlane California V FIPS 0405 (US Feet)')

# Contra Costa
# --------------

# Contra Costa shared two networks with us - 2010 and 2018

input_features = r"C:\Users\ftsang\Box\Modeling and Surveys\Development\Travel Model Two Development\Model Inputs\County Networks\Contra_Costa\CCTA Model Networks - MTC Transmittal\CCTA_2018_Network.shp"
output_features = r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\county_networks_reprojected\CCTA_2018_epsg102645.shp"
arcpy.Project_management(input_features, output_features, out_coordinate_system)

input_features = r"C:\Users\ftsang\Box\Modeling and Surveys\Development\Travel Model Two Development\Model Inputs\County Networks\Contra_Costa\CCTA Model Networks - MTC Transmittal\CCTA_2010_Network.shp"
output_features = r"M:\Development\Travel Model Two\Supply\Network_QA_2022\Maps_to_publish\county_networks_reprojected\CCTA_2010_epsg102645.shp"
arcpy.Project_management(input_features, output_features, out_coordinate_system)