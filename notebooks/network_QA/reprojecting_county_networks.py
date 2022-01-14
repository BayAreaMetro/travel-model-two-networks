import arcpy

# input data is in GCS_WGS_1984 coordinate system
input_features = r"C:/Users/ftsang/Documents/tm2_network_exploration/County Networks/Contra_Costa/CCTA Model Networks - MTC Transmittal/CCTA_2018_Network.shp"

# output data
output_feature_class = r"C:/Users/ftsang/Documents/tm2_network_exploration/tm2_roadway_QA/CCTA_2018_epsg102645.shp"

# create a spatial reference object for the output coordinate system
out_coordinate_system = arcpy.SpatialReference('NAD 1983 StatePlane California V FIPS 0405 (US Feet)')

# run the tool
arcpy.Project_management(input_features, output_feature_class, out_coordinate_system)