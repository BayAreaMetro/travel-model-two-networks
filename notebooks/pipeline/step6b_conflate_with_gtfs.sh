#!/bin/bash
#
# This script should run in a shst docker
#
# Run a command (/bin/bash) in a new container layer over the specified image:
#   docker run -it --rm -v [path to this directory]:/usr/node/ shst:latest /bin/bash
# On a Mac machine:
#   e.g. docker run -it --rm -v /Users/lzorn/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# On a Windows machine:
#   e.g. docker run -it --rm -v /c/Users/ywang/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# 
# First, create folders to store the output from match
# cd /usr/node/data/interim/
# mkdir -p step6_gtfs/shst_match
# cd /.

# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step6_conflate_with_gtfs.sh (skip this line when running on a Windows machine)
# ./step6_conflate_with_gtfs.sh (if getting "/bin/bash^M: bad interpreter: No such file or directory" error, 
#   it means this file has Windows line endings, remove them by running "sed -i -e 's/\r$//' step6_conflate_with_gtfs.sh" before this step)

for filename in ../../data/external/gtfs/*.geojson
do
    name=$(basename "$filename" .geojson)

    if [[ "$name" =~ ^(BART_2015_8_3.transit|Caltrain_2015_5_13.transit|Capitol_2017_3_20.transit|GGFerries_2017_3_18.transit|SF_Bay_Ferry2016_07_01.transit)$ ]]; then
    	echo "Skip rail and ferry match: ${name}"

	else
		echo "Matching ${name} to shst following line direction"
    	shst match ../../data/external/gtfs/$name.geojson --out=../../data/interim/step6_gtfs/shst_match/$name.out.geojson --follow-line-direction --tile-hierarchy=8
	fi

done
