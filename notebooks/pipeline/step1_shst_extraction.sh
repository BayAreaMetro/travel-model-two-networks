#!/bin/bash
#
# This script should run in a shst docker

# First, create folder "/data/external/sharedstreets_extract" to store the extracted data
# cd /data/external
# mkdir "sharedstreets_extract"
# cd /.

# To build the docker image using the Dockerfile in this directory use (only need to run once):
# docker build -t shst .
#
# see it:
# docker image list
#
# Run a command (/bin/bash) in a new container layer over the specified image:
# 	docker run -it --rm -v [path to this directory]:/usr/node/ shst:latest /bin/bash
# On a Mac machine:
# 	e.g. docker run -it --rm -v /Users/lzorn/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# On a Windows machine:
#   e.g. docker run -it --rm -v /c/Users/ywang/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash

# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step1_shst_extraction.sh (skip this line when running on a Windows machine)
# ./step1_shst_extraction.sh (if getting "/bin/bash^M: bad interpreter: No such file or directory" error, 
#	it means this file has Windows line endings, remove them by running "sed -i -e 's/\r$//' step1_shst_extraction.sh" before this step)


for i in {1..14}
do
  echo "Processing bay area piece $i"
  shst extract ../../data/external/county_boundaries/boundary_$i.geojson --out=../../data/external/sharedstreets_extract/mtc_$i.geojson --metadata --tile-hierarchy=8 --tiles
done
