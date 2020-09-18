#!/bin/bash
#
# This script should run in a shst docker
# To build the docker image using the Dockerfile in this directory use:
# docker build -t shst .
#
# see it:
# docker image list
#
# Run a command (/bin/bash) in a new container layer over the specified image:
# docker run -it --rm -v [path to this directory]:/usr/node/ shst:latest /bin/bash
# e.g. docker run -it --rm -v /Users/lzorn/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash

# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step1_shst_extraction.sh
# ./step1_shst_extraction.sh

for i in {1..14}
do
  echo "Processing bay area piece $i"
  shst extract ../../data/external/county_boundaries/boundary_$i.geojson --out=../../data/external/sharedstreets_extract/mtc_$i.geojson --metadata --tile-hierarchy=8 --tiles
done
