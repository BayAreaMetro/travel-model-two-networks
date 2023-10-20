#!/bin/bash
################################################################################
# NOTE: step0_preepare_for_shst_extraction.py now includes this functionality
# SO THIS SCRIPT IS JUST FOR REFERENCE
################################################################################
# This script should run in a shst docker
# 
# To build the docker image using the Dockerfile in this directory use (only need to run once):
# docker build -t shst .
#
# see it:
# docker image list
#
# Run a command (/bin/bash) in a new container layer over the specified image:
# 	docker run -it --rm -v [path to this directory]:/usr/node/ shst:latest /bin/bash
# e.g. on a Mac machine:
# 	docker run -it --rm -v /Users/lzorn/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# e.g. on a Windows machine:
#   docker run -it --rm -v /c/Users/ywang/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
#  
# To mount a shared drive (e.g. M drive): 
# NOTE: I couldn't get this to work with the domain (models.ad.mtc.gov) but it appeared to work with the IP address
#   see discussion here: https://github.com/docker/cli/issues/706
# docker volume create \
#   --driver local \
#   --opt type=cifs \
#   --opt device=//10.164.0.74/data/models \
#   --opt o=user=yourusername,password=yourpassword
#   m_volume
# For a local drive that is not C: (e.g. E drive):
# Make that drive available for sharing: Properties -> Sharing -> Advanced Sharing
#    Check "Share this folder"
#    Click "Permissions" button and within that dialog, 
#      add yourself as user with Allo Permissions for all three rows ("Full Control","Change","Read")
# Then follow the docker volume creation listed above, with the IP address being your own machine's IP, e.g.
# docker volume create \
#   --driver local \
#   --opt type=cifs \
#   --opt device=//10.164.4.143/e \
#   --opt o=user=yourusername,password=yourpassword,file_mode=0777,dir_mode=0777
#    e_volume
# docker volume list
# docker run -it --rm \
#  --mount type=bind,source=C:/Users/lzorn/Documents/GitHub/travel-model-two-networks,destination=/usr/node \
#  --mount type=volume,source=m_volume,destination=/usr/m_volume \
#  --mount type=volume,source=e_volume,destination=/usr/e_volume \
#  shst:latest /bin/bash
#
# First, create folder "data/step1_shst_extracts" to store the extracted data
# cd /usr/e_volume
# mkdir step1_shst_extracts
# 
# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step1_shst_extraction.sh (skip this line when running on a Windows machine)
# ./step1_shst_extraction.sh (if getting "/bin/bash^M: bad interpreter: No such file or directory" error, 
#	it means this file has Windows line endings, remove them by running "sed -i -e 's/\r$//' step1_shst_extraction.sh" before this step)
#
# Alternatively, since the script below is very short,
# you can just copy/paste them into the command line

for i in {1..14}
do
  echo "Processing bay area piece $i"
  shst extract step0_boundaries/boundary_$i.geojson --out=step1_shst_extracts/mtc_$i.geojson --metadata --tile-hierarchy=8 --tiles
done
