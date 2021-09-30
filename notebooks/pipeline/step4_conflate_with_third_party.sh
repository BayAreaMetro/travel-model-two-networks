#!/bin/bash
#
# This script should run in a shst docker
# 
# To build the docker image using the Dockerfile in this directory use (only need to run once):
# docker build -t shst .
#
# see it:
# docker image list
#
# Run a command (/bin/bash) in a new container layer over the specified image:
#   docker run -it --rm -v [path to this directory]:/usr/node/ shst:latest /bin/bash
# On a Mac machine:
#   e.g. docker run -it --rm -v /Users/lzorn/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# On a Windows machine:
#   e.g. docker run -it --rm -v /c/Users/ywang/Documents/GitHub/travel-model-two-networks:/usr/node/ shst:latest /bin/bash
# 
# First, create folders to store the output from match
# cd /usr/node/data
# mkdir "interim"
# cd interim
# mkdir -p tomtom/{bike_rules,car_rules,ped_rules}
# mkdir -p TM2_nonMarin/{car_rules,ped_rules,reverse_dir}
# mkdir -p TM2_Marin/{car_rules,ped_rules,reverse_dir}
# mkdir -p sfclines/{car_rules,ped_rules}
# mkdir -p sfcta/{car_rules,ped_rules,reverse_dir}
# mkdir "mtc"
# cd /.

# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step4_conflate_with_third_party.sh (skip this line when running on a Windows machine)
# ./step4_conflate_with_third_party.sh (if getting "/bin/bash^M: bad interpreter: No such file or directory" error, 
#   it means this file has Windows line endings, remove them by running "sed -i -e 's/\r$//' step4_conflate_with_third_party.sh" before this step)

for i in {1..14}
do
    echo "Matching tomtom_$i to shst network using bike rules"
    shst match ../../data/external/tomtom/tomtom$i.in.geojson --out=../../data/interim/tomtom/bike_rules/$i_tomtom.out.geojson --tile-hierarchy=8  --match-bike --follow-line-direction
    
    echo "Matching tomtom_$i to shst network using car rules"
    shst match ../../data/external/tomtom/tomtom$i.in.geojson --out=../../data/interim/tomtom/car_rules/$i_tomtom.out.geojson --tile-hierarchy=8 --follow-line-direction
    
    echo "Matching tomtom_$i to shst network using pedestrian rules"
    shst match ../../data/external/tomtom/tomtom$i.in.geojson --out=../../data/interim/tomtom/ped_rules/$i_tomtom.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction


    echo "Matching TM2nonMarin_$i to shst network using car rules"
    shst match ../../data/external/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=../../data/interim/TM2_nonMarin/car_rules/$i_tm2nonMarin.out.geojson --tile-hierarchy=8 --follow-line-direction

    echo "Matching TM2nonMarin_$i to shst network using pedestrian rules"
    shst match ../../data/external/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=../../data/interim/TM2_nonMarin/ped_rules/$i_tm2nonMarin.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

    echo "Matching TM2nonMarin_$i to shst network using car rules following direction"
    shst match ../../data/external/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=../../data/interim/TM2_nonMarin/reverse_dir/$i_tm2nonMarin.out.geojson --tile-hierarchy=8


    echo "Matching TM2Marin_$i to shst network using car rules"
    shst match ../../data/external/TM2_Marin/tm2Marin_$i.in.geojson --out=../../data/interim/TM2_Marin/car_rules/$i_tm2Marin.out.geojson --tile-hierarchy=8 --follow-line-direction

    echo "Matching TM2Marin_$i to shst network using pedestrian rules"
    shst match ../../data/external/TM2_Marin/tm2Marin_$i.in.geojson --out=../../data/interim/TM2_Marin/ped_rules/$i_tm2Marin.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

    echo "Matching TM2Marin_$i to shst network using car rules following direction"
    shst match ../../data/external/TM2_Marin/tm2Marin_$i.in.geojson --out=../../data/interim/TM2_Marin/reverse_dir/$i_tm2Marin.out.geojson --tile-hierarchy=8

done

echo "Matching SFCTA true shape to shst network using car rules"
shst match ../data/external/sfclines/sfcta.in.geojson --out=../../data/interim/sfclines/car_rules/sfcta.out.geojson --tile-hierarchy=8 --follow-line-direction

echo "Matching SFCTA true shape to shst network using pedestrian rules"
shst match ../data/external/sfclines/sfcta.in.geojson --out=../../data/interim/sfclines/ped_rules/sfcta.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction


echo "Matching SFCTA Stick Network to shst network using car rules"
shst match ../../data/external/sfcta/sfcta_in.geojson --out=../../data/interim/sfcta/car_rules/sfcta.out.geojson --tile-hierarchy=8 --follow-line-direction

echo "Matching SFCTA Stick Network to shst network using pedestrian rules"
shst match ../../data/external/sfcta/sfcta_in.geojson --out=../../data/interim/sfcta/ped_rules/sfcta.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

echo "Matching SFCTA Stick Network to shst network using car rules following direction"
shst match ../../data/external/sfcta/sfcta_in.geojson --out=../../data/interim/sfcta/reverse_dir/sfcta.out.geojson --tile-hierarchy=8
