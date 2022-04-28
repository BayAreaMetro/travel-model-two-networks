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
# mkdir "interim/step4b_third_party_shst_match"
# cd interim/step4b_third_party_shst_match
# mkdir -p TomTom/{bike_rules,car_rules,ped_rules}
# mkdir -p TM2_nonMarin/{car_rules,ped_rules,reverse_dir}
# mkdir -p TM2_Marin/{car_rules,ped_rules,reverse_dir}
# mkdir -p sfclines/{car_rules,ped_rules}
# mkdir -p sfcta/{car_rules,ped_rules,reverse_dir}
# mkdir actc
# mkdir ccta
# mkdir pems
# cd /.

# Then you can cd to this directory, make this script executable, and run this script:
# cd /usr/node/notebooks/pipeline
# chmod u+x step4_conflate_with_third_party.sh (skip this line when running on a Windows machine)
# ./step4b_third_party_shst_match.sh (if getting "/bin/bash^M: bad interpreter: No such file or directory" error,
#   it means this file has Windows line endings, remove them by running "sed -i -e 's/\r$//' step4b_third_party_shst_match.sh" before this step)

for i in {1..14}
do
    echo "Matching tomtom_$i to shst network using car rules following direction"
    shst match external/step4a_third_party_data/modified/TomTom/tomtom_$i.in.geojson --out=interim/step4b_third_party_shst_match/TomTom/car_rules/tomtom_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction

    echo "Matching tomtom_$i to shst network using bike rules following direction"
    shst match external/step4a_third_party_data/modified/TomTom/tomtom_$i.in.geojson --out=interim/step4b_third_party_shst_match/TomTom/bike_rules/tomtom_$i.out.geojson --tile-hierarchy=8  --match-bike --follow-line-direction

    echo "Matching tomtom_$i to shst network using pedestrian rules following direction"
    shst match external/step4a_third_party_data/modified/TomTom/tomtom_$i.in.geojson --out=interim/step4b_third_party_shst_match/TomTom/ped_rules/tomtom_$i.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction


    echo "Matching TM2nonMarin_$i to shst network using car rules following direction"
    shst match external/step4a_third_party_data/modified/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_nonMarin/car_rules/tm2nonMarin_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction

    echo "Matching TM2nonMarin_$i to shst network using pedestrian rules following direction"
    shst match external/step4a_third_party_data/modified/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_nonMarin/ped_rules/tm2nonMarin_$i.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

    echo "Matching TM2nonMarin_$i to shst network using car rules not following direction"
    shst match external/step4a_third_party_data/modified/TM2_nonMarin/tm2nonMarin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_nonMarin/reverse_dir/tm2nonMarin_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections


    echo "Matching TM2Marin_$i to shst network using car rules following direction"
    shst match external/step4a_third_party_data/modified/TM2_Marin/tm2Marin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_Marin/car_rules/tm2Marin_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction

    echo "Matching TM2Marin_$i to shst network using pedestrian rules following direction"
    shst match external/step4a_third_party_data/modified/TM2_Marin/tm2Marin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_Marin/ped_rules/tm2Marin_$i.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

    echo "Matching TM2Marin_$i to shst network using car rules not following direction"
    shst match external/step4a_third_party_data/modified/TM2_Marin/tm2Marin_$i.in.geojson --out=interim/step4b_third_party_shst_match/TM2_Marin/reverse_dir/tm2Marin_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections


    echo "Matching actc_$i to shst network"
    shst match external/step4a_third_party_data/modified/actc/actc_$i.in.geojson --out=interim/step4b_third_party_shst_match/actc/actc_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction

    echo "Matching ccta_$i to shst network"
    shst match external/step4a_third_party_data/modified/ccta/ccta_$i.in.geojson --out=interim/step4b_third_party_shst_match/ccta/ccta_$i.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction
done

echo "Matching SFCTA Stick Network to shst network using car rules following direction"
shst match external/step4a_third_party_data/modified/sfcta/sfcta_in.geojson --out=interim/step4b_third_party_shst_match/sfcta/car_rules/sfcta.out.geojson --tile-hierarchy=8 --search-radius=50 --snap-intersections --follow-line-direction

echo "Matching SFCTA Stick Network to shst network using pedestrian rules following direction"
shst match external/step4a_third_party_data/modified/sfcta/sfcta_in.geojson --out=interim/step4b_third_party_shst_match/sfcta/ped_rules/sfcta.out.geojson --tile-hierarchy=8 --match-pedestrian --follow-line-direction

echo "Matching SFCTA Stick Network to shst network using car rules not following direction"
shst match external/step4a_third_party_data/modified/sfcta/sfcta_in.geojson --out=interim/step4b_third_party_shst_match/sfcta/reverse_dir/sfcta.out.geojson --tile-hierarchy=8

# TODO: decide if need to run shst_match on PEMS data
#echo "Matching PEMS to shst network using best direction rules"
#shst match external/step4a_third_party_data/modified/pems/pems.in.geojson --search-radius=250 --out=interim/step4b_third_party_shst_match/pems/pems.out.geojson --best-direction
