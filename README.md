# travel-model-two-networks
Network rebuild for MTC's Travel Model Two

## Folder Structure
Please follow the [Cookiecutter for Data Science](http://drivendata.github.io/cookiecutter-data-science/) structure.

## Data
Data used in this repository (the /data folder) is stored in [MTC's Box folder](https://app.box.com/folder/130570381446).

## Environment Setup
```bat
:: Create conda environment.  Use python 3.8 as it's required for osmnx 1.2.0 which includes the reversed edge attribute
(base) conda create python=3.8 -n tm2_network_dev_py38
(base) conda activate tm2_network_dev_py38
:: Install windows binary versions of the GDAL/fiona related packages required for geopandas since these can be tricky
(tm2_network_dev_py38) M:
(tm2_network_dev_py38) cd M:\Software\Python\py38_geopandas_set
(tm2_network_dev_py38) pip install .\Rtree-1.0.0-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\GDAL-3.4.3-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\pyproj-3.3.1-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\Fiona-1.8.21-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\Shapely-1.8.2-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install geopandas osmnx
:: Install network_wrangler
(tm2_network_dev_py38) C:
(tm2_network_dev_py38) cd C:\Users\lzorn\Documents\GitHub\network_wrangler
(tm2_network_dev_py38) pip install -e .
:: Install new dependencies introduced in this repo, noted in requirements.txt
(tm2_network_dev_py38) pip install scipy geofeather docker
:: Set environment variables needed for the pipeline and reactivate conda env for them to take effect
(tm2_network_dev_py38) conda env config vars set INPUT_DATA_DIR="E:\Box\Modeling and Surveys\Development\Travel Model Two Development\Travel Model Two Network Rebuild\travel-model-two-networks\data"
(tm2_network_dev_py38) conda env config vars set OUTPUT_DATA_DIR=E:\tm2_network_version_13
(tm2_network_dev_py38) conda activate tm2_network_dev_py38
```