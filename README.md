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
:: Note geopandas looks for pygeos
(tm2_network_dev_py38) pip install .\pygeos-0.12.0-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\GDAL-3.4.3-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\pyproj-3.3.1-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\Fiona-1.8.21-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install .\Shapely-1.8.2-cp38-cp38-win_amd64.whl
(tm2_network_dev_py38) pip install geopandas osmnx
:: can check that geopandas sees pygeos here: import geopandas; geopandas.show_versions()
(tm2_network_dev_py38) pip install geojson partridge dbfread urlopen pyarrow glob
:: Install network_wrangler
(tm2_network_dev_py38) C:
(tm2_network_dev_py38) cd C:\Users\lzorn\Documents\GitHub\network_wrangler
(tm2_network_dev_py38) pip install -e .
:: Install new dependencies introduced in this repo, noted in requirements.txt
(tm2_network_dev_py38) pip install scipy geofeather docker peartree
:: Set environment variables needed for the pipeline and reactivate conda env for them to take effect
(tm2_network_dev_py38) conda env config vars set INPUT_DATA_DIR="E:\Box\Modeling and Surveys\Development\Travel Model Two Development\Travel Model Two Network Rebuild\travel-model-two-networks\data"
(tm2_network_dev_py38) conda env config vars set OUTPUT_DATA_DIR=E:\tm2_network_version_13
(tm2_network_dev_py38) conda activate tm2_network_dev_py38
```

## Troubleshooting

### Docker Error from python
Note: the above environment worked fine but I got a runtime error on importing docker:
```
(tm2_network_dev_py38) PS C:\Users\lzorn\Documents\GitHub\travel-model-two-networks-version13\notebooks\pipeline> python
Python 3.8.13 (default, Mar 28 2022, 06:59:08) [MSC v.1916 64 bit (AMD64)] :: Anaconda, Inc. on win32
Type "help", "copyright", "credits" or "license" for more information.
>>> import docker
>>> client = docker.from_env()
Traceback (most recent call last):
  File "C:\Users\lzorn\.conda\envs\tm2_network_dev_py38\lib\site-packages\docker\api\client.py", line 159, in __init__
    self._custom_adapter = NpipeHTTPAdapter(
NameError: name 'NpipeHTTPAdapter' is not defined

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "C:\Users\lzorn\.conda\envs\tm2_network_dev_py38\lib\site-packages\docker\client.py", line 96, in from_env
    return cls(
  File "C:\Users\lzorn\.conda\envs\tm2_network_dev_py38\lib\site-packages\docker\client.py", line 45, in __init__
    self.api = APIClient(*args, **kwargs)
  File "C:\Users\lzorn\.conda\envs\tm2_network_dev_py38\lib\site-packages\docker\api\client.py", line 164, in __init__
    raise DockerException(
docker.errors.DockerException: Install pypiwin32 package to enable npipe:// support
```

Following [these directions](https://github.com/twosixlabs/armory/issues/156), this was resolved by running
```bat
python <path-to-python-env>\Scripts\pywin32_postinstall.py -install
```
