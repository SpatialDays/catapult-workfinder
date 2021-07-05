import json
import logging
import os
import shutil

import requests

from urllib import request
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pystac import Collection

from workfinder import get_config
from workfinder.api.s3 import S3Api


def get_crs():
    crs = get_config("app", "crs")
    return {"init": crs}


def get_aoi_wkt(s3: S3Api, region: str):
    value = get_aoi(s3, region)
    return value.wkt


def get_aoi(s3: S3Api, region: str):
    borders = get_world_borders(s3)
    aoi = borders.loc[borders.NAME == region]
    if aoi.empty:
        raise ValueError(f"region \"{region}\" not found in world borders file")
    envelope = aoi.to_crs(get_crs()).envelope
    value = envelope.to_crs({"init": "epsg:4326"}).values[0]
    return value


def get_world_borders(s3: S3Api):
    download_ancillary_file(s3, "TM_WORLD_BORDERS.geojson", "TM_WORLD_BORDERS/TM_WORLD_BORDERS.geojson")

    anc_dir = get_ancillary_dir()
    borders_local = os.path.join(anc_dir, "TM_WORLD_BORDERS.geojson")

    return gpd.read_file(borders_local)


def get_ancillary_dir():
    inter_dir = get_config("app", "temp_dir")
    anc_dir = os.path.join(inter_dir, "ancillary")
    os.makedirs(os.path.join(inter_dir, "outputs"), exist_ok=True)
    os.makedirs(anc_dir, exist_ok=True)
    return anc_dir


def download_ancillary_file(s3: S3Api, name, remote_path):
    anc_dir = get_ancillary_dir()
    anc_dir_rem = 'common_sensing/ancillary_products/'
    local = os.path.join(anc_dir, name)
    remote = os.path.join(anc_dir_rem, remote_path)

    if not os.path.exists(local):
        logging.info(f'Downloading {remote}')
        s3.fetch_file(remote, local)
        logging.info(f'Downloaded {remote}')
    else:
        logging.info(f'{name} already available')

    return local


def download_ancillary_http(name, url):
    anc_dir = get_ancillary_dir()
    local = os.path.join(anc_dir, name)
    if not os.path.exists(local):
        logging.info(f'Downloading {url}')
        with request.urlopen(url) as response, open(local, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        logging.info(f'Downloaded {url}')
    else:
        logging.info(f"{name} already available")

    return local


def list_catalog(s3: S3Api, collection_path: str):

    catalog_body = s3.get_object_body(collection_path)
    catalog = json.loads(catalog_body.decode('utf-8'))
    collection = Collection.from_dict(catalog)
    result = [i.id for i in collection.get_items()]
    return result


def get_ard_list(s3: S3Api, folder: str):
    path_sizes = s3.list_s3_files(folder)
    logging.info(f"got {len(path_sizes)} files, for '{folder}'")
    df_result = pd.DataFrame({'id': [], 'url': []})
    for r in path_sizes:

        if r['name'].endswith(".yaml"):
            url = r['name']
            id = _extract_id_ard_path(url)
            df_result = df_result.append({'id': id, 'url': url}, ignore_index=True)

    logging.info(f"found {df_result.size} entries")
    return df_result


def _extract_id_ard_path(p: str):
    parts = Path(os.path.split(p)[0]).stem
    return parts


def espa_api(endpoint, verb='get', body=None, uauth=None):
    """ Suggested simple way to interact with the ESPA JSON REST API """
    host = get_config("usgs", "host")

    auth_tup = uauth
    if not uauth:
        username = get_config("usgs", "username")
        password = get_config("usgs", "password")
        auth_tup = (username, password)
    response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=body)
    logging.debug('{} {}'.format(response.status_code, response.reason))
    data = response.json()
    if isinstance(data, dict):
        messages = data.pop("messages", None)
        if messages:
            logging.debug(json.dumps(messages, indent=4))
    try:
        response.raise_for_status()
    except Exception as e:
        logging.warning(e)
        return None
    else:
        return data
