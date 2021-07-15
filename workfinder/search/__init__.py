import json
import logging
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pystac import Collection

from workfinder import get_config
from workfinder.api.s3 import S3Api


def get_crs():
    crs = get_config("app", "crs")
    return {"init": crs}


def get_aoi(s3: S3Api, region: str):
    borders = get_world_borders(s3)
    aoi = borders.loc[borders.NAME == region]
    if aoi.empty:
        raise ValueError(f"region \"{region}\" not found in world borders file")
    envelope = aoi.to_crs(get_crs()).envelope
    value = envelope.to_crs({"init": "epsg:4326"}).values[0]
    return value.wkt


def get_world_borders(s3: S3Api):
    download_world_borders(s3)

    inter_dir = get_config("app", "temp_dir")
    anc_dir = os.path.join(inter_dir, "ancillary")
    borders_local = os.path.join(anc_dir, "TM_WORLD_BORDERS.geojson")

    return gpd.read_file(borders_local)


def download_world_borders(s3: S3Api):
    inter_dir = get_config("app", "temp_dir")
    anc_dir = os.path.join(inter_dir, "ancillary")
    os.makedirs(anc_dir, exist_ok=True)
    os.makedirs(os.path.join(inter_dir, "outputs"), exist_ok=True)
    anc_dir_rem = 'common_sensing/ancillary_products/'
    borders_local = os.path.join(anc_dir, "TM_WORLD_BORDERS.geojson")
    borders_remote = os.path.join(anc_dir_rem, "TM_WORLD_BORDERS/TM_WORLD_BORDERS.geojson")

    if not os.path.exists(borders_local):
        logging.info(f'Downloading {borders_remote}')
        s3.fetch_file(borders_remote, borders_local)
        logging.info(f'Downloaded {borders_remote}')
    else:
        logging.info(f'{borders_local} already available')


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
