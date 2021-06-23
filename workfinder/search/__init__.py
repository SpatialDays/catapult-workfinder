import json
import logging
import os

import geopandas as gpd
import redis as redis
from libcatapult.storage.s3_tools import S3Utils

from nats.aio.client import Client as NATS
from pystac import Collection

from workfinder import get_config


def get_crs():
    crs = get_config("app", "crs")
    return {"init": crs}


def get_aoi():
    region = get_config("app", "region")
    borders = get_world_borders()
    aoi = borders.loc[borders.NAME == region]
    if aoi.empty:
        raise ValueError(f"region \"{region}\" not found in world borders file")
    envelope = aoi.to_crs(get_crs()).envelope
    value = envelope.to_crs({"init": "epsg:4326"}).values[0]
    return value.wkt


def get_world_borders():
    download_world_borders()

    inter_dir = get_config("app", "temp_dir")
    anc_dir = os.path.join(inter_dir, "ancillary")
    borders_local = os.path.join(anc_dir, "TM_WORLD_BORDERS.geojson")

    return gpd.read_file(borders_local)


def download_world_borders():
    inter_dir = get_config("app", "temp_dir")
    anc_dir = os.path.join(inter_dir, "ancillary")
    os.makedirs(anc_dir, exist_ok=True)
    os.makedirs(os.path.join(inter_dir, "outputs"), exist_ok=True)
    anc_dir_rem = 'common_sensing/ancillary_products/'
    borders_local = os.path.join(anc_dir, "TM_WORLD_BORDERS.geojson")
    borders_remote = os.path.join(anc_dir_rem, "TM_WORLD_BORDERS/TM_WORLD_BORDERS.geojson")

    if not os.path.exists(borders_local):
        logging.info(f'Downloading {borders_remote}')

        access_key = get_config("AWS", "access_key_id")
        secret_key = get_config("AWS", "secret_access_key")
        bucket = get_config("AWS", "bucket")

        s3_tools = S3Utils(access_key, secret_key, bucket, get_config("AWS", "end_point"),
                           get_config("AWS", "region"))
        s3_tools.fetch_file(borders_remote, borders_local)
        logging.info(f'Downloaded {borders_remote}')
    else:
        logging.info(f'{borders_local} already available')


def get_redis_connection():

    host = get_config("redis", "url")
    port = get_config("redis", "port")

    connection = redis.Redis(host=host, port=port, db=0)

    return connection


_s3_conn = None


def get_s3_connection():
    global _s3_conn

    if not _s3_conn:

        access = get_config("AWS", "access_key_id")
        secret = get_config("AWS", "secret_access_key")
        bucket_name = get_config("AWS", "bucket")
        endpoint_url = get_config("AWS", "end_point")
        s3_region = get_config("AWS", "region")

        _s3_conn = S3Utils(access, secret, bucket_name, endpoint_url, s3_region)
    return _s3_conn


def list_s3_files(prefix):
    s3 = get_s3_connection()
    path_sizes = s3.list_files_with_sizes(prefix)
    return path_sizes


def list_catalog(collection_path):
    s3 = get_s3_connection()
    catalog_body = s3.get_object_body(collection_path)
    catalog = json.loads(catalog_body.decode('utf-8'))
    collection = Collection.from_dict(catalog)
    result = [i.id for i in collection.get_items()]
    return result


_nc = NATS()


def nats_connect():
    global _nc
    options = {
        "servers": [get_config("NATS", "url")],
    }

    _nc.connect(**options)


def nats_close():
    global _nc

    _nc.close()


def nats_publish(topic, message):
    _nc.publish(topic, message)


def get_ard_list(folder):
    path_sizes = list_s3_files(folder)
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
