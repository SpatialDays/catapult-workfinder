import logging
import os
from pathlib import Path

import pandas as pd
import geopandas as gpd

from workfinder import get_config
from workfinder.search import get_ard_list, get_redis_connection, get_aoi, get_ancillary_dir, download_ancillary_file, \
    download_ancillary_http
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class Landsat8 (BaseWorkFinder):
    def find_work_list(self):
        rows_df = _get_rows_paths()
        df = _download_metadata()
        logging.info("finding scenes")
        df = df[df['WRS Path'].isin(rows_df['ROW'].values) & df['WRS Path'].isin(rows_df['PATH'].values)]
        logging.info("converting to required objects")
        logging.info(df.columns)
        df_result = df.apply(_apply_row_mapping, axis=1, result_type='expand')
        return df_result

    def find_already_done_list(self):
        region = get_config("app", "region")
        return get_ard_list(f"common_sensing/{region.lower()}/landsat_8/")

    def submit_tasks(self, to_do_list):
        channel = get_config("landsat8", "redis_channel")
        # get redis connection
        conn = get_redis_connection()
        # submit each task.
        for index, e in to_do_list.iterrows():
            payload = {
                "in_scene": e['id'],
                "s3_bucket": "public-eo-data",
                "s3_dir": "test/landsat_8/",
            }
            conn.publish(channel, payload)


def _order_products(to_do_list):



def _get_rows_paths():
    region = get_config("app", "region")
    aoi = get_aoi()
    file_path = download_ancillary_file("WRS2_descending.geojson", "SatelliteSceneTiles/landsat_pr/WRS2_descending.geojson")
    world_granules = gpd.read_file(file_path)
    # Create bool for intersection between any tiles - should try inversion to speed up...
    world_granules[region] = world_granules.geometry.apply(lambda x: gpd.GeoSeries(x).intersects(aoi))
    # Filter based on any True intersections
    world_granules[region] = world_granules[world_granules[region]].any(1)
    region_ls_grans = world_granules[world_granules[region] == True]
    return region_ls_grans


def _download_metadata():
    logging.info("downloading landsat8 metadata")
    file_path = download_ancillary_http("LANDSAT_OT_C2_L2.csv.gz", "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_OT_C2_L2.csv.gz")
    df = pd.read_csv(file_path)
    logging.info(f"got metadata: {df.size}")
    return df


def _apply_row_mapping(row):
    return {'id': row['Landsat Product Identifier L1'][:25], 'url': row['Landsat Product Identifier L1']}
