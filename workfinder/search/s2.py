import json
import logging

import geopandas as gpd
import pandas as pd
from libcatapult.queues.base_queue import BaseQueue
from sentinelsat import SentinelAPI

logger = logging.getLogger(__name__)

from workfinder import get_config, S3Api
from workfinder.search import get_gpd_file, get_ard_list, get_aoi
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class S2(BaseWorkFinder):

    def __init__(self, s3: S3Api, redis: BaseQueue, esa_api: SentinelAPI):
        super().__init__()
        self._s3 = s3
        self._redis = redis
        self._esa_api = esa_api

    def find_work_list(self):
        self._s3.get_s3_connection()

        region = get_config("APP", "REGION")
        logger.info(f"Querying ESA API for Sentinel 2 data for {region} region")
        aoi = get_aoi(None, region)

        world_granules = get_gpd_file(self._s3,
                                      "sentinel2_tiles_world.geojson",
                                      "SatelliteSceneTiles/sentinel2_tiles_world/sentinel2_tiles_world.geojson")
        # Create bool for intersection between any tiles - should try inversion to speed up...
        world_granules[region] = world_granules.geometry.apply(lambda x: gpd.GeoSeries(x).intersects(aoi))
        # Filter based on any True intersections
        world_granules[region] = world_granules[world_granules[region]].any(1)
        region_s2_grans = world_granules[world_granules[region] == True]

        res = self._esa_api.query(
            area=aoi,
            platformname='Sentinel-2',
            producttype='S2MSI2A'
        )
        esa_result = self._esa_api.to_geodataframe(res)
        esa_result['id'] = esa_result.title.apply(
            lambda x: f"{x.split('_')[0]}_{x.split('_')[1]}_{x.split('_')[2]}_{x.split('_')[5]}")
        esa_result['granules'] = esa_result.identifier.str[39:44]
        esa_result_sorted_by_position = esa_result.sort_values('beginposition', ascending=False)
        esa_results_granule_filtered = esa_result_sorted_by_position[
            esa_result_sorted_by_position['granules'].isin(region_s2_grans.Name.values)]
        logger.info(f"Found {len(esa_results_granule_filtered)} L1C scenes to process")
        # drop the stuff that cant be jsonified
        esa_results_granule_filtered.drop(columns='geometry', inplace=True)
        esa_results_granule_filtered.drop(columns='generationdate', inplace=True)
        esa_results_granule_filtered.drop(columns='beginposition', inplace=True)
        esa_results_granule_filtered.drop(columns='endposition', inplace=True)
        esa_results_granule_filtered.drop(columns='ingestiondate', inplace=True)
        esa_results_granule_filtered = pd.DataFrame(esa_results_granule_filtered)
        return esa_results_granule_filtered

    def find_already_done_list(self):
        region = get_config("APP", "REGION")
        imagery_path = get_config("S3", "IMAGERY_PATH")
        return get_ard_list(self._s3, f"{imagery_path}/{region.lower()}/sentinel_2/")

    def submit_tasks(self, to_do_list: pd.DataFrame):
        self._redis.connect()
        logger.info(f"Submitting {len(to_do_list)} tasks to redis")
        logger.info(f"Type of to_do_list: {type(to_do_list)}")
        logger.info(f"to_do_list: {to_do_list}")
        target_queue = get_config("S2", "REDIS_PROCESSED_CHANNEL")
        imagery_path = get_config("S3", "IMAGERY_PATH")
        region = get_config("APP", "REGION")

        for index, row in to_do_list.iterrows():
            # convert row to dict
            row_dict = row.to_dict()
            row_dict['s3_bucket'] = get_config("S3", "BUCKET")
            row_dict["s3_dir"] = f"{imagery_path}/{region.lower()}/sentinel_2/"
            row_dict["region"] = region
            self._redis.publish(target_queue, json.dumps(row_dict))
        self._redis.close()
