import json
from shapely.wkt import loads

import geopandas as gpd
import pandas as pd
from libcatapult.queues.base_queue import BaseQueue
from sentinelsat import SentinelAPI
import logging

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

        # logger.info(f"Querying ESA for {len(region_s2_grans.index)} tiles")
        # res = self._esa_api.query(
        #     area=aoi,
        #     platformname='Sentinel-2',
        #     producttype='S2MSI1c',
        #     limit=1
        # )
        #
        # esa_l2a = self._esa_api.to_geodataframe(res) # expecting l2a, but calling l1c??

        res2 = self._esa_api.query(
            area=aoi,
            platformname='Sentinel-2',
            producttype='S2MSI1C',
            limit=1
        )
        esa_l1c = self._esa_api.to_geodataframe(res2)
        esa_l1c['id'] = esa_l1c.title.apply(
            lambda x: f"{x.split('_')[0]}_{x.split('_')[1]}_{x.split('_')[2]}_{x.split('_')[5]}")
        # esa_l2a['scenename'] = esa_l2a.title.apply(
        #     lambda x: f"{x.split('_')[0]}_MSIL1C_{x.split('_')[2]}_{x.split('_')[5]}")
        esa_l1c['granules'] = esa_l1c.identifier.str[39:44]
        # esa_l2a['granules'] = esa_l2a.identifier.str[39:44]
        #
        esa_l1c_srt = esa_l1c.sort_values('beginposition', ascending=False)
        # esa_l2a_srt = esa_l2a.sort_values('beginposition', ascending=False)
        # esa_l1c_srt = esa_l1c_srt.loc[~esa_l1c_srt['scenename'].isin(esa_l2a_srt.scenename.values)]
        #
        esa_l1c_precise = esa_l1c_srt[esa_l1c_srt['granules'].isin(region_s2_grans.Name.values)]
        # esa_l2a_precise = esa_l2a_srt[esa_l2a_srt['granules'].isin(region_s2_grans.Name.values)]
        #
        # result = pd.concat([esa_l1c_precise['scenename'], esa_l2a_precise['scenename']], axis=0)

        logger.info(f"Found {len(esa_l1c_precise)} L1C scenes to process")
        # save esa_l1c to .csv
        # rename scenename column to id
        #  esa_l1c.rename(columns={'scenename': 'id'}, inplace=True)
        # drop the stuff that cant be jsonified
        esa_l1c_precise.drop(columns='geometry', inplace=True)
        esa_l1c_precise.drop(columns='datatakesensingstart', inplace=True)
        esa_l1c_precise.drop(columns='generationdate', inplace=True)
        esa_l1c_precise.drop(columns='beginposition', inplace=True)
        esa_l1c_precise.drop(columns='endposition', inplace=True)
        esa_l1c_precise.drop(columns='ingestiondate', inplace=True)
        esa_l1c_precise = pd.DataFrame(esa_l1c_precise)
        logger.info(f"Type of ESAL1C : {type(esa_l1c_precise)}")
        return esa_l1c_precise

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
            self._redis.publish(target_queue, json.dumps(row_dict))
        self._redis.close()


def _row_mapping(row):
    return {'id': row['scenename'], 'url': row['scenename']}
