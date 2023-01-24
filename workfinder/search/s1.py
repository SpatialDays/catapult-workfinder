import json
import logging

import geopandas as gpd
import pandas as pd
from libcatapult.queues.base_queue import BaseQueue
from sentinelsat import SentinelAPI

from workfinder import get_config
from workfinder.api.s3 import S3Api
from workfinder.search import get_gpd_file, get_ard_list, get_aoi
from workfinder.search.BaseWorkFinder import BaseWorkFinder

logger = logging.getLogger(__name__)


class S1(BaseWorkFinder):

    def __init__(self, s3: S3Api, redis: BaseQueue, esa_api: SentinelAPI):
        super().__init__()
        self._s3 = s3
        self._redis = redis
        self._esa_api = esa_api

    def submit_tasks(self, to_do_list: pd.DataFrame):

        if to_do_list is not None and len(to_do_list) > 0:
            channel = get_config("S1", "REDIS_PROCESSED_CHANNEL")
            # get redis connection
            self._redis.connect()
            # submit each task.
            imagery_path = get_config("S3", "IMAGERY_PATH")
            for index, e in to_do_list.iterrows():
                payload = {
                    "in_scene": e['id'],
                    "s3_bucket": "public-eo-data",
                    "s3_dir": "test/sentinel_1/",
                    "ext_dem": f"{imagery_path}/ancillary_products/SRTM1Sec/SRTM30_Fiji_{e['hemisphere']}.tif"}
                self._redis.publish(channel, json.dumps(payload))

    def find_work_list(self):
        self._s3.get_s3_connection()

        region = get_config("APP", "REGION")
        aoi = get_aoi(None, region)
        res = self._esa_api.query(
            area=aoi,
            platformname='Sentinel-1',
            producttype='GRD',
            sensoroperationalmode='IW',
        )

        world_granules = get_gpd_file(self._s3,
                                      "sentinel2_tiles_world.geojson",
                                      "SatelliteSceneTiles/sentinel2_tiles_world/sentinel2_tiles_world.geojson")
        # Create bool for intersection between any tiles - should try inversion to speed up...
        world_granules[region] = world_granules.geometry.apply(lambda x: gpd.GeoSeries(x).intersects(aoi))
        # Filter based on any True intersections
        world_granules[region] = world_granules[world_granules[region]].any(1)
        region_s1_grans = world_granules[world_granules[region] == True]

        esa_grd = self._esa_api.to_geodataframe(res)
        esa_grd['id'] = esa_grd.title.apply(
            lambda x: f"{x.split('_')[0]}_{x.split('_')[1]}_{x.split('_')[2]}_{x.split('_')[5]}")
        # esa_grd['granules'] = esa_grd.identifier.str[39:44]
        # esa_grd_sorted = esa_grd.sort_values('beginposition', ascending=False)
        # esa_grd_precise = esa_grd_sorted[esa_grd_sorted['granules'].isin(region_s1_grans.Name.values)]
        esa_grd_precise = esa_grd
        logger.info(f"Found {len(esa_grd_precise)} L1C scenes to process")
        # save esa_l1c to .csv
        # rename scenename column to id
        #  esa_l1c.rename(columns={'scenename': 'id'}, inplace=True)
        # drop the stuff that cant be jsonified
        esa_grd_precise.drop(columns='geometry', inplace=True)
        # esa_grd_precise.drop(columns='generationdate', inplace=True)
        esa_grd_precise.drop(columns='beginposition', inplace=True)
        esa_grd_precise.drop(columns='endposition', inplace=True)
        esa_grd_precise.drop(columns='ingestiondate', inplace=True)
        esa_grd_precise = pd.DataFrame(esa_grd_precise)
        # log the colums of esa_grd_precise
        return esa_grd_precise
        # asf_grd_matches = get_s1_asf_urls(esa_grd.title.values)

        # df = pd.merge(left=esa_grd, right=asf_grd_matches, how='left', left_on='title', right_on='Granule Name')
        # for n, g in zip(aoi.NAME, aoi.geometry):
        #     df[n] = df.geometry.apply(lambda x: gpd.GeoSeries(x).intersects(g))
        # # Filter based on any True intersections
        # df[region] = df[df[aoi.NAME.values]].any(1)
        # scenes = df[df[region] is True]
        # return scenes

    def find_already_done_list(self):
        region = get_config("APP", "REGION")
        imagery_path = get_config("S3", "IMAGERY_PATH")
        return get_ard_list(self._s3, f"{imagery_path}/{region.lower()}/sentinel_1/")

    def submit_tasks(self, to_do_list: pd.DataFrame):
        self._redis.connect()
        logger.info(f"Submitting {len(to_do_list)} tasks to redis")
        logger.info(f"Type of to_do_list: {type(to_do_list)}")
        logger.info(f"to_do_list: {to_do_list}")
        target_queue = get_config("S1", "REDIS_PROCESSED_CHANNEL")
        imagery_path = get_config("S3", "IMAGERY_PATH")
        region = get_config("APP", "REGION")

        for index, row in to_do_list.iterrows():
            # convert row to dict
            row_dict = row.to_dict()
            row_dict['s3_bucket'] = get_config("S3", "BUCKET")
            row_dict["s3_dir"] = f"{imagery_path}/{region.lower()}/sentinel_1/"
            self._redis.publish(target_queue, json.dumps(row_dict))
        self._redis.close()

# def get_s1_asf_urls(s1_name_list: pd.Series):
#     df = pd.DataFrame()

#     num_parts = math.ceil(len(s1_name_list) / 119)
#     s1_name_lists = numpy.array_split(numpy.array(s1_name_list), num_parts)
#     for entry in s1_name_lists:
#         try:
#             df = df.append(
#                 pd.read_csv(
#                     f"https://api.daac.asf.alaska.edu/services/search/param?granule_list={','.join(entry)}&output=csv"
#                 ),
#                 ignore_index=True
#             )
#         except Exception as e:
#             logging.error(e)

#     return df.loc[df['Processing Level'] == 'GRD_HD']
