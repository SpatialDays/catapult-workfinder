import logging
import math
import os
from pathlib import Path

import numpy
import pandas as pd
from libcatapult.storage.s3_tools import S3Utils
from sentinelsat import SentinelAPI

from workfinder import get_config
from workfinder.search import get_aoi, get_redis_connection, list_s3_files, get_ard_list
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class S1 (BaseWorkFinder):

    def submit_tasks(self, to_do_list):

        channel = get_config("s1", "redis_channel")
        # get redis connection
        conn = get_redis_connection()
        # submit each task.
        for e in to_do_list:
            payload = {
                "in_scene": e['id'],
                "s3_bucket": "public-eo-data",
                "s3_dir": "test/sentinel_1/",
                "ext_dem": f"common_sensing/ancillary_products/SRTM1Sec/SRTM30_Fiji_{e['hemisphere']}.tif"}
            conn.publish(channel, payload)
        pass

    def find_work_list(self):
        aoi = get_aoi()
        region = get_config("app", "region")
        user = get_config("copernicus", "username")
        pwd = get_config("copernicus", "pwd")
        logging.info(f"{user} #### {pwd}")

        esa_api = SentinelAPI(user, pwd)
        print(esa_api.dhus_version)
        res = esa_api.query(
            area=aoi,
            platformname='Sentinel-1',
            producttype='GRD',
            sensoroperationalmode='IW'
        )

        esa_grd = esa_api.to_geodataframe(res)
        asf_grd_matches = get_s1_asf_urls(esa_grd.title.values)

        df = pd.merge(left=esa_grd, right=asf_grd_matches, how='left', left_on='title', right_on='Granule Name')
        for n, g in zip(aoi.NAME, aoi.geometry):
            df[n] = df.geometry.apply(lambda x: gpd.GeoSeries(x).intersects(g))
        # Filter based on any True intersections
        df[region] = df[df[aoi.NAME.values]].any(1)
        scenes = df[df[region] is True]
        return scenes

    def find_already_done_list(self):
        region = get_config("app", "region")
        return get_ard_list(f"common_sensing/{region.lower()}/sentinel_1/")


def get_s1_asf_urls(s1_name_list):
    df = pd.DataFrame()

    num_parts = math.ceil(len(s1_name_list) / 119)
    s1_name_lists = numpy.array_split(numpy.array(s1_name_list), num_parts)
    logging.debug([len(l) for l in s1_name_lists])

    for entry in s1_name_lists:
        try:
            df = df.append(
                pd.read_csv(
                    f"https://api.daac.asf.alaska.edu/services/search/param?granule_list={','.join(entry)}&output=csv"
                ),
                ignore_index=True
            )
        except Exception as e:
            logging.error(e)

    return df.loc[df['Processing Level'] == 'GRD_HD']


def _extract_id_ard_path(p: str):
    parts = Path(os.path.split(p)[0]).stem
    return parts
