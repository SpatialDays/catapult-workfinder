import logging
import math

import numpy
import pandas as pd
from libcatapult.storage.s3_tools import S3Utils
from sentinelsat import SentinelAPI

from workfinder import get_config
from workfinder.search import get_aoi
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class S1 (BaseWorkFinder):

    def find_work_list(self):
        aoi = get_aoi()
        region = get_config("app", "region")
        esa_api = SentinelAPI(get_config("copernicus", "username"), get_config("copernicus", "pwd"))
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
        access = get_config("S3", "access_key_id")
        secret = get_config("S3", "secret_access_key")
        bucket_name = get_config("S3", "bucket")
        endpoint_url = get_config("S3", "end_point")
        s3_region = get_config("S3", "region")

        s3 = S3Utils(access, secret, bucket_name, endpoint_url, s3_region)

        path_sizes = s3.list_files_with_sizes(f'common_sensing/{region}/sentinel_1/')
        return path_sizes


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
