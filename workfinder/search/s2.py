import pandas as pd

from workfinder import get_config
from workfinder.search import get_aoi_wkt
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class S2(BaseWorkFinder):

    def __init__(self, s3: S3Api, redis: BaseQueue, esa_api: SentinelAPI):
        super().__init__()
        self._s3 = s3
        self._redis = redis
        self._esa_api = esa_api

    def find_work_list(self):
        self._s3.get_s3_connection()

        region = get_config("app", "region")
        aoi = get_aoi_wkt(self._s3, region)
        print(self._esa_api.dhus_version)
        res = self._esa_api.query(
            area=aoi,
            platformname='Sentinel-2',
            producttype='S2MSI1C',
        )
        esa_l2a = self._esa_api.to_geodataframe(res)

        res = self._esa_api.query(
            area=aoi, 
            platformname='Sentinel-2',
            producttype='S2MSI1C'
        )
        esa_l1c = self._esa_api.to_geodataframe(res)


        esa_l1c['scenename'] = esa_l1c.title.apply(
            lambda x: f"{x.split('_')[0]}_{x.split('_')[1]}_{x.split('_')[2]}_{x.split('_')[5]}")
        esa_l2a['scenename'] = esa_l2a.title.apply(
            lambda x: f"{x.split('_')[0]}_MSIL1C_{x.split('_')[2]}_{x.split('_')[5]}")
        esa_l1c['granules'] = esa_l1c.identifier.str[39:44]
        esa_l2a['granules'] = esa_l2a.identifier.str[39:44]

    def find_already_done_list(self):
        pass

    def submit_tasks(self, to_do_list: pd.DataFrame):
        pass
