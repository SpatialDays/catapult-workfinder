import logging
import pathlib

import pandas as pd
from libcatapult.queues.nats import NatsQueue
from workfinder import get_config
from workfinder.api.s3 import S3Api
from workfinder.search import list_catalog, get_ard_list
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class LandsatARD (BaseWorkFinder):

    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__()
        self.s3 = s3
        self.nats = nats

    def submit_tasks(self, to_do_list: pd.DataFrame):
        # get nats connection
        item_channel = get_config("landsat_ard", "item_nats_channel")
        collection_channel = get_config("landsat_ard", "collection_nats_channel")
        stac_key = get_config("landsat_ard", "stac_collection_path")

        self.nats.connect()
        self.nats.publish(collection_channel, stac_key)

        for index, r in to_do_list.iterrows():
            path = '/'.join(r['url'].split('/')[0:-1]) + '/'
            logging.info(f"publishing {r['url']} as {path}")
            self.nats.publish(item_channel, path)
        self.nats.close()

    def find_work_list(self):
        self.s3.get_s3_connection()
        region = get_config("app", "region")
        return get_ard_list(self.s3, f"common_sensing/{region.lower()}/landsat_8/")

    def find_already_done_list(self):
        self.s3.get_s3_connection()
        stac_key = get_config("landsat_ard", "stac_collection_path")
        path_sizes = list_catalog(self.s3, stac_key)
        # map filenames to ids
        df_result = pd.DataFrame({'id': []})
        for r in path_sizes:
            df_result = df_result.append({'id': r}, ignore_index=True)
        return df_result
