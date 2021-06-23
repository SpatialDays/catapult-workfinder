import logging
import os
from pathlib import Path

import pandas as pd

from workfinder import get_config
from workfinder.search import list_s3_files, nats_connect, nats_publish, nats_close, list_catalog, get_ard_list
from workfinder.search.BaseWorkFinder import BaseWorkFinder


class S1ARD (BaseWorkFinder):

    def submit_tasks(self, to_do_list):
        # get nats connection
        channel = get_config("s1_ard", "nats_channel")
        nats_connect()
        for index, r in to_do_list.iterrows():
            logging.info(f"publishing {r['url']}")
            nats_publish(channel, r['url'])
        nats_close()

    def find_work_list(self):
        region = get_config("app", "region")
        return get_ard_list(f"common_sensing/{region.lower()}/sentinel_1/")

    def find_already_done_list(self):
        stac_key = get_config("s1_ard", "stac_collection_path")
        path_sizes = list_catalog(stac_key)
        # map filenames to ids
        df_result = pd.DataFrame({'id': []})
        for r in path_sizes:
            df_result = df_result.append({'id': r}, ignore_index=True)
        return df_result


def _extract_id_ard_path(p: str):
    parts = Path(os.path.split(p)[0]).stem
    return parts
