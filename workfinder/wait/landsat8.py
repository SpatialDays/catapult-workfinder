import json
import logging
from typing import List, Dict

from libcatapult.queues.base_queue import BaseQueue

from workfinder import get_config
from workfinder.api.espa_api import EspaAPI
from workfinder.wait.basewaiter import BaseWaiter


class Landsat8(BaseWaiter):

    def __init__(self, q: BaseQueue, espa: EspaAPI):
        super().__init__(q)
        self.espa = espa

    def check_order(self, order_id: str):
        resp = self.espa.call(f'item-status/{order_id}')
        order_as_list: List[Dict] = resp[order_id]
        # only if each dict in the list has a status of complete or unavailable return true
        # return all([x['status'] in ['complete', 'unavailable'] for x in order_as_list])
        for i in order_as_list:
            if i['status'] not in ['complete', 'unavailable']:
                logging.info(f"Item {i['name']} in order {order_id} is not complete or unavailable")
                return False
        return True

    def send_complete_order(self, order_details: dict):
        order_id = order_details['order_id']
        resp = self.espa.call(f"item-status/{order_details['order_id']}", body={'status': 'complete'})
        target_queue = get_config("landsat8", "redis_channel")
        target_bucket = get_config("AWS", "bucket")
        region = get_config("app", "region")

        landsat_registry = {
            'LE04': 'landsat_4',
            'LE05': 'landsat_5',
            'LE07': 'landsat_7',
            'LE08': 'landsat_8',

            'LC04': 'landsat_4',
            'LC05': 'landsat_5',
            'LC07': 'landsat_7',
            'LC08': 'landsat_8',
        }

        for item in resp[order_id]:
            url = item.get('product_dload_url')
            logging.info(f"got url {url}")
            nm = None
            for k, v in landsat_registry.items():
                if k in url:
                    nm = v
                    break
            if not nm:
                raise Exception("unknown landsat url")
            # build payload object
            item_name = item['name']
            # TODO: this s3_dir should to be configurable
            payload = json.dumps(
                {"in_scene": url, "s3_bucket": target_bucket, "item": item_name,
                 "s3_dir": f"common_sensing/{region.lower()}/{nm}/"}
            )
            # send to redis
            logging.info(f"sending payload {payload}")
            self.q.publish(target_queue, payload)

    def get_redis_source_queue(self):
        return get_config("landsat8", "redis_pending_channel")
