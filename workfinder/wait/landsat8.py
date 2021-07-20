import json

from libcatapult.queues.base_queue import BaseQueue

from workfinder import get_config
from workfinder.api.espa_api import EspaAPI
from workfinder.wait.basewaiter import BaseWaiter


class Landsat8(BaseWaiter):

    def __init__(self, q: BaseQueue, espa: EspaAPI):
        super().__init__(q)
        self.espa = espa

    def check_order(self, order_id: str):
        resp = self.espa.call(f'item-status/{order_id}', body={'status': 'complete'})
        return len(resp) > 0

    def send_complete_order(self, order_details: dict):
        order_id = order_details['orderid']
        resp = self.espa.call(f"item-status/{order_details['orderid']}", body={'status': 'complete'})
        target_queue = get_config("landsat8", "redis_channel")
        target_bucket = get_config("AWS", "bucket")
        for item in resp[order_id]:
            url = item.get('product_dload_url')
            if 'LE07' in url:
                nm = 'landsat_7'
            elif 'LT05' in url:
                nm = 'landsat_5'
            elif 'LT04' in url:
                nm = 'landsat_5'
            elif 'LC08' in url:
                nm = 'landsat_8'
            else:
                raise Exception("unknown landsat url")
            # build payload object
            # TODO: this s3_dir should to be configurable
            payload = {"in_scene": url, "s3_bucket": target_bucket, "s3_dir": f"common_sensing/{orderref}/{nm}/"}
            # send to redis
            self.q.publish(target_queue, json.dumps(payload))

    def get_redis_source_queue(self):
        return get_config("landsat8", "redis_pending_channel")
