
from libcatapult.queues.nats import NatsQueue
from workfinder import get_config
from workfinder.api.s3 import S3Api
from workfinder.search.base_ard_work_finder import BaseArdWorkFinder


class Landsat8ARD (BaseArdWorkFinder):

    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_8"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path")

