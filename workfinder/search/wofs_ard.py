from libcatapult.queues.nats import NatsQueue

from workfinder import get_config, S3Api
from workfinder.search.base_ard_work_finder import BaseArdWorkFinder


class Landsat8WofsArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_8_wofs"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_8_wofs")


class Landsat7WofsArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_7_wofs"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_7_wofs")


class Landsat5WofsArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_5_wofs"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_5_wofs")

class Sentinel2WofsArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "sentinel_2_wofs"

    def get_stac_key(self):
        return get_config("s2_ard", "stac_collection_path_wofs")
