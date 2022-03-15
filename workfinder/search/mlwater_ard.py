from libcatapult.queues.nats import NatsQueue

from workfinder import get_config, S3Api
from workfinder.search.base_ard_work_finder import BaseArdWorkFinder


class Landsat8MlWaterArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_8_mlwater"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_8_mlwater")


class Landsat7MlWaterArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_7_mlwater"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_7_mlwater")


class Landsat5MlWaterArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "landsat_5_mlwater"

    def get_stac_key(self):
        return get_config("landsat_ard", "stac_collection_path_5_mlwater")


class Sentinel2MlWaterArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "sentinel_2_mlwater"

    def get_stac_key(self):
        return get_config("s2_ard", "stac_collection_path_mlwater")


class Sentinel1MlWaterArd(BaseArdWorkFinder):
    def __init__(self, s3: S3Api, nats: NatsQueue):
        super().__init__(s3, nats)

    def get_sensor_name(self):
        return "sentinel_1_mlwater"

    def get_stac_key(self):
        return get_config("s1_ard", "stac_collection_path_mlwater")
