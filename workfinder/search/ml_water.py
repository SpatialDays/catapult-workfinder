import json
from abc import abstractmethod

import pandas as pd
from libcatapult.queues.base_queue import BaseQueue

from workfinder import S3Api, get_config
from workfinder.search.wofs import BaseWofs


class BaseMLWater(BaseWofs):

    @abstractmethod
    def get_source_sensor_name(self):
        pass

    @abstractmethod
    def get_target_name(self):
        pass

    def __init__(self, s3: S3Api, redis: BaseQueue):
        super().__init__(s3, redis)



class Landsat8MLWater(BaseMLWater):

    def get_source_sensor_name(self):
        return "landsat_8"

    def get_target_name(self):
        return "landsat_8_mlwater"


class Landsat7MLWater(BaseMLWater):
    def get_source_sensor_name(self):
        return "landsat_7"

    def get_target_name(self):
        return "landsat_7_mlwater"


class Landsat5MLWater(BaseMLWater):
    def get_source_sensor_name(self):
        return "landsat_5"

    def get_target_name(self):
        return "landsat_5_mlwater"


class S2MLWater(BaseMLWater):
    def get_source_sensor_name(self):
        return "sentinel_2"

    def get_target_name(self):
        return "sentinel_2_mlwater"


class S1MLWater(BaseMLWater):
    def get_source_sensor_name(self):
        return "sentinel_1"

    def get_target_name(self):
        return "sentinel_1_mlwater"
