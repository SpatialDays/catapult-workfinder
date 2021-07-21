"""Top-level package for workfinder."""

__author__ = """Emily Selwood"""
__email__ = 'Emily.selwood@sa.catapult.org.uk'
__version__ = '0.1.0'

import configparser
import logging
from os import environ

from libcatapult.queues.nats import NatsQueue
from libcatapult.queues.redis import RedisQueue
from sentinelsat import SentinelAPI

from workfinder.api.espa_api import EspaAPI
from workfinder.api.s3 import S3Api

_config = configparser.ConfigParser()
_config.read("config.cfg")


def get_config(section: str, key: str):
    """
    Get a configuration value from the environment if it is available if not fall back to the config file.

    Environment version of the values from the config file are the {section}_{key} upper cased.

    :param section: The configuration section to look in.
    :param key: The config key to return the value of.
    :return: the value of the config key.
    """
    env_result = environ.get(f"{section}_{key}".upper())
    if env_result:
        return env_result

    return _config.get(section, key)


console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(
    logging.Formatter('%(asctime)s %(levelname)s %(name)s %(filename)s:%(lineno)s -- %(message)s')
)

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
logging.getLogger("fiona").setLevel(logging.INFO)
logging.getLogger("boto3").setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("s3transfer").setLevel(logging.INFO)

logger.addHandler(console)


def default_s3_api():
    access = get_config("AWS", "access_key_id")
    secret = get_config("AWS", "secret_access_key")
    bucket_name = get_config("AWS", "bucket")
    endpoint_url = get_config("AWS", "end_point")
    s3_region = get_config("AWS", "region")

    return S3Api(access, secret, bucket_name, endpoint_url, s3_region)


def default_nats_api():
    nats_url = get_config("NATS", "url")
    return NatsQueue(nats_url)


def default_redis_api():
    host = get_config("REDIS", "host")
    port = get_config("REDIS", "port")
    return RedisQueue(host, port)


def default_esa_api():
    user = get_config("copernicus", "username")
    pwd = get_config("copernicus", "pwd")
    return SentinelAPI(user, pwd)


def default_espa_api():
    host = get_config("usgs", "host")
    username = get_config("usgs", "username")
    password = get_config("usgs", "password")
    return EspaAPI(host, username, password)
