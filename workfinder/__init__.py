"""Top-level package for workfinder."""

__author__ = """Emily Selwood"""
__email__ = 'Emily.selwood@sa.catapult.org.uk'
__version__ = '0.1.0'

import configparser
import logging
from os import environ

_config = configparser.ConfigParser()
_config.read("config.cfg")


def get_config(section, key):
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


logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
logging.getLogger("fiona").setLevel(logging.INFO)
logging.getLogger("boto3").setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
