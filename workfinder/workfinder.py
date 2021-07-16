"""Main module."""
import logging
import sys

import click

from workfinder.search.Landsat import Landsat8
from workfinder import get_default_s3_api, get_default_nats_api, get_default_redis_api, get_default_esa_api
from workfinder.search.s1 import S1
from workfinder.search.s1_ard import S1ARD

# Mapping of processor names to objects.
# Note: Names must be upper case.

processors = {
    "S1": S1(get_default_s3_api(), get_default_redis_api(), get_default_esa_api()),
    "S1_ARD": S1ARD(get_default_s3_api(), get_default_nats_api()),
    "LANDSAT8": Landsat8(get_default_s3_api(), get_default_redis_api()),
}


@click.command()
@click.option('--limit', default=-1, help='Number records to process', )
@click.argument("process")
def main(process, limit):

    param = process.upper()
    if param in processors:
        logging.info(f"starting work search for {param}")
        work = processors[param].find_new_work()
        if limit > 0:
            processors[param].submit_tasks(work.head(limit))
        else:
            processors[param].submit_tasks(work)
        logging.info(f"done work search for {param}")
    else:
        print(f"unknown processor type {param}")


if __name__ == '__main__':
    main()
