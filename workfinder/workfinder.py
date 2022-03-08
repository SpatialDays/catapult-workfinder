"""Main module."""
import asyncio
import logging
import sys

import click

from workfinder.search.Landsat import Landsat8
from workfinder import default_s3_api, default_nats_api, default_redis_api, default_esa_api, \
    default_espa_api

from workfinder.search.landsat_ard import Landsat8ARD, Landsat4ARD, Landsat5ARD, Landsat7ARD
from workfinder.search.mlwater_ard import Landsat4MlWaterArd, Landsat8MlWaterArd, Landsat7MlWaterArd, \
    Landsat5MlWaterArd, Sentinel1MlWaterArd, Sentinel2MlWaterArd
from workfinder.search.s1 import S1
from workfinder.search.s1_ard import S1ARD
from workfinder.search.s2_ard import S2ARD

# Mapping of processor names to objects.
# Note: Names must be upper case.
processors = {
    "S1": S1(default_s3_api(), default_redis_api(), default_esa_api()),
    "S1_ARD": S1ARD(default_s3_api(), default_nats_api()),
    "S2_ARD": S2ARD(default_s3_api(), default_nats_api()),
    "LANDSAT8": Landsat8(default_s3_api(), default_redis_api(), default_espa_api()),
    "LANDSAT8_ARD": Landsat8ARD(default_s3_api(), default_nats_api()),
    "LANDSAT7_ARD": Landsat7ARD(default_s3_api(), default_nats_api()),
    "LANDSAT5_ARD": Landsat5ARD(default_s3_api(), default_nats_api()),
    "LANDSAT4_ARD": Landsat4ARD(default_s3_api(), default_nats_api()),
    "LANDSAT8_MLWATER": Landsat8MlWaterArd(default_s3_api(), default_nats_api()),
    "LANDSAT7_MLWATER": Landsat7MlWaterArd(default_s3_api(), default_nats_api()),
    "LANDSAT5_MLWATER": Landsat5MlWaterArd(default_s3_api(), default_nats_api()),
    "LANDSAT4_MLWATER": Landsat4MlWaterArd(default_s3_api(), default_nats_api()),
    "S2_MLWATER": Sentinel2MlWaterArd(default_s3_api(), default_nats_api()),
    "S1_MLWATER": Sentinel1MlWaterArd(default_s3_api(), default_nats_api()),

}


@click.command()
@click.option('--limit', default=-1, help='Number records to process', )
@click.argument("process")
def main(process, limit):

    param = process.upper()
    if param in processors:
        logging.info(f"starting work search for {param}")
        work = processors[param].find_new_work()
        if 0 < limit < work.size:
            processors[param].submit_tasks(work.sample(n=limit))
        else:
            processors[param].submit_tasks(work)

        logging.info(f"done work search for {param}")
    else:
        print(f"unknown processor type {param}")


if __name__ == '__main__':
    main()
