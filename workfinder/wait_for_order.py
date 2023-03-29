import logging

import click

from workfinder import default_redis_api, default_espa_api
from workfinder.wait.landsat import Landsat8, Landsat7, Landsat5, Landsat4

processors = {
    "LANDSAT8": Landsat8(default_redis_api(), default_espa_api()),
    "LANDSAT7": Landsat7(default_redis_api(), default_espa_api()),
    "LANDSAT5": Landsat5(default_redis_api(), default_espa_api()),
    "LANDSAT4": Landsat4(default_redis_api(), default_espa_api())
}


@click.command()
@click.argument("process")
def main(process):
    param = process.upper()
    if param in processors:
        logging.info(f"starting completed order search for {param}")
        processors[param].process()
        logging.info(f"DONE completed order search for {param}")
    else:
        print(f"unknown processor type {param}")


if __name__ == '__main__':
    main()
