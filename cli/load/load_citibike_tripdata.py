import logging
from datetime import datetime

import click

from data_engineer_pet_project.load.citibike_tripdata import load_citibike_dataset_locally, \
    load_citibike_dataset_to_hdfs

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def load_citibike_dataset_locally_cli(date: datetime) -> None:
    """Load citibike dataset locally cli command

    :param date: last month date
    :return:
    """
    log.info(f"Run `load_citibike_dataset_locally` cli for {date:'%Y-%m'}...")
    load_citibike_dataset_locally(date=date)
    log.info("Done...")


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def load_citibike_dataset_to_hdfs_cli(date: datetime) -> None:
    """Load citibike dataset to hdfs directly cli command

    :param date: last month date
    :return:
    """
    log.info(f"Run `load_citibike_dataset_hdfs` cli for {date:'%Y-%m'}...")
    load_citibike_dataset_to_hdfs(date=date)
    log.info("Done...")
