import logging
from datetime import datetime

import click

from data_engineer_pet_project.jobs.load_citibike_dataset_to_parquet import BikeDatasetLandingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def load_bike_dataset_to_parquet_cli(date: datetime):
    """Change citibike files format from csv to parquet

    :param date: last month date
    :return:
    """
    log.info(f"Run changing citibike dataset format from csv to parquet for {date:'%Y-%m'}...")
    bike_dataset_job = BikeDatasetLandingJob()
    bike_dataset_job.run(date=date)
    log.info(f"`loading citibike dataset to parquet cli done...")
