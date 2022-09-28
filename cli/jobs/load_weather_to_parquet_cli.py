import logging
from datetime import datetime

import click

from data_engineer_pet_project.jobs.load_weather_to_parquet import WeatherDatasetLandingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def load_weather_dataset_to_parquet_cli(date: datetime):
    """Change weather files format from csv to parquet

    :param date: last month date
    :return:
    """
    log.info(f"Run changing weather dataset format from csv to parquet for {date:'%Y-%m'}...")
    weather_dataset_job = WeatherDatasetLandingJob()
    weather_dataset_job.run(date=date)
    log.info(f"`loading weather dataset to parquet cli done...")
