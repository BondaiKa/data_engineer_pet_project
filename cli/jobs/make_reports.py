import logging
from datetime import datetime

import click

from data_engineer_pet_project.jobs.make_reports import BikeTripTemperatureDependencyJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--date', '-d', type=click.DateTime(), required=True)
def create_dataset_temperature_report_cli(date: datetime):
    """Create report of bike trip of temperature dependency

    :param date: last month date
    :return:
    """
    log.info(f"Find bike trips from temperature dependency {date:'%Y-%m'}...")
    trip_temp_depend_job = BikeTripTemperatureDependencyJob()
    trip_temp_depend_job.run(date=date)
    log.info(f"`Writing bike trip temperature dependency done...")
