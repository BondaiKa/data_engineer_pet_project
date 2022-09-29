from abc import ABC
from datetime import datetime

from data_engineer_pet_project.base.utils import get_bike_weather_temperature_report_file_path
from data_engineer_pet_project.datalake.base import BaseDataLakeArea
from data_engineer_pet_project.datalake.staging import BaseStagingArea


class BasePublicArea(BaseDataLakeArea, ABC):
    schemas = None
    AREA_CONTAINER = 'public'

    def get_staging_joined_dataset_parquet_paths(self, date: datetime):
        return BaseStagingArea().get_staging_joined_dataset_parquet_paths(date=date)

    def get_public_bike_weather_temperature_report_path(self, date):
        filename = f"{get_bike_weather_temperature_report_file_path(date=date)}.csv"
        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_bike_weather_dataset_name)
