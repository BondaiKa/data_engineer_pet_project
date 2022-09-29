from abc import ABC
from datetime import datetime
from typing import List

from data_engineer_pet_project.base.utils import get_bike_weather_dataset_file_path
from data_engineer_pet_project.datalake.base import BaseDataLakeArea
from data_engineer_pet_project.datalake.landing import BaseLandingArea


class BaseStagingArea(BaseDataLakeArea, ABC):
    schemas = None
    AREA_CONTAINER = 'staging'

    def get_landing_weather_dataset_paths(self, date: datetime) -> List[str]:
        return BaseLandingArea().get_landing_weather_dataset_parquet_paths(to_date=date)

    def get_landing_bike_dataset_paths(self, date: datetime) -> List[str]:
        return BaseLandingArea().get_landing_bike_dataset_parquet_paths(to_date=date)

    def get_staging_joined_dataset_parquet_paths(self, date: datetime) -> List[str]:
        filename = f"{get_bike_weather_dataset_file_path(date=date)}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_bike_weather_dataset_name)
