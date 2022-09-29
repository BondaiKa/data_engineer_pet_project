import calendar
from abc import ABC
from datetime import datetime
from typing import List

from data_engineer_pet_project.base.utils import BIKE_DATASET_FILE_PATTERN, \
    get_weather_dataset_file_path
from data_engineer_pet_project.datalake.base import BaseDataLakeArea


class BaseLandingArea(BaseDataLakeArea, ABC):
    schemas = None
    AREA_CONTAINER = 'landing'

    def get_landing_bike_dataset_csv_paths(self, to_date: datetime, from_date: datetime = None, ) -> List[str]:
        """Get initial weather dataset on csv format paths"""
        filename = f"{to_date.year}{to_date.month:02d}-{BIKE_DATASET_FILE_PATTERN}.csv"
        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_bike_dataset_name)

    def get_landing_bike_dataset_parquet_paths(self, to_date: datetime, from_date: datetime = None, ) -> List[str]:
        """Get initial weather dataset on parquet format paths"""
        filename = f"{to_date.year}{to_date.month:02d}-{BIKE_DATASET_FILE_PATTERN}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_bike_dataset_name)

    def get_landing_weather_dataset_csv_paths(self, end_date: datetime, start_date: datetime = None) -> List[str]:
        """Get initial weather csv dataset paths"""
        start_date = datetime(year=end_date.year, month=end_date.month, day=1)
        filename = f"{get_weather_dataset_file_path(start_date=start_date, end_date=end_date)}.csv"
        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_weather_dataset_name)

    def get_landing_weather_dataset_parquet_paths(self, to_date: datetime, start_date: datetime = None) -> List[str]:
        """Get weather dataset with parquet format paths"""
        filename = get_weather_dataset_file_path(
            start_date=datetime(
                year=to_date.year,
                month=to_date.month,
                day=1,
            ),
            end_date=datetime(
                year=to_date.year,
                month=to_date.month,
                day=calendar.monthrange(to_date.year, to_date.month)[1]
            )
        )
        filename = f"{filename}.parquet"

        return self.get_full_paths(paths=[filename], dataset_name=self.config.get_hdfs_weather_dataset_name)
