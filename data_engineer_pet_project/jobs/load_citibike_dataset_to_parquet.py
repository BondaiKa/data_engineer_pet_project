from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.datalake.landing import BaseLandingArea
from data_engineer_pet_project.jobs.base import BaseJob
from data_engineer_pet_project.jobs.session import Session


class BikeDatasetLandingJob(BaseJob):
    """Clean and convert bike dataset to parquet"""
    area = BaseLandingArea()

    def extract(self, date: datetime) -> DataFrame:
        return self.filter_df(
            dataset=Session().load_csv_file(
                paths=self.area.get_landing_bike_dataset_csv_paths(to_date=date),
                header=True)
        )

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, date: datetime):
        for path in self.area.get_landing_bike_dataset_parquet_paths(date):
            df.repartition(1).write.mode('overwrite').parquet(path=path)
