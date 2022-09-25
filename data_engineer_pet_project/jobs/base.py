import logging
from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.jobs.session import Session

log = logging.getLogger(__name__)


class BaseJob:
    """Base dataset job worker"""

    def extract(self, date: datetime) -> DataFrame:
        return self.filter_df(
            dataset=Session().load_dataframe(paths=self._get_dataset_paths(date))
        )

    def _get_dataset_paths(self, date: datetime):
        raise NotImplementedError

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def save(self, df: DataFrame, date: datetime):
        # TODO:
        df.save()

    def run(self, date: datetime):
        df = self.extract(date)
        df = self.transform(df)
        self.save(df, date)

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        raise NotImplementedError
