import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.jobs.session import Session

log = logging.getLogger(__name__)


class BaseJob(metaclass=ABCMeta):
    """Base dataset job worker"""

    def extract(self, date: datetime) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_dataframe(paths=self._get_dataset_paths(date))
        )

    def _get_dataset_paths(self, date: datetime):
        raise NotImplementedError

    @abstractmethod
    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        """Apply transformations"""
        raise NotImplementedError

    @abstractmethod
    def save(self, df: DataFrame, *args, **kwargs):
        """Save results"""
        raise NotImplementedError

    def run(self, date: datetime):
        df = self.extract(date)
        df = self.transform(df)
        self.save(df, date)

    @abstractmethod
    def filter_df(self, dataset: DataFrame) -> DataFrame:
        """Filter dataset"""
        raise NotImplementedError
