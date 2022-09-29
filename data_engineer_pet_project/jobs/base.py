import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project.datalake.base import BaseDataLakeArea
from data_engineer_pet_project.jobs.session import Session

log = logging.getLogger(__name__)


class BaseJob(metaclass=ABCMeta):
    """Base dataset job worker"""
    area: BaseDataLakeArea

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
    def save(self, df: DataFrame, date: datetime, *args, **kwargs):
        """Save results"""
        raise NotImplementedError

    def run(self, date: datetime):
        """run extracting, transforming and saving dataframe job"""
        log.info(f'Start to extract data for {date}...')
        df = self.extract(date)

        log.info(f'Start dataframe transformation for for {date}...')
        df = self.transform(df)

        log.info(f'Start save transformed results for for {date}...')
        self.save(df, date)

    @abstractmethod
    def filter_df(self, dataset: DataFrame) -> DataFrame:
        """Filter dataset"""
        raise NotImplementedError
