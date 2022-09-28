import logging
from pathlib import Path
from typing import Any, Dict

from data_engineer_pet_project.base import MetaSingleton
from data_engineer_pet_project.config.reader import YmlConfigReader

log = logging.getLogger(__name__)


class Config(metaclass=MetaSingleton):
    def __init__(self, cfg=None):
        self._cfg: Dict[str, Any] = cfg if cfg else YmlConfigReader().read()
        self.datasets = self._cfg.get('datasets', dict())
        self.hdfs = self._cfg.get('hdfs', dict())

    @property
    def spark_conf(self) -> Dict[str, str]:
        return self._cfg.get('spark', dict())

    @property
    def get_bike_dataset_local_core_path(self) -> Path:
        return Path(self.datasets.get('citibike_local_core_path'))

    @property
    def get_bike_dataset_bucket_name(self) -> str:
        return self.datasets.get('citibike_bucket_name')

    @property
    def get_hdfs_url(self) -> str:
        return f"hdfs://{self.hdfs.get('ip_address')}:{self.hdfs.get('port')}"

    @property
    def get_hdfs_bike_dataset_name(self) -> Path:
        return Path(self.hdfs.get("bike_dataset_name"))

    @property
    def get_hdfs_weather_dataset_name(self) -> Path:
        return Path(self.hdfs.get("weather_dataset_name"))

    @property
    def get_hadoop_user(self) -> str:
        return self.hdfs.get('user')

    @property
    def get_hdfs_bike_weather_core_path(self) -> Path:
        return Path(self.hdfs.get("bike_weather_path"))

    @property
    def get_hdfs_dataset_core_path(self) -> Path:
        return Path(self.hdfs.get("hdfs_dataset_core_path"))
