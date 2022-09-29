from abc import ABC, ABCMeta
from pathlib import Path
from typing import Iterable, List, Union

from data_engineer_pet_project.config import Config


class BaseDataLakeArea(metaclass=ABCMeta):
    """Base class for all data lake areas that should implement given interface."""
    schemas: type
    config = Config()
    AREA_CONTAINER: str
    BASE_PATH: str

    def add_hdfs_protocol_prefix(self, path: Union[str, Path]) -> str:
        """Add hdfs protocol `hdfs://`"""
        return f"{self.config.get_hdfs_url}/{path}"

    def add_container_area_prefix(self, path: Union[str, Path]) -> str:
        """Add datalake area name """
        return f"{self.AREA_CONTAINER}/{path}"

    def add_base_hdfs_prefix(self, path: Union[str, Path]):
        """Add directory path inside hdfs

        like `/user/karim/`"""
        return f"{self.config.get_hdfs_dataset_core_path}/{path}"

    def add_dataset_name_prefix(self, path: Union[str, Path], dataset_name: str):
        return f"{dataset_name}/{path}"

    def get_full_paths(self, paths: Iterable[Union[str, Path]], dataset_name: str) -> List[str]:
        """Add hdfs protocol, hostname, ip, base directory and area prefixes"""
        return [
            self.add_hdfs_protocol_prefix(
                self.add_base_hdfs_prefix(
                    self.add_container_area_prefix(
                        self.add_dataset_name_prefix(path, dataset_name=dataset_name)))) for path in paths]


class BaseStagingArea(BaseDataLakeArea, ABC):
    schemas = None
    ...


class BasePublicArea(BaseDataLakeArea, ABC):
    schemas = None
    ...
