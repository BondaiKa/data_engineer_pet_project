import logging
from pathlib import Path
from typing import Any, Dict, Union

import yaml

log = logging.getLogger(__name__)


class YmlConfigReader:
    """YAML config file reader"""

    DEFAULT_DIRECTORY_PATH = Path(__file__).parent.resolve()
    DEFAULT_FILE_FOLDER = 'files'
    DEFAULT_FILE_FORMAT = 'yml'
    DEFAULT_FILE_NAME = 'default'

    def __init__(self,
                 filename=DEFAULT_FILE_NAME,
                 directory_path: Union[str, Path] = DEFAULT_DIRECTORY_PATH,
                 file_folder: str = DEFAULT_FILE_FOLDER,
                 file_format: str = DEFAULT_FILE_FORMAT):

        self.filename = filename
        self.directory_path = directory_path
        self.file_format = file_format
        self.file_folder = file_folder

    @property
    def file_path(self) -> Path:
        return Path(self.directory_path) / self.file_folder / f"{self.filename}.{self.file_format}"

    def read(self) -> Dict[str, Any]:
        """Read configuration from file
        Read and parse yaml configuration file by rule: `<directory_path>/<filename>.<file_format>`
        :return: configuration dictionary
        """
        try:
            log.debug(f'Read config from {self.file_path}')
            with open(self.file_path) as config_file:
                return yaml.load(config_file, Loader=yaml.Loader)
        except FileNotFoundError as ex:
            log.error(ex)
            raise ex
