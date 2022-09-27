from datetime import datetime


class MetaSingleton(type):
    """Metaclass for create singleton"""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


BIKE_DATASET_FILE_PATTERN = "citibike-tripdata"
BIKE_DATASET_CSV_FILE_EXTENSION = '.csv'
BIKE_DATASET_PARQUET_FILE_EXTENSION = '.parquet'
BIKE_DATASET_ZIP_FILE_EXTENSION = f'{BIKE_DATASET_CSV_FILE_EXTENSION}.zip'


def get_weather_dataset_file_path(start_date: datetime, end_date: datetime):
    """Get weather file name"""
    return f"new_york_{start_date.year}-{start_date.month:02d}-{start_date.day:02d}_to_{end_date.year}-{end_date.month:02d}-{end_date.day:02d}"


def get_bike_weather_dataset_file_path(date: datetime):
    return f"{date.year}{date.month:02d}_new_york_bike_weather"


def get_bike_weather_temperature_report_file_path(date: datetime):
    return f"{date.year}{date.month:02d}_temperature_dependency_bike_weather"
