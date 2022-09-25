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
