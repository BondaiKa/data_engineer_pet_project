#!/bin/bash

DATE=$1
echo "Date param: $DATE"

echo "1. Start to change datasets format from .csv to .parquet..."

echo "2. Start to change citibike dataset format..."
python3 data-engineer-pet-project-cli.py load-bike-dataset-to-parquet-cli --date "$DATE"

echo "3. Start to change weather dataset format..."
python3 data-engineer-pet-project-cli.py load-weather-dataset-to-parquet-cli --date "$DATE"

echo "4. Join weather and citibike datasets..."
python3 data-engineer-pet-project-cli.py join-citibike-weather-datasets-cli --date "$DATE"

echo "5. Create reports..."
echo "6. Create bike trip temperature dependency report..."
python3 data-engineer-pet-project-cli.py create-dataset-temperature-report-cli --date "$DATE"

echo "Done..."
