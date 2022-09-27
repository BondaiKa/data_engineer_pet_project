import pandas as pd


def create_bike_weather_temperature_report(df: pd.DataFrame):
    df.plot(rot=0).bar(x='temp_group', y='numb_of_trip')


if __name__ == "__main__":
    file_path = "/Users/karim/src/data_engineer_pet_project/tmp/part-00000-33b9cc10-9bff-4b2e-bd58-99fbbffeb663-c000.csv"
    df = pd.read_csv(file_path)
    df.temp_group.map({0: "< 10°C", 1: "Between 10°C and 20°C", 2: "More than 20°C"})
    create_bike_weather_temperature_report(df=df)
