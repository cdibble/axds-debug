def run() -> dict:
    # Mapping column names to the input data
        # and expected type.
    #  * Column 1 - time
    #  * Column 2 - humidity
    #  * Column 3 - salinity
    #  * Column 4 - air_temperature
    #  * Column 5 - water_temperature
    #  * Column 6 - wind_speed

    COLUMN_MAPPING = {
        "time": "datetime64[ns]",
        "humidity": float,
        "salinity": float,
        "air_temperature": float,
        "water_temperature": float,
        "wind_speed": float
    }

    # Load data from a local CSV file into pd.DataFrame
    data: pd.DataFrame = pd.read_csv('data.csv', header = None, names = COLUMN_MAPPING.keys())
    # cast to desired types
    data = data.astype(COLUMN_MAPPING)
    # compute columnar means for numeric columns
    data_means: dict = data.mean(skipna=True, numeric_only=True).to_dict()
    # Return the averages of each column
    return data_means

if __name__ == '__main__':
    import sys
    import time
    import math
    import pandas as pd

    start = time.perf_counter()
    averages = run()
    end = time.perf_counter()

    CORRECT_HUMIDITY = 80.8129
    CORRECT_SALINITY = 36.1433
    CORRECT_AIR_TEMPERATURE = 19.7976
    CORRECT_WIND_TEMPERATURE = 34.1683
    CORRECT_WIND_SPEED = 5.6777

    ANSWERS = {
        'humidity': CORRECT_HUMIDITY,
        'salinity': CORRECT_SALINITY,
        'air_temperature': CORRECT_AIR_TEMPERATURE,
        'water_temperature':CORRECT_WIND_TEMPERATURE,
        'wind_speed': CORRECT_WIND_SPEED,
    }

    for column, value in ANSWERS.items():
        assert math.isclose(
            averages[column],
            value,
            rel_tol=1e-5,
        ), f"{column} should be {value}, instead {averages[column]}"

    print("Succesfully validated the data using {} in {} seconds".format(__file__, end - start))

    sys.exit(0)
