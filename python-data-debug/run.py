def run():

    # Mapping column names to the input data.
    #  * Column 1 - time
    #  * Column 2 - humidity
    #  * Column 3 - salinity
    #  * Column 4 - air_temperature
    #  * Column 5 - water_temperature
    #  * Column 6 - wind_speed

    COLUMN_NAMES = [
        "time",
        "humidity",
        "salinity",
        "air_temperature",
        "water_temperature",
        "wind_speed"
    ]

    # Load data from a local CSV file into a list of lists
    data = []
    with open('data.csv') as csvdata:
        csv_reader = csv.reader(csvdata)
        for row in csv_reader:
            data.append(row)

    # Turn all of the data into numerical values
    # so we can take the average of each column
    numeric_data = []
    for row in data:
        new_row = []
        for value in row:
            new_row.append(float(value))
        numeric_data.append(new_row)

    # Organize the data into columns
    column_2 = []
    column_3 = []
    column_4 = []
    column_5 = []
    column_6 = []
    for row in numeric_data:
        column_2.append(row[2])
        column_3.append(row[3])
        column_4.append(row[4])
        column_5.append(row[5])
        column_6.append(row[6])

    # Calculate the average of each column
    col_2_avg = sum(column_2) / len(column_2)
    col_3_avg = sum(column_3) / len(column_3)
    col_4_avg = sum(column_4) / len(column_4)
    col_5_avg = sum(column_5) / len(column_5)
    col_6_avg = sum(column_6) / len(column_6)

    # Return the averages of each column
    return {
        'humidity': col_2_avg,
        'salinity': col_3_avg,
        'air_temperature': col_4_avg,
        'water_temperature': col_5_avg,
        'wind_speed': col_6_avg
    }


if __name__ == '__main__':
    import sys
    import time
    import math

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
