import pandas as pd
from typing import Optional

class CsvHandler:
    '''
    Generic class housing methods for ingesting and handling CSV data
    '''
    @staticmethod
    def _read_csv(csv_path: str, column_names: list, header: Optional[bool] = None) -> pd.DataFrame:
        '''
        Generic method wrapping pd.read_csv.

        Parameters
        -----------
        csv_path: str
            Path to CSV file
        column_names: list
            Ordered list used to name columns in CSV
        header: bool
            Whether CSV includes a header with column names
        
        Returns
        --------
        pd.DataFrame
        '''
        return pd.read_csv(csv_path, header = header, names = column_names)
    
    @staticmethod
    def _cast_types(data: pd.DataFrame, type_mapping: dict) -> pd.DataFrame:
        '''
        Convert types given a mapping of column_names:types

        Parameters
        -----------
        data: pd.DataFrame
            Data needing type conversion
        type_mapping: dict
            Mapping of column names in data to desired types
        
        Returns
        --------
        pd.DataFrame
        '''
        return data.astype(type_mapping)

class SensorData(CsvHandler):
    COLUMN_MAPPING = {
        "time": "datetime64[ns]",
        "humidity": float,
        "salinity": float,
        "air_temperature": float,
        "water_temperature": float,
        "wind_speed": float
    }
    def __init__(self, csv_path: str) -> None:
        '''
        Handles sensor data ETL from CSV files with a schema given by SensorData.COLUMN_MAPPING

        Parameters
        -----------
        csv_path: str
            Path to a CSV file. Assumed headerless and matching schema given by SensorData.COLUMN_MAPPING, though this can be over-ridden on a given object instance.
        
        Returns
        --------
        None
            Assigns self.data attribute with pd.DataFrame containing properly typed data.
        '''
        super().__init__()
        # Load data from a local CSV file into pd.DataFrame
        self.data: pd.DataFrame = self._read_csv(csv_path = csv_path, column_names = self.COLUMN_MAPPING.keys())
        self.data= self._cast_types(data = self.data, type_mapping = self.COLUMN_MAPPING)
    
    def get_column_means(self, columns: Optional[list] = None) -> dict:
        '''
        Computes columnar means of data attribute's numeric columns, skipping NA values.

        Parameters
        -----------
        columns: list
            Used to subset to certain columns. Defaults to all numeric columns

        Returns
        --------
        dict
            maps column names to mean value.
        '''
        if not columns:
            columns = self.data.columns
        return self.data[columns].mean(skipna = True, numeric_only = True).to_dict()

def run() -> dict:
    sensor_csv = SensorData(csv_path='data.csv')
    return sensor_csv.get_column_means()

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
