from typing import Tuple
from datetime import datetime
import pandas
DATASOURCE_CSV = 'csv'

class DataSource:
    def __init__(self, source_type:str):
        self.source_type = source_type
        self.min_time : datetime = None
        self.max_time : datetime = None

    def set_time_range(self, min_time: datetime, max_time:datetime):
        self.min_time = min_time
        self.max_time = max_time

    def get_time_range(self):
        return (self.min_time, self.max_time)

    def has_time_range(self):
        return self.min_time is not None

    def is_csv(self):
        return self.source_type == DATASOURCE_CSV

    def load(self)->pandas.DataFrame:
        pass

class CSVDataSource(DataSource):

    def __init__(self, csv_file: str, time_column:str):
        super().__init__(DATASOURCE_CSV)
        self.csv_file = csv_file

    def load(self)->pandas.DataFrame:
        rows = pandas.read_csv(self.csv_file)
        return rows
        


