import pandas as pd
import mysql.connector as sql
from typing import List

class DataTable:

  def __init__(self, name: str, df: pd.DataFrame):
    self.name = name
    self.df = df

  def set_name(self, name:str):
    self.name = name

  def fetch_name(self) -> str:
    return self.name

  def fetch_table(self) -> pd.DataFrame:
    return self.df


class DataDict:
  
  def __init__(self, input_dt: List[DataTable]):
    self.data_dict = {}
    for dt in input_dt:
      self.data_dict[dt.fetch_name()] = dt

  def edit_dt(self, dt: DataTable) -> bool:
    self.data_dict[dt.fetch_name()] = dt
    return True

  def fetch_dt(self, name: str) -> DataTable:
    return self.data_dict[name]


class DataReader:

  @staticmethod
  def generate_csv_data_dict(file_path: str, table_name: str):
    df = pd.read_csv(file_path)
    dt = DataTable(table_name, df)
    return DataDict([dt])

  @staticmethod
  def generate_excel_data_dict(file_path: str):
    df_dict = pd.read_excel(file_path, sheet_name=None)
    dt_list = []
    for sheet in df_dict:
      dt_list.append(DataTable(sheet, df_dict[sheet]))
    return DataDict(dt_list)

  @staticmethod
  def generate_sql_data_dict(host: str, db_name: str, username: str, password: str, table_name_list: List[str]):
    db_connection = sql.connect(host=host, database=db_name, user=username, password=password)
    dt_list = []
    for table_name in table_name_list:
      df = pd.read_sql('SELECT * FROM ' + table_name, con=db_connection)
      dt_list.append(DataTable(table_name, df))
    return DataDict(dt_list)
