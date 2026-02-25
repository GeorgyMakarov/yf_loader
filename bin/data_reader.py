import sqlite3
import pandas as pd
import polars as pl
import yfinance as yf
from datetime import datetime, timedelta
from bin.read_query import read_query
from bin.rename_yf import rename_yf

class DataReader:
  def __init__(self, con: str, logger, bs: int = 50000):
    self.con = con
    self.logger = logger
    self.bs = bs
  
  def get_latest_date(self, ticker_id: int):
    query = read_query("get_latest_date.sql")
    query = query.replace("<symbol_id>", str(ticker_id))
    with sqlite3.connect(self.con) as conn:
      df = pl.read_database(query, conn)
    if df.is_empty():
      return None
    df = df.with_columns(pl.col("latest_date").str.slice(0, 10))
    return df[0, 'latest_date']
  
  @staticmethod
  def _check_and_transform(data):
    if data is not None:
      schema = {
        "price_date": pl.Date,
        "open_price": pl.Float64,
        "high_price": pl.Float64,
        "low_price": pl.Float64,
        "close_price": pl.Float64,
        "volume": pl.Int64
      }      
      return pl.from_pandas(data[list(schema.keys())], schema_overrides=schema)
    else:
      return None
  
  def load_data(self, id, sd, ed):
    self.logger.info(f"Loading data for {id} from {sd} to {ed}...")
    data = yf.download(id, start=sd, end=ed, progress=False, auto_adjust=True)
    if data.empty:
      self.logger.warning(f"No data found for {id} from {sd} to {ed}.")
      return None
    data.index.name = None
    data.reset_index(inplace=True)
    data.columns = data.columns.get_level_values("Price")
    data.columns.name = None
    data.rename(columns=rename_yf, inplace=True)
    data = self._check_and_transform(data)
    return data
  
  def get_latest_id(self, query: str):
    with sqlite3.connect(self.con) as conn:
      df = pl.read_database(query, conn)
    if df.is_empty():
      return 0
    return df[0, 0]