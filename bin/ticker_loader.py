import polars as pl
import re
import sqlite3

from bin.setup_logger import setup_logger
from bin.read_query import read_query

class TickerLoader:
  def __init__(self, con: str, logger=setup_logger("TickerLoader()")):
    self.con = con
    self.logger = logger

  def get_tickers(self, sector) -> pl.DataFrame:
    query_file = "get_tickers_sector.sql" if sector else "get_all_tickers.sql"
    query = read_query(query_file)
    query = re.sub(r"<placeholder>", sector, query) if sector else query
    with sqlite3.connect(self.con) as conn:
      df = pl.read_database(query, conn)
    return df

