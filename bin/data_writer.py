import polars as pl
import sqlite3

class DataWriter:
  def __init__(self, con: str, logger, bs: int = 50000):
    self.con = con
    self.logger = logger
    self.bs = bs

  def write_data(self, data: pl.DataFrame, table_name: str = "daily_price"):
    self.logger.info(f"Writing data to table {table_name}...")
    with sqlite3.connect(self.con) as conn:
      for start in range(0, data.height, self.bs):
        end = min(start + self.bs, data.height)
        batch = data.slice(start, end - start).to_pandas()
        batch.to_sql(table_name, conn, if_exists='append', index=False)
        self.logger.info(f"Wrote rows {start} to {end - 1} to {table_name}...")
    self.logger.info(f"Finished writing data to table {table_name}...")