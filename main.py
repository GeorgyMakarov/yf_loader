import time
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from bin.setup_logger import setup_logger
from bin.read_args import read_args
from bin.add_columns import add_columns
from bin.ticker_loader import TickerLoader
from bin.data_reader import DataReader
from bin.data_writer import DataWriter

ARG = ['sector']
CON = r"C:\sqlite\\trading.db"
BS  = 50000
CUTOFF = "2010-01-01"

def main():
  args = read_args(ARG, setup_logger("read_args()"))
  tickers = TickerLoader(CON).get_tickers(getattr(args, 'sector'))
  reader = DataReader(CON, setup_logger("DataReader"), bs=BS)
  res = []
  for row in tickers.iter_rows():
    ld = reader.get_latest_date(row[0])
    if ld:
      sd = datetime.strptime(ld, "%Y-%m-%d").date() + timedelta(days=1)
      ed = datetime.now().date()
    else:
      sd = datetime.strptime(CUTOFF, "%Y-%m-%d").date()
      ed = datetime.now().date()
    data = reader.load_data(row[1].upper(), sd, ed)
    data = add_columns(data, row[0])
    res.append(data)
  # Get latest id from daily_price and update resulting table id prior to
  # writing new data to daily price table.
  latest_id = reader.get_latest_id("SELECT MAX(id) FROM daily_price;")
  res = pl.concat(res, how="vertical")
  fills = np.arange(latest_id + 1, latest_id + 1 + res.height)
  res = res.with_columns(pl.Series("id", fills))
  writer = DataWriter(CON, setup_logger("DataWriter"), bs=BS)
  writer.write_data(res)

if __name__ == "__main__":
  start_time = time.time()
  main()
  end_time = time.time()
  print(f"Execution time: {(end_time - start_time):.2f} seconds...")

