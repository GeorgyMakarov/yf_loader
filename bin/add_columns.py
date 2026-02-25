import polars as pl

def add_columns(data: pl.DataFrame, symbol_id: int) -> pl.DataFrame:
  n = len(data)
  df = pl.DataFrame({"id": pl.repeat(None, n, eager=True), 
                     "data_vendor_id": pl.repeat(1, n, eager=True), 
                     "symbol_id": pl.repeat(symbol_id, n, eager=True)})
  return pl.concat([df, data], how="horizontal")