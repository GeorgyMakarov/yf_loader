SELECT
  symbol_id,
  MAX(price_date) AS latest_date
FROM daily_price
WHERE symbol_id = <symbol_id>
GROUP BY symbol_id;
  