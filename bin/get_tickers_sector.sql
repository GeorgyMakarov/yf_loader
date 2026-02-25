SELECT
  id,
  ticker
FROM symbol
WHERE sector = '<placeholder>'
GROUP BY
  id,
  ticker;