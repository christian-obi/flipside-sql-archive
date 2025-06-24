SELECT
  date_trunc({{period_type}}, HOUR) as date,
  avg(price) as price,
FROM
  core.price.ez_prices_hourly
where
  symbol like 'CORE'
  and date >= '2024-01-01'
GROUP by
  1
