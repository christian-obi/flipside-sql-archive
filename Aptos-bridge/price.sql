SELECT
  date_trunc('day', hour) as date,
  avg(price) as price
FROM
  aptos.price.ez_prices_hourly
where
  symbol = 'APT'
  and date >= '2024-01-01'
group by
  1
order by
  1 desc
