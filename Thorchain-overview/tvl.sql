SELECT
  date_trunc('week', day) as date,
  sum(TOTAL_VALUE_LOCKED_USD) as tvl
FROM
  thorchain.defi.fact_daily_tvl
where
  day > '2024-01-01'
group by
  1
order by
  1
