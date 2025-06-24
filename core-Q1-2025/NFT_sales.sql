with bridging_out as(
SELECT
day,
case when tx_count = tx_count then -tx_count end as bridge_out,
FROM $query('8dcce56d-6022-4be5-aa5b-437d176304e8')
order by day
),
bridging_in as(
select
day,
tx_count as bridge_in
from $query('04848f75-2b41-4aa2-9b16-5c51267e4b21')
order by day
)
SELECT
pt.day as date,
pt.bridge_in as bridge_in,
bo.bridge_out as bridge_out
FROM  bridging_in pt
JOIN  bridging_out bo
on pt.day= bo.day
WHERE date >= '2025-01-01'
ORDER by date DESC
