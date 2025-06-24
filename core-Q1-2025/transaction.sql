SELECT 
date_trunc('day',block_timestamp) as date,
count(*) as total_transaction,
sum(total_transaction) over (order by date) as cumulative
FROM core.core.fact_transactions
WHERE date >= '2025-01-01'
GROUP by 1
order by 1 desc
