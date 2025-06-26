SELECT
  count(distinct tx_id) as tx_number,
  block_timestamp :: date as date,
  sum(tx_number) over (
    order by
      date
  ) as total_tx_number,
  count(
    CASE
      when tx_succeeded = 'TRUE' then 1
      else null
    end
  ) * 100 / count(*) as success_rate
FROM
  aleo.core.fact_transactions
GROUP BY
  2
ORDER BY
  2
