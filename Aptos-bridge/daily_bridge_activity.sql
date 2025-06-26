SELECT
  date_trunc('day', block_timestamp) as date,
  sum(
    case
      when direction = 'outbound' then - amount_in_usd
    end
  ) as bridge_out,
  sum(
    CASE
      when direction = 'inbound' then amount_in_usd
    end
  ) as bridge_in,
  count(tx_hash) as transaction,
  count(distinct sender) as user
FROM
  aptos.defi.ez_bridge_activity
WHERE
  block_timestamp >= '{{start_date}}'
GROUP BY
  1
order by
  1 DESC
