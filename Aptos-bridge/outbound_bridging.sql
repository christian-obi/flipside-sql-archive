SELECT
  --date_trunc('day', block_timestamp) as date,
  destination_chain as destination,
  count(tx_hash) as no_trans,
  sum(
    case
      when direction = 'outbound' then amount_in_usd
    end
  ) as bridge_out
FROM
  aptos.defi.ez_bridge_activity
where
  block_timestamp >= '{{start_date}}'
  and destination_chain != 'Aptos'
  and destination_chain != 'aptos'
GROUP BY
  1
