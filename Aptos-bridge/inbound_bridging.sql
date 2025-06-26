SELECT
  --date_trunc('day', block_timestamp) as date,
  soruce_chain as source_chain,
  count(tx_hash) as no_trans,
  sum(
    case
      when direction = 'inbound' then amount_in_usd
    end
  ) as bridge_in
FROM
  aptos.defi.ez_bridge_activity
where
  block_timestamp >= '{{start_date}}'
  and soruce_chain != 'Aptos'
  and soruce_chain != 'aptos'
GROUP BY
  1
