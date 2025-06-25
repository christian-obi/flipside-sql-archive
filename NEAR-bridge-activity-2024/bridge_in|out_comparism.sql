with bridge_in as (
  SELECT
    tx_date,
    sum(total_inbound_amount_usd) as bridge_in_usd,
  FROM
    $ query('6440246d-a184-432d-8a9b-e7194b8d0432')
  group by
    1
),
bridge_out as (
  SELECT
    tx_date,
    sum(outbound_amount_usd) as bridge_out_usd,
  FROM
    $ query('f9aa1cfc-625d-4265-ac47-e1846e5d6360')
  GROUP by
    1
)
SELECT
  bi.tx_date as date,
  case
    when bo.bridge_out_usd = bo.bridge_out_usd then - bo.bridge_out_usd
  end as bridge_out,
  --bo.bridge_out_usd ,
  bi.bridge_in_usd
FROM
  bridge_in bi
  JOIN bridge_out bo on bi.tx_date = bo.tx_date
ORDER BY
  1 DESC
