with bridging_out as(
  SELECT
    day,
    case
      when tx_count = tx_count then - tx_count
    end as bridge_out,
  FROM
    $ query('00d30397-f664-4fe2-8948-10f10b712f03')
  order by
    day
),
bridging_in as(
  select
    day,
    tx_count as bridge_in
  from
    $ query('67e2d2b3-f8a9-47a3-96d8-879f781211f7')
  order by
    day
)
SELECT
  pt.day as date,
  pt.bridge_in as bridge_in,
  bo.bridge_out as bridge_out
FROM
  bridging_in pt
  JOIN bridging_out bo on pt.day = bo.day
WHERE
  date >= '2024-01-01'
ORDER by
  date DESC
