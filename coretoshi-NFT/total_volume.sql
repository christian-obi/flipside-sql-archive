WITH coretoshi_address_transactions AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS date,
    from_address AS seller,
    to_address AS buyer,
    token_id,
    tx_hash,
    quantity
  FROM
    core.nft.ez_nft_transfers
  WHERE
    contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    AND is_mint = 'FALSE'
    AND origin_function_signature = '0x99818378'
    AND block_timestamp >= '2024-12-11'
),
price AS (
  SELECT
    DATE_TRUNC('day', hour) AS price_date,
    AVG(price) AS price
  FROM
    core.price.ez_prices_hourly
  WHERE
    symbol = 'CORE'
    AND is_native = 'TRUE'
  GROUP BY
    price_date
),
coretoshi_value_transaction AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS sales_date,
    tx_hash,
    value,
    tx_fee_precise AS tx_fee
  FROM
    core.core.fact_transactions
  WHERE
    tx_succeeded = 'TRUE'
),
coretoshi_sales_value AS (
  SELECT
    cat.date,
    cat.seller,
    cat.buyer,
    cat.token_id,
    cat.tx_hash,
    cat.quantity,
    cvt.value,
    cvt.tx_fee,
    p.price
  FROM
    coretoshi_address_transactions cat
    LEFT JOIN coretoshi_value_transaction cvt ON cat.tx_hash = cvt.tx_hash
    JOIN price p ON cat.date = p.price_date
)
SELECT
  date,
  COUNT(DISTINCT seller) AS number_of_sellers,
  COUNT(DISTINCT buyer) AS number_of_buyers,
  COUNT(DISTINCT tx_hash) AS number_of_transaction,
  sum(coalesce(tx_fee, 0)) AS tx_fee,
  sum(coalesce(tx_fee, 0) * price) as tx_fee_usd,
  SUM(COALESCE(value, 0)) AS sales_core,
  SUM(COALESCE(value, 0) * price) AS sales_usd,
  sum(sum(coalesce(tx_fee, 0))) over (
    ORDER BY
      date
  ) AS cumulative_tx_fee,
  sum(sum(coalesce(tx_fee, 0) * price)) over (
    ORDER BY
      date
  ) as cumulative_fee_usd,
  SUM(COUNT(DISTINCT tx_hash)) OVER (
    ORDER BY
      date
  ) AS cumulative_tx_volume,
  SUM(SUM(COALESCE(value, 0))) OVER (
    ORDER BY
      date
  ) AS cumulative_sales_volume_core,
  SUM(SUM(COALESCE(value, 0) * price)) OVER (
    ORDER BY
      date
  ) AS cumulative_sale_volume_usd
FROM
  coretoshi_sales_value
GROUP BY
  date
ORDER BY
  date DESC
