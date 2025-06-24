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
  token_id,
  tx_hash,
  seller,
  buyer,
  max(value) as highest_sale_price
FROM
  coretoshi_sales_value
where
  value is not NULL
group by
  token_id,
  tx_hash,
  seller,
  buyer
ORDER by
  highest_sale_price DESC
limit
  1
