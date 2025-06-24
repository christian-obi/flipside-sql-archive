with nft_table as(
  SELECT
    nft.block_timestamp :: date as tx_date,
    nft.tx_hash as transaction,
    nft.from_address as seller,
    nft.to_address as buyers,
    nft.contract_address as nft_contract,
    nft.token_id as token_id,
    nft.name as nft_name,
    nft.origin_function_signature as nft_signature,
    tr.value_precise as nft_price,
    tr.tx_fee_precise as tx_fee
  FROM
    core.nft.ez_nft_transfers nft
    JOIN core.core.fact_transactions tr on transaction = tr.tx_hash
    and nft_signature = tr.origin_function_signature
  ORDER by
    1 DESC
)
SELECT
  DISTINCT nft_contract as n_nft_contracts,
  nft_name,
  count(DISTINCT transaction) as nft_transactions,
FROM
  nft_table
where
  tx_date >= '2024-01-01'
group by
  1,
  2
order by
  3 DESC
limit
  15
