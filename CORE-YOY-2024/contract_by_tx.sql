with contract_table as (
  SELECT
    address as contract_address,
    address_name as contract_name,
    label_type
  FROM
    core.core.dim_labels
  WHERE
    label_type IN ('nft', 'bridge', 'games', 'defi', 'dex', 'dapp')
),
SELECT
  *
FROM
  core.core.ez_decoded_event_logs
WHERE
  contract_address = '0x82ae9509c137256e02c0d0d6bffce52b343f9a69'
limit
  10
