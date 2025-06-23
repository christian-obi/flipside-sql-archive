WITH logs AS (
  SELECT 
    *,
    REGEXP_SUBSTR_ALL(SUBSTR(DATA, 3), '.{64}') AS segmented,
    contract_address AS nft_address
  FROM monad.testnet.fact_event_logs
  WHERE block_timestamp::date >= '2025-02-10'
    AND topics[0]::string IN (
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', -- 721 
      '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', -- transferSingle 
      '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb', -- transferBatch
      '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'
    ) 
),

erc721_transfers AS (
  SELECT 
    block_timestamp, 
    tx_hash,
    event_index, 
    topics, 
    data,
    segmented,
    nft_address,
    '0x' || SUBSTR(topics[1]::string, 27, 40) AS from_address,
    '0x' || SUBSTR(topics[2]::string, 27, 40) AS to_address,
    utils.udf_hex_to_int(topics[3]::string)::string AS tokenid,
    1 AS quantity,
    1 AS intra_grouping
  FROM logs
  WHERE topics[0]::string = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),

erc1155_transfersingle AS (
  SELECT 
    block_timestamp, 
    tx_hash,
    event_index, 
    topics, 
    data,
    segmented,
    nft_address,
    '0x' || SUBSTR(topics[2]::string, 27, 40) AS from_address,
    '0x' || SUBSTR(topics[3]::string, 27, 40) AS to_address,
    utils.udf_hex_to_int(segmented[0]::string)::string AS tokenid,
    utils.udf_hex_to_int(segmented[1]::string)::string AS quantity,
    1 AS intra_grouping
  FROM logs 
  WHERE topics[0]::string = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
),

erc1155_batch_raw AS (
  SELECT 
    block_timestamp, 
    tx_hash,
    event_index, 
    topics, 
    data,
    segmented,
    nft_address,
    '0x' || SUBSTR(topics[2]::string, 27, 40) AS from_address,
    '0x' || SUBSTR(topics[3]::string, 27, 40) AS to_address,
    utils.udf_hex_to_int(segmented[0]::string) / 32 AS tokenid_offset,
    utils.udf_hex_to_int(segmented[1]::string) / 32 AS quantity_offset,
    utils.udf_hex_to_int(segmented[tokenid_offset]::string) AS tokenid_array_objects,
    utils.udf_hex_to_int(segmented[quantity_offset]::string) AS quantity_array_objects
  FROM logs 
  WHERE topics[0]::string = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
),

tokenid_flatten AS (
  SELECT 
    tx_hash, 
    event_index, 
    utils.udf_hex_to_int(value)::string AS tokenid,
    ROW_NUMBER() OVER (PARTITION BY tx_hash, event_index ORDER BY index ASC) AS intra_grouping 
  FROM erc1155_batch_raw, LATERAL FLATTEN(input => segmented)
  WHERE index BETWEEN (tokenid_offset + 1) AND (tokenid_offset + tokenid_array_objects)
),

quantity_flatten AS (
  SELECT 
    tx_hash, 
    event_index, 
    utils.udf_hex_to_int(value)::string AS quantity,
    ROW_NUMBER() OVER (PARTITION BY tx_hash, event_index ORDER BY index ASC) AS intra_grouping 
  FROM erc1155_batch_raw, LATERAL FLATTEN(input => segmented)
  WHERE index BETWEEN (quantity_offset + 1) AND (quantity_offset + quantity_array_objects)
),

flatten_final AS (
  SELECT 
    tx_hash, 
    event_index, 
    intra_grouping,
    tokenid,
    quantity
  FROM tokenid_flatten
  INNER JOIN quantity_flatten USING (tx_hash, event_index, intra_grouping) 
),

erc1155_batch_final AS (
  SELECT 
    block_timestamp, 
    tx_hash,
    event_index, 
    topics, 
    data,
    segmented,
    nft_address,
    from_address,
    to_address,
    tokenid,
    quantity,
    intra_grouping
  FROM erc1155_batch_raw
  INNER JOIN flatten_final USING (tx_hash, event_index)
),

nft_transfers AS (
  SELECT * FROM erc721_transfers
  UNION ALL
  SELECT * FROM erc1155_transfersingle
  UNION ALL 
  SELECT * FROM erc1155_batch_final 
),

swap_table AS (
  SELECT
    block_timestamp,
    tx_hash,
    origin_from_address AS user_address,
    regexp_substr_all(substr(input, 11), '.{64}') as segmented_input,
    regexp_substr_all(substr(output, 3), '.{64}') as segmented_output,
    livequery.utils.udf_hex_to_int(segmented_output [2]) as amount_in,
    livequery.utils.udf_hex_to_int(segmented_output [3]) as amount_out,
    '0x' || substr(segmented_input [5], 25) as token_in

  FROM
    monad.testnet.fact_traces
  WHERE
    to_address IN (
      '0xca810d095e90daae6e867c19df6d9a8c56db2c89',
      '0xdf0a565f332278ff2d0c50876da3a6701cbc6123',
      '0xe0a11266ff9eb6182d88a0a29523b39423a6a5e7'
    )
    AND trace_address = 'ORIGIN'
    AND tx_succeeded = 'TRUE'
    AND block_timestamp > '2025-02-15'
    AND LEFT(input, 10) IN (
      '0x8803dbee',
      '0xb6f9de95',
      '0x791ac947',
      '0x18cbafe5',
      '0xfb3bdb41',
      '0x7ff36ab5',
      '0x4a25d94a',
      '0x5c11d795',
      '0x38ed1739'
    )
),
filtered_swap_table as (
select
*,
amount_in / pow(10, 18) as amount_in_precise,
amount_out / pow(10,18) as amount_out_precise
from swap_table
where amount_in_precise > 10
and token_in not ilike '0x0000000000000000000000%'
and token_in ='0x760afe86e5de5fa0ee542fc7b7b713e1c5425701'
and block_timestamp >= DATEADD(MONTH, -3, CURRENT_DATE)
-- or amount_out_precise >1
),

swappers AS (
  SELECT DISTINCT user_address 
  FROM filtered_swap_table
),

nft_collections AS (
  SELECT 
    'blocknads' AS collection_name, '0x6ed438b2a8eff227e7e54b5324926941b140eea0' AS nft_address, '2025-04-14' AS min_date
  UNION ALL SELECT 'skrumpet', '0xe8f0635591190fb626f9d13c49b60626561ed145', '2025-02-18'
  UNION ALL SELECT 'beannads', '0xb03b60818fd7f391e2e02c2c41a61cff86e4f3f5', '2025-03-08'
  UNION ALL SELECT 'r3tards', '0xed52e0d80f4e7b295df5e622b55eff22d262f6ed', '2025-03-02'
  UNION ALL SELECT 'purple_fren', '0xc5c9425d733b9f769593bd2814b6301916f91271', '2025-02-27'
  UNION ALL SELECT 'La_mouch_1st', '0x800f8cacc990dda9f4b3f1386c84983ffb65ce94', '2025-02-14'
  UNION ALL SELECT 'La_mouch_2nd', '0x5a21b0f4a4f9b54e16282b6ed5ad014b3c77186f', '2025-03-06'
  UNION ALL SELECT 'scroll_of_coronation', '0x4fcf36ac3d46ef5c3f91b8e3714c9bfdb46d63a3', '2025-02-21'
  UNION ALL SELECT 'thedaks', '0x78ed9a576519024357ab06d9834266a04c9634b7', '2025-03-14'
  UNION ALL SELECT 'Chewy', '0x88bbcba96a52f310497774e7fd5ebadf0ece21fb', '2025-03-17'
  UNION ALL SELECT 'Monshape_Hopium', '0x69f2688abe5dcde0e2413f77b80efcc16361a56e', '2025-03-04'
  UNION ALL SELECT 'The10kSquad', '0x3a9454c1b4c84d1861bb1209a647c834d137b442', '2025-03-08'
  UNION ALL SELECT 'Yaiko_nads', '0x78a7c5dae2999e90f705def373cc0118d6f49378', '2025-05-14'
),

latest_owners AS (
  SELECT
    nc.collection_name,
    nt.to_address AS owner,
    nt.tokenid
  FROM nft_transfers nt
  JOIN nft_collections nc ON nt.nft_address = nc.nft_address
  WHERE nt.block_timestamp > nc.min_date
  QUALIFY ROW_NUMBER() OVER(PARTITION BY nc.collection_name, nt.tokenid ORDER BY nt.block_timestamp DESC) = 1
),

nft_swappers AS (
  SELECT 
    lo.collection_name,
    lo.owner
  FROM latest_owners lo
  INNER JOIN swappers s ON lo.owner = s.user_address
),

collection_stats AS (
  SELECT
    lo.collection_name AS nft_collection,
    COUNT(DISTINCT lo.owner) AS total_nft_owners,
    COUNT(DISTINCT CASE WHEN ns.owner IS NOT NULL THEN lo.owner END) AS nft_owners_that_swapped
  FROM latest_owners lo
  LEFT JOIN nft_swappers ns ON lo.collection_name = ns.collection_name AND lo.owner = ns.owner
  GROUP BY lo.collection_name
)

SELECT
  nft_collection,
  total_nft_owners,
  nft_owners_that_swapped,
  ROUND(nft_owners_that_swapped * 100.0 / NULLIF(total_nft_owners, 0), 2) AS percent_nft_owners_that_swapped
FROM collection_stats
ORDER BY nft_collection

