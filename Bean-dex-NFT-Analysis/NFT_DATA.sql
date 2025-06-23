--FORK from freemartian on Selling monad NFT on magic eden
-- WITH collections AS (
--   SELECT
--     value:address::string AS collection_address,
--     value:name::string AS collection_name
--   FROM (
--     SELECT livequery.live.udf_api(
--       'https://raw.githubusercontent.com/mehdimarjan/magiceden-on-monad/refs/heads/main/collections.json'
--     ) AS response
--   ), LATERAL FLATTEN(input => response:data)
-- )
-- ,
with transactions AS (
  -- ======= Single Sale =======
  SELECT
    el.block_timestamp,
    el.tx_hash,
    el.origin_from_address AS buyer,
    tr.value AS amount,
    COUNT(*) AS nft_count,
    'MON' AS token_paid,
    1 AS rank
  FROM
    monad.testnet.fact_event_logs el
    LEFT JOIN monad.testnet.fact_transactions tr ON el.tx_hash = tr.tx_hash
  WHERE
    el.origin_to_address = '0x0000000000000068f116a894984e2db1123eb395'
    AND el.contract_address <> '0x0000000000000068f116a894984e2db1123eb395'
    AND el.origin_function_signature = '0xe7acab24'
    AND el.tx_succeeded
  GROUP BY
    el.block_timestamp,
    el.tx_hash,
    el.origin_from_address,
    tr.value

  UNION
  ALL -- ======= Bulk Sale (Single Buyer) =======
  SELECT
    el.block_timestamp,
    el.tx_hash,
    el.origin_from_address AS buyer,
    tr.value AS amount,
    COUNT(*) AS nft_count,
    'MON' AS token_paid,
    1 AS rank
  FROM
    monad.testnet.fact_event_logs el
    LEFT JOIN monad.testnet.fact_transactions tr ON el.tx_hash = tr.tx_hash
  WHERE
    el.origin_to_address = '0x0000000000000068f116a894984e2db1123eb395'
    AND el.contract_address <> '0x0000000000000068f116a894984e2db1123eb395'
    AND el.origin_function_signature = '0x87201b41'
    AND el.tx_succeeded
  GROUP BY
    el.block_timestamp,
    el.tx_hash,
    el.origin_from_address,
    tr.value
  UNION
  ALL -- ======= WMON Transfer =======
  SELECT
    el.block_timestamp,
    el.tx_hash,
    el.origin_from_address AS buyer,
    utils.udf_hex_to_int(SUBSTR(el.data, 3)) :: int / POW(10, 18) AS amount,
    1 AS nft_count,
    'WMON' AS token_paid,
    RANK() OVER (
      PARTITION BY el.tx_hash
      ORDER BY
        el.event_index ASC
    ) AS rank
  FROM
    monad.testnet.fact_event_logs el
  WHERE
    el.tx_succeeded
    AND el.origin_function_signature = '0x74afcbe6'
    AND el.origin_to_address = '0x224ecb4eae96d31372d1090c3b0233c8310dbbab'
    AND el.topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND LENGTH(el.data) = 66 QUALIFY rank = 1
),
datas AS (
  SELECT
    t.block_timestamp,
    t.tx_hash,
    l.contract_address,
    case
      when l.contract_address = '0xe8f0635591190fb626f9d13c49b60626561ed145' then 'skrumpet'
      when l.contract_address = '0xb03b60818fd7f391e2e02c2c41a61cff86e4f3f5' then 'beannads'
      when l.contract_address = '0x26c86f2835c114571df2b6ce9ba52296cc0fa6bb' then 'lil_chogstar'
      when l.contract_address = '0xed52e0d80f4e7b295df5e622b55eff22d262f6ed' then 'r3tards'
      when l.contract_address = '0xc5c9425d733b9f769593bd2814b6301916f91271' then 'purple_frens'
      when l.contract_address = '0x66e40f67afd710386379a6bb24d00308f81c183f' then 'molandaks'
      when l.contract_address = '0xa568cabe34c8ca0d2a8671009ae0f6486a314425' then 'meowwnads'
      when l.contract_address = '0x66b655de495268eb4c7b70bf4ac1ab4094589f93' then 'overnads'
      when l.contract_address = '0x800f8cacc990dda9f4b3f1386c84983ffb65ce94' then 'La_mouch_1st_collection'
      when l.contract_address = '0x5a21b0f4a4f9b54e16282b6ed5ad014b3c77186f' then 'La_mouch_2nd_collection'
      when l.contract_address = '0x72b6f0b8018ed4153b4201a55bb902a0f152b5c7' then 'wolly_eggs'
      when l.contract_address = '0x869650871ebcc881faaf191b70f810abb66a1b5a' then 'owl_nads'
      when l.contract_address = '0x4fcf36ac3d46ef5c3f91b8e3714c9bfdb46d63a3' then 'scroll_of_coronation'
      when l.contract_address = '0x78ed9a576519024357ab06d9834266a04c9634b7' then 'thedaks'
      when l.contract_address = '0x0bbf4a69f5e20ddf2244add5fc412b9631a2cba1' then 'RealNadsClub'
      when l.contract_address = '0x88bbcba96a52f310497774e7fd5ebadf0ece21fb' then 'chewy'
      when l.contract_address = '0x69f2688abe5dcde0e2413f77b80efcc16361a56e' then 'monshape_hopium'
      when l.contract_address = '0xb0a663cf4853e67221fee43322fda402e21debfc' then 'llamao_genesis'
      when l.contract_address = '0x3a9454c1b4c84d1861bb1209a647c834d137b442' then 'the10squad'
      when l.contract_address = '0x6ed438b2a8eff227e7e54b5324926941b140eea0' then 'blocknads'
      else 'unknown'
    end as collections,
    -- c.collection_name AS collection,
    t.buyer,
    t.amount,
    t.token_paid,
    t.nft_count
  FROM
    monad.testnet.fact_event_logs l
    LEFT JOIN transactions t ON t.tx_hash = l.tx_hash 
-- LEFT JOIN collections c ON c.collection_address = l.contract_address
  WHERE
    l.tx_hash IN (
      SELECT
        tx_hash
      FROM
        transactions
    )
    AND l.contract_address NOT IN (
      '0x0000000000000068f116a894984e2db1123eb395',
      '0x760afe86e5de5fa0ee542fc7b7b713e1c5425701'
    )
  GROUP BY
    t.block_timestamp,
    t.tx_hash,
    l.contract_address,
    collections,
    t.buyer,
    t.amount,
    t.token_paid,
    t.nft_count
)
,
-- ========= getting buyers per collection =========
purple_frens_buyers_table as (
select
distinct buyer as purple_fren_buyers,
ROW_NUMBER() OVER ( order by purple_fren_buyers) AS rn

from
  datas
where
  contract_address = '0xc5c9425d733b9f769593bd2814b6301916f91271'
)
,
la_mouch_1st_collection_buyers_table as (
select
distinct buyer as la_mouch_1st_collection_buyers,
ROW_NUMBER() OVER (order by la_mouch_1st_collection_buyers) AS rn

from
  datas
where
  contract_address = '0x800f8cacc990dda9f4b3f1386c84983ffb65ce94'
)
,
chewy_buyers_table as (
select
distinct buyer as chewy_buyers,
ROW_NUMBER() OVER (order by chewy_buyers) AS rn

from
  datas
where
  contract_address = '0x88bbcba96a52f310497774e7fd5ebadf0ece21fb'
)
,
combined as (
select 
  pf.purple_fren_buyers,
  lc.la_mouch_1st_collection_buyers,
  c.chewy_buyers,
  from purple_frens_buyers_table pf
full outer join la_mouch_1st_collection_buyers_table lc on pf.rn = lc.rn
full outer join chewy_buyers_table  c on coalesce(pf.rn,lc.rn)= c.rn
)
select
*
from combined
