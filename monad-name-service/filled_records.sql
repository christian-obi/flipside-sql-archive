--count of people who filled records
WITH raw AS (
  SELECT
    block_number,
    tx_hash,
    tx_position,
    trace_index,
    type,
    LEFT(input, 10) AS function_sig,
    REGEXP_SUBSTR_ALL(SUBSTR(input, 11), '.{64}') AS part,
    part [0] :: STRING AS identifier_bytes,
    livequery.utils.udf_hex_to_int(part [1]) / 32 AS offset_a,
    livequery.utils.udf_hex_to_int(part [offset_a]) AS array_count_a,
    (offset_a + 1) AS offset_a_lower,
    (offset_a + array_count_a) AS offset_a_higher,
    input,
    output,
    ROW_NUMBER() OVER (
      PARTITION BY tx_hash
      ORDER BY
        tx_hash ASC
    ) AS rn
  FROM
    monad.testnet.fact_traces
  WHERE
    type = 'DELEGATECALL'
    AND from_address = LOWER('0x3019BF1dfB84E5b46Ca9D0eEC37dE08a59A41308')
    AND to_address = LOWER('0xfc13DCB5E3E97a21DE6d3ea07Ebcd72F9b6e75fb')
    AND function_sig = '0x0f244045'
),
position_a AS (
  SELECT
    block_number,
    tx_position,
    tx_hash,
    part,
    input,
    identifier_bytes,
    trace_index,
    index,
    offset_a_lower,
    livequery.utils.udf_hex_to_int(value) / 32 AS offsets_b,
    ROW_NUMBER() OVER (
      PARTITION BY tx_hash,
      trace_index
      ORDER BY
        index ASC
    ) - 1 AS intra_index
  FROM
    raw,
    LATERAL FLATTEN (input = > part)
  WHERE
    index BETWEEN offset_a_lower
    AND offset_a_higher
),
decoding AS (
  SELECT
    *,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b]) / 32 AS offsets_c,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b + offsets_c]) * 2 AS category_length,
    livequery.utils.udf_hex_to_string(
      SUBSTR(
        part [offset_a_lower + offsets_b + offsets_c + 1] :: STRING,
        1,
        category_length
      )
    ) AS category_name,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b + offsets_c + 2]) * 2 AS value_length,
    CEIL(value_length / 64, 0) AS value_max_length,
    (offset_a_lower + offsets_b + offsets_c + 2) AS category_offset,
    (
      (
        (offset_a_lower + offsets_b + offsets_c + 3) * 64
      ) + 11
    ) AS value_position,
    livequery.utils.udf_hex_to_string(
      SUBSTR(
        input,
        (
          (
            (offset_a_lower + offsets_b + offsets_c + 3) * 64
          ) + 11
        ),
        value_length
      )
    ) AS category_value
  FROM
    position_a
)
SELECT
  category_name,
  -- COUNT(*) AS total_count,
  COUNT(category_value) AS non_null_count
FROM
  decoding
GROUP BY
  category_name
