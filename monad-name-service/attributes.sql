with raw as (
  select
    block_number,
    tx_hash,
    tx_position,
    trace_index,
    type,
    left(input, 10) as function_sig,
    regexp_substr_all(SUBSTR(input, 11), '.{64}') AS part,
    part [0] :: string as identifier_bytes,
    livequery.utils.udf_hex_to_int(part [1]) / 32 as offset_a,
    -- 2 -- main way to telling how many attributes were changed 
    livequery.utils.udf_hex_to_int(part [offset_a]) as array_count_a,
    (offset_a + 1) as offset_a_lower,
    (offset_a + array_count_a) as offset_a_higher,
    input,
    output,
    row_number() over (
      partition by tx_hash
      order by
        tx_hash asc
    ) as rn
  from
    monad.testnet.fact_traces
  where
    1 = 1
    and type = 'DELEGATECALL'
    and from_address = lower('0x3019BF1dfB84E5b46Ca9D0eEC37dE08a59A41308')
    and to_address = lower('0xfc13DCB5E3E97a21DE6d3ea07Ebcd72F9b6e75fb')
    and function_sig = '0x0f244045'
),
position_a as (
  select
    block_number,
    tx_position,
    tx_hash,
    part,
    input,
    identifier_bytes,
    trace_index,
    index,
    offset_a_lower,
    livequery.utils.udf_hex_to_int(value) / 32 as offsets_b,
    row_number() over (
      partition by tx_hash,
      trace_index
      order by
        index asc
    ) - 1 as intra_index
  from
    raw,
    lateral flatten (input = > part)
  where
    index between offset_a_lower
    and offset_a_higher
),
decoding as (
  select
    *,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b]) / 32 as offsets_c,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b + offsets_c]) * 2 as category_length,
    livequery.utils.udf_hex_to_string(
      substr(
        part [offset_a_lower + offsets_b + offsets_c + 1] :: string,
        1,
        category_length
      )
    ) as category_name,
    livequery.utils.udf_hex_to_int(part [offset_a_lower + offsets_b + offsets_c + 2]) * 2 as value_length,
    ceil(value_length / 64, 0) as value_max_length,
    (offset_a_lower + offsets_b + offsets_c + 2),
    (
      (
        (offset_a_lower + offsets_b + offsets_c + 3) * 64
      ) + 11
    ) as value_position,
    livequery.utils.udf_hex_to_string(
      substr(
        input,
        (
          (
            (offset_a_lower + offsets_b + offsets_c + 3) * 64
          ) + 11
        ),
        value_length
      )
    ) as category_value
  from
    position_a
)
select
  block_number,
  tx_position,
  tx_hash,
  trace_index,
  identifier_bytes,
  object_agg(category_name :: variant, category_value :: variant) as attributes
from
  decoding
group by
  all
