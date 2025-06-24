WITH raw AS (
  SELECT
    *
  FROM
    core.core.ez_decoded_event_logs
  WHERE
    contract_address IN (
      '0xf5fa1728babc3f8d2a617397fac2696c958c3409',
      '0x0000000000000000000000000000000000001007'
    )
    AND tx_succeeded
),
delegated_amount AS (
  SELECT
    '0x' || substr(topic_1 :: string, 27) AS validators,
    SUM(
      utils.udf_hex_to_int(substr(data, 3, 64)) / POW(10, 18)
    ) AS delegated_core_amount,
    COUNT(DISTINCT topic_2) AS num_delegator
  FROM
    raw
  WHERE
    topic_0 = LOWER(
      '0x69e36aaf9558a3c30415c0a2bc6cb4c2d592c041a0718697bf69c2e7c7e0bdac'
    ) -- delegated
  GROUP BY
    1
),
undelegate_amount AS (
  SELECT
    '0x' || substr(topic_1 :: string, 27) AS validator,
    SUM(
      utils.udf_hex_to_int(substr(data, 3, 64)) / POW(10, 18)
    ) AS undelegated_core_amount,
    COUNT(DISTINCT topic_2) AS num_undelegator
  FROM
    raw
  WHERE
    topic_0 = LOWER(
      '0x888585afd9421c43b48dc50229aa045dd1048c03602b46c83ad2aa36be798d42'
    ) -- undelegated
  GROUP BY
    1
),
totals AS (
  SELECT
    d.validators,
    d.delegated_core_amount,
    ud.undelegated_core_amount,
    d.num_delegator,
    (
      d.delegated_core_amount - ud.undelegated_core_amount
    ) AS current_delegated_core
  FROM
    delegated_amount d
    JOIN undelegate_amount ud ON d.validators = ud.validator
  ORDER BY
    current_delegated_core DESC
  LIMIT
    27
), val_names AS (
  SELECT
    validators,
    CASE
      WHEN validators = '0x651da43be21fdb85615a58350cc09d019c3f47c4' THEN 'OKXEarn'
      WHEN validators = '0xdbc83c2093def988fbe96993292c058ef7da0784' THEN 'Satoshi App'
      WHEN validators = '0x1c151923cf6c381c4af6c3071a2773b3cdbbf704' THEN 'Kiln'
      WHEN validators = '0x307f36ff0aff7000ebd4eea1e8e9bbbfa0e1134c' THEN 'Everstake'
      WHEN validators = '0xf6fdbc19a25dc91454cec19ef7714e8b67c4e0e6' THEN 'Animoca Brands'
      WHEN validators = '0x608988097efc97679e3e2f5820ea81ff7ab5c85a' THEN 'Bitget'
      WHEN validators = '0x2d058b58dcf4b0db11168c62d3109f6e02710b02' THEN 'M labs'
      WHEN validators = '0x2e50087fb834747606ed01ad67ad0f32129ab431' THEN 'Foundry'
      WHEN validators = '0x33c724450ab1d9c5e583fcdd74701a7019706024' THEN 'Valour'
      WHEN validators = '0xe2f8cefcdee51f48e3ce5c4deea3095c43369b36' THEN 'Infstones'
      WHEN validators = '0xa21cbd3caa4fe89bccd1d716c92ce4533e4d4733' THEN 'DAO Mining pool1'
      WHEN validators = '0x64db24a662e20bbdf72d1cc6e973dbb2d12897d5' THEN 'DAO Mining pool4'
      WHEN validators = '0xebbaf365b0d5fa072e2b2429db23696291f2c038' THEN 'Ardennes'
      WHEN validators = '0x42fdeae88682a965939fee9b7b2bd5b99694ff64' THEN 'DAO Mining pool3'
      WHEN validators = '0x2953559db5cc88ab20b1960faa9793803d070337' THEN 'DAO Mining pool2'
      WHEN validators = '0x58d8efc838d2de558eedeabce631c7dff92c947a' THEN 'DAO Validator6'
      WHEN validators = '0x5b9b30813264eaab2b70817a36c94733812e591c' THEN 'DAO Validator2'
      WHEN validators = '0x536de38d1db7c68636fc989e4d0daac51e4eb950' THEN 'Solv'
      WHEN validators = '0x7c706ca44a28fdd25761250961276bd61d5aa87b' THEN 'DAO Validator1'
      WHEN validators = '0xa898e2f126b642d6e401bdcb79979c691a8fd90d' THEN 'Wolfedge Labs'
      WHEN validators = '0x83ee5d8b74a1d9310f0c152d30c0772529efedff' THEN 'Houbi'
      WHEN validators = '0xc24fe962e6230841c5019e531e3c713ed30161b4' THEN 'Fireblocks'
      WHEN validators = '0xbfbbacbd59c3bd551d40729061dc4d21ccbea792' THEN 'UTXO'
      ELSE validators
    END AS validators_name
  FROM
    totals
)
SELECT
  t.validators,
  t.delegated_core_amount,
  t.undelegated_core_amount,
  '🟢' || t.num_delegator as delegators,
  '🟧' || t.current_delegated_core as current_delegated_core,
  vn.validators_name
FROM
  totals t
  LEFT JOIN val_names vn ON t.validators = vn.validators
ORDER by
  t.current_delegated_core DESC
