with raw as (
SELECT
block_timestamp,
tx_hash,
event_name,
decoded_log:amount::int / 1e18 as amount,
decoded_log:candidate::string as validator,
decoded_log:delegator::string as delegator
from core.core.ez_decoded_event_logs
where 
origin_to_address in ('0xee21ab613d30330823d35cf91a84ce964808b83f' , '0x04ea61c431f7934d51fed2acb2c5f942213f8967')
and event_name in ('delegatedCoin', 'undelegatedCoin')
and topics[0] in ('0x69e36aaf9558a3c30415c0a2bc6cb4c2d592c041a0718697bf69c2e7c7e0bdac', 
'0x888585afd9421c43b48dc50229aa045dd1048c03602b46c83ad2aa36be798d42')
),

prices as (
SELECT
hour,
price,
row_number() over(order by hour desc) as rn
from core.price.ez_prices_hourly
where is_native = 'true'
),

latest_price as (
SELECT
*
from prices where rn = 1
),

validators as (
SELECT
block_timestamp,
tx_hash,
event_name,
amount,
amount * price as amount_usd,
validator,
CASE
            WHEN validator = '0x651da43be21fdb85615a58350cc09d019c3f47c4' THEN 'OKXEarn'
            WHEN validator = '0xdbc83c2093def988fbe96993292c058ef7da0784' THEN 'Satoshi App'
            WHEN validator = '0x1c151923cf6c381c4af6c3071a2773b3cdbbf704' THEN 'Kiln'
            WHEN validator = '0x307f36ff0aff7000ebd4eea1e8e9bbbfa0e1134c' THEN 'Everstake'
            WHEN validator = '0xf6fdbc19a25dc91454cec19ef7714e8b67c4e0e6' THEN 'Animoca Brands'
            WHEN validator = '0x608988097efc97679e3e2f5820ea81ff7ab5c85a' THEN 'Bitget'
            WHEN validator = '0x2d058b58dcf4b0db11168c62d3109f6e02710b02' THEN 'M labs'
            WHEN validator = '0x2e50087fb834747606ed01ad67ad0f32129ab431' THEN 'Foundry'
            WHEN validator = '0x33c724450ab1d9c5e583fcdd74701a7019706024' THEN 'Valour'
            WHEN validator = '0xe2f8cefcdee51f48e3ce5c4deea3095c43369b36' THEN 'Infstones'
            WHEN validator = '0xa21cbd3caa4fe89bccd1d716c92ce4533e4d4733' THEN 'DAO Mining pool1'
            WHEN validator = '0x64db24a662e20bbdf72d1cc6e973dbb2d12897d5' THEN 'DAO Mining pool4'
            WHEN validator = '0xebbaf365b0d5fa072e2b2429db23696291f2c038' THEN 'Ardennes'
            WHEN validator = '0x42fdeae88682a965939fee9b7b2bd5b99694ff64' THEN 'DAO Mining pool3'
            WHEN validator = '0x2953559db5cc88ab20b1960faa9793803d070337' THEN 'DAO Mining pool2'
            WHEN validator = '0x58d8efc838d2de558eedeabce631c7dff92c947a' THEN 'DAO Validator6'
            WHEN validator = '0x5b9b30813264eaab2b70817a36c94733812e591c' THEN 'DAO Validator2'
            WHEN validator = '0x536de38d1db7c68636fc989e4d0daac51e4eb950' THEN 'Solv'
            WHEN validator = '0x7c706ca44a28fdd25761250961276bd61d5aa87b' THEN 'DAO Validator1'
            WHEN validator = '0xa898e2f126b642d6e401bdcb79979c691a8fd90d' THEN 'Wolfedge Labs'
            WHEN validator = '0x83ee5d8b74a1d9310f0c152d30c0772529efedff' THEN 'Houbi'
            WHEN validator = '0xc24fe962e6230841c5019e531e3c713ed30161b4' THEN 'Fireblocks'
            WHEN validator = '0xbfbbacbd59c3bd551d40729061dc4d21ccbea792' THEN 'UTXO'
            ELSE validator
        END AS validators_names,
delegator
from raw left join prices on date_trunc('hour', block_timestamp) = hour
)

SELECT
count(distinct tx_hash) as total_transactions,
count(distinct delegator) as total_delegators_involved,
count(distinct validator) as total_number_validators,
sum(case when event_name = 'delegatedCoin' then amount end) as total_amount_delegated,
sum(case when event_name = 'undelegatedCoin' then amount end) as total_amount_undelegated,
total_amount_delegated - total_amount_undelegated as net_amount_staked,
net_amount_staked * (select price from latest_price) as TVL
from validators
