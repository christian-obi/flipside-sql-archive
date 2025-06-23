with base as (
  select
    block_timestamp,
    tx_hash,
    origin_from_address,
    left(input, 10) as funct_sig,
    input,
    value,
    output
  from
    monad.testnet.fact_traces
  where
    to_address in (
      '0xca810d095e90daae6e867c19df6d9a8c56db2c89',
      '0xdf0a565f332278ff2d0c50876da3a6701cbc6123',
      '0xe0a11266ff9eb6182d88a0a29523b39423a6a5e7'
    )
    and trace_address = 'ORIGIN'
    and tx_succeeded = 'TRUE'
    and block_timestamp > '2025-02-15'
),
swap_table as (
  select
    block_timestamp,
    tx_hash,
    origin_from_address as user_address,
    regexp_substr_all(substr(input, 11), '.{64}') as segmented_input,
    regexp_substr_all(substr(output, 3), '.{64}') as segmented_output,
    '0x' || substr(segmented_input [2], 25) as user,
    '0x' || substr(segmented_input [5], 25) as token_in,
    livequery.utils.udf_hex_to_int(segmented_output [2]) as amount_in,
    '0x' || substr(segmented_input [6], 25) as token_out,
    livequery.utils.udf_hex_to_int(segmented_output [3]) as amount_out,
  from
    base
  where
    funct_sig in (
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
token_symbol as (
  select
    block_timestamp,
    tx_hash,
    user_address,
    user,
    token_in,
    case
      when token_in = '0x70f893f65e3c1d7f82aad72f71615eb220b74d10' then 'JAI'
      when token_in = '0x020f16358a99c3839686014617de3d039d2cdeec' then 'PAGOL'
      when token_in = '0x77f77926c6596c78f285d230cd0dc8dc3540e3a6' then 'USDC'
      when token_in = '0x07aabd925866e8353407e67c1d157836f7ad923e' then 'sMON'
      when token_in = '0x62534e4bbd6d9ebac0ac99aeaa0aa48e56372df0' then 'USDC'
      when token_in = '0x0f0bdebf0f83cd1ee3974779bcb7315f9808c714' then 'DAK'
      when token_in = '0xa296f47e8ff895ed7a092b4a9498bb13c46ac768' then 'wWETH'
      when token_in = '0x878819cec0b61e78fa92fbb5bb258a92852fb3d1' then 'CHARLIE'
      when token_in = '0x6fe981dbd557f81ff66836af0932cba535cbc343' then 'LINK'
      when token_in = '0x6bb379a2056d1304e73012b99338f8f581ee2e18' then 'wBTC'
      when token_in = '0x646be04ae14385f9d483bbe6d90a9da4f4a0141f' then 'GREEN'
      when token_in = '0x88b8e2161dedc77ef4ab7585569d2415a1c1055d' then 'USDT'
      when token_in = '0xe0590015a873bf326bd645c3e1266d4db41c4e6b' then 'CHOG'
      when token_in = '0x43d614b1ba4ba469faeaa4557aeafdec039b8795' then 'MOCKB'
      when token_in = '0xe7b284fd1d6770ef880664e6a109145e21666c58' then 'azUSDT'
      when token_in = '0xcc5b42f9d6144dfdfb6fb3987a2a916af902f5f8' then 'JAI'
      when token_in = '0x37bba84a61a07f2a26e6a31ec3d1fb457987a381' then 'Khalie'
      when token_in = '0xa5e5caf9153c317dcdd003a4e314bc7f9f013b7a' then 'leMONade'
      when token_in = '0x268e4e24e0051ec27b3d27a95977e71ce6875a05' then 'BEAN'
      when token_in = '0x989d38aeed8408452f0273c7d4a17fef20878e62' then 'MUK'
      when token_in = '0x760afe86e5de5fa0ee542fc7b7b713e1c5425701' then 'wMON'
      when token_in = '0xf817257fed379853cde0fa4f97ab987181b1e5ea' then 'USDC'
      when token_in = '0x983a01a0808795b2f9cb93b5d2c82d1b79bdb641' then 'aUSD'
      when token_in = '0xae19bb7168f897d5dc144da1386d42b67c9d3ee2' then 'wMON'
      when token_in = '0xbb444821e159dd6401bb92fb18c2ac0a37113025' then 'fUSD'
      when token_in = '0xb5a30b0fdc5ea94a52fdc42e3e9760cb8449fb37' then 'wETH'
      when token_in = '0x5a8b923a25b97fe24dfc0acab6eea07254d11a78' then 'azWBTC'
      when token_in = '0x7fdf92a43c54171f9c278c67088ca43f2079d09b' then 'LUSD'
      when token_in = '0xfe140e1dce99be9f4f15d657cd9b7bf622270c50' then 'YAKI'
      when token_in = '0xbca648989410ea7f7cdecd0475201086a279e1c2' then 'USDC'
      when token_in = '0x39b1db3af4843618e7912084d4b7b05df65aacef' then 'SORAYA'
      when token_in = '0x5d876d73f4441d5f2438b1a3e2a51771b337f27a' then 'USDC'
      when token_in = '0x8a0cd3d017620c7d0f68f1fa74b2047dd986d0f3' then 'azUSDC'
      when token_in = '0xcf5a6076cfa32686c0df13abada2b40dec133f1d' then 'wBTC'
      when token_in = '0x6593f49ca8d3038ca002314c187b63dd348c2f94' then 'USDT'
      when token_in = '0x836047a99e11f376522b447bffb6e3495dd0637c' then 'ETH'
      when token_in = '0x673cd70fa883394a1f3deb3221937ceb7c2618d7' then 'USDC'
      when token_in = '0xb1c27ef5c51259d6517a01761894143e63969a55' then 'SPI'
      when token_in = '0x6bd0e6a144d4a702292a6a2be438e0a48b37f8c3' then 'HAHA'
      when token_in = '0xae1cbb015687e8d112013e154941567563c702fb' then 'aUSD'
      when token_in = '0xe60c974ed6d3b19b97ca3097ad85181a814c888c' then 'ANAGO'
      when token_in = '0x570ccb877b39fb7fb25eeb933abdbd810e510db4' then 'SHIB'
      when token_in = '0x247c7d1f7754885c2f009b25beb0f3e2cc29acec' then 'BOME'
      when token_in = '0x9a4f7399f4e9d154162ed8a6f9fb9e4ef0628270' then 'ANAD'
      when token_in = '0xa8037dac651bbb919a470cb5e2ad23d29dfe4c86' then 'YPY'
      when token_in = '0xb5481b57ff4e23ea7d2fda70f3137b16d0d99118' then 'CVE'
      when token_in = '0xa39e57996d2649ec4fdc104659b28e8d20265beb' then 'YAKI'
      when token_in = '0x9ebcd0ab11d930964f8ad66425758e65c53a7df1' then 'USDT'
      when token_in = '0xf5eb09ce44ba30283200e39bc42e0a3181f12f29' then 'WETH'
      when token_in = '0x5b54153100e40000f6821a7ea8101dc8f5186c2d' then 'sWETH'
      when token_in = '0xdfce5441cd392a16289c0b3b4a8d3349a40ff207' then 'USDC'
      when token_in = '0x24020ad2109a375cd0588a808c810827a6dbdb37' then 'THUGCAT'
      when token_in = '0xb996af59c449ce106d7ffdd11e638e4945873d9b' then 'LFG'
      when token_in = '0x6ba1e745d458c4393453ef15eae622cb1c520945' then 'EIXA'
      when token_in = '0xcf27f781841484d5cf7e155b44954d7224caf1dd' then 'BUSD'
      when token_in = '0xb41766fd99bdfc27477be59a86e75f4a4d10355b' then 'lhm'
      when token_in = '0xff62ca852a2704ca77b19ab2dd2cf4b49dd567fb' then 'MTU'
      when token_in = '0x2ff8a1480f12f9f491597f60442bdce926befe49' then 'seed'
      when token_in = '0x43000c521670f680c8b118d68d956846afccea27' then 'azPEPE'
      when token_in = '0x0abf3fc7f3aa27b2d42bd0aaf5e8a210642cdd99' then 'test'
      when token_in = '0x0bb0aa6aa3a3fd4f7a43fb5e3d90bf9e6b4ef799' then 'bmBTC'
      when token_in = '0x77dc30f399f684f39ff3a86bfb7dc01c634fecba' then 'NAD'
      when token_in = '0x11f7379c0262913ef5d27439a861ec7f73a5e70e' then 'COFFEE'
      when token_in = '0x0d12ea2aae3cbc118bd98b704fc204d28c6c962a' then 'MOFOR'
      when token_in = '0x0d611b3e62cf506f7b78208aceb09f8c0ab03666' then '0xa'
      when token_in = '0x36f2154c5fea784a99c015ab9a5cf6bb0247639a' then 'JAI'
      when token_in = '0x922f1de43b99dc836f3f769798787d8f58c77fd6' then '16e'
      when token_in = '0x5ccc781bb696d1d476f4046bda298fd092a2b2be' then 'MONX'
      when token_in = '0x6d4bc87e41a544ec75c0d79b1f42b4cc5ee34a89' then 'BONAD'
      when token_in = '0x0cbc18e73c2f52e04945716a2b72393a3eacd081' then 'ME'
      when token_in = '0xcc655f0c0d8602a65315c6f8a83c5e13907957f7' then 'MXT'
      when token_in = '0x3bb8a17d8edcaabc0e064500367efc89f90a6d83' then 'USDC-Zaros'
      when token_in = '0xb2082908eaa742b961fe48d6a26513bf1e15a628' then 'DOGE'
      when token_in = '0xa90b46348e6493268fc3680b161864899c12631e' then 'SALMONAD'
      when token_in = '0xf2cee9d6cf05c392d11372c9f719155f0fda5c38' then 'AMD'
      when token_in = '0x924f1bf31b19a7f9695f3fc6c69c2ba668ea4a0a' then 'USDC'
      when token_in = '0x3b428df09c3508d884c30266ac1577f099313cf6' then 'maamBTC'
      when token_in = '0xa19ab2a4d55fb64b10db6cb6ff565e4d6aeb7f41' then 'KING'
      when token_in = '0x199c0da6f291a897302300aaae4f20d139162916' then 'stMON'
      when token_in = '0xaeef2f6b429cb59c9b2d7bb2141ada993e8571c3' then 'gMON'
      when token_in = '0xb2f82d0f38dc453d596ad40a37799446cc89274a' then 'aprMON'
      when token_in = '0xe1d2439b75fb9746e7bc6cb777ae10aa7f7ef9c5' then 'sMON'
      when token_in = '0x3a98250f98dd388c211206983453837c8365bdc1' then 'shMON'
      else 'unknown'
    end as token_in_symbol,
    case
      when token_in = '0x70f893f65e3c1d7f82aad72f71615eb220b74d10' then '6'
      when token_in = '0x020f16358a99c3839686014617de3d039d2cdeec' then '18'
      when token_in = '0x77f77926c6596c78f285d230cd0dc8dc3540e3a6' then '6'
      when token_in = '0x07aabd925866e8353407e67c1d157836f7ad923e' then '18'
      when token_in = '0x62534e4bbd6d9ebac0ac99aeaa0aa48e56372df0' then '6'
      when token_in = '0x0f0bdebf0f83cd1ee3974779bcb7315f9808c714' then '18'
      when token_in = '0xa296f47e8ff895ed7a092b4a9498bb13c46ac768' then '18'
      when token_in = '0x878819cec0b61e78fa92fbb5bb258a92852fb3d1' then '18'
      when token_in = '0x6fe981dbd557f81ff66836af0932cba535cbc343' then '18'
      when token_in = '0x6bb379a2056d1304e73012b99338f8f581ee2e18' then '8'
      when token_in = '0x646be04ae14385f9d483bbe6d90a9da4f4a0141f' then '18'
      when token_in = '0x88b8e2161dedc77ef4ab7585569d2415a1c1055d' then '6'
      when token_in = '0xe0590015a873bf326bd645c3e1266d4db41c4e6b' then '18'
      when token_in = '0x43d614b1ba4ba469faeaa4557aeafdec039b8795' then '6'
      when token_in = '0xe7b284fd1d6770ef880664e6a109145e21666c58' then '6'
      when token_in = '0xcc5b42f9d6144dfdfb6fb3987a2a916af902f5f8' then '6'
      when token_in = '0x37bba84a61a07f2a26e6a31ec3d1fb457987a381' then '18'
      when token_in = '0xa5e5caf9153c317dcdd003a4e314bc7f9f013b7a' then '18'
      when token_in = '0x268e4e24e0051ec27b3d27a95977e71ce6875a05' then '18'
      when token_in = '0x989d38aeed8408452f0273c7d4a17fef20878e62' then '18'
      when token_in = '0x760afe86e5de5fa0ee542fc7b7b713e1c5425701' then '18'
      when token_in = '0xf817257fed379853cde0fa4f97ab987181b1e5ea' then '6'
      when token_in = '0x983a01a0808795b2f9cb93b5d2c82d1b79bdb641' then '18'
      when token_in = '0xae19bb7168f897d5dc144da1386d42b67c9d3ee2' then '18'
      when token_in = '0xbb444821e159dd6401bb92fb18c2ac0a37113025' then '18'
      when token_in = '0xb5a30b0fdc5ea94a52fdc42e3e9760cb8449fb37' then '18'
      when token_in = '0x5a8b923a25b97fe24dfc0acab6eea07254d11a78' then '8'
      when token_in = '0x7fdf92a43c54171f9c278c67088ca43f2079d09b' then '18'
      when token_in = '0xfe140e1dce99be9f4f15d657cd9b7bf622270c50' then '18'
      when token_in = '0xbca648989410ea7f7cdecd0475201086a279e1c2' then '18'
      when token_in = '0x39b1db3af4843618e7912084d4b7b05df65aacef' then '18'
      when token_in = '0x5d876d73f4441d5f2438b1a3e2a51771b337f27a' then '6'
      when token_in = '0x8a0cd3d017620c7d0f68f1fa74b2047dd986d0f3' then '6'
      when token_in = '0xcf5a6076cfa32686c0df13abada2b40dec133f1d' then '8'
      when token_in = '0x6593f49ca8d3038ca002314c187b63dd348c2f94' then '18'
      when token_in = '0x836047a99e11f376522b447bffb6e3495dd0637c' then '18'
      when token_in = '0x673cd70fa883394a1f3deb3221937ceb7c2618d7' then '6'
      when token_in = '0x3b428df09c3508d884c30266ac1577f099313cf6' then '8'
      when token_in = '0x924f1bf31b19a7f9695f3fc6c69c2ba668ea4a0a' then '6'
      when token_in = '0xb996af59c449ce106d7ffdd11e638e4945873d9b' then '2'
      when token_in = '0xdfce5441cd392a16289c0b3b4a8d3349a40ff207' then '6'
      else '18'
    end as decimal_in,
    amount_in,
    token_out,
    case
      when token_out = '0x70f893f65e3c1d7f82aad72f71615eb220b74d10' then 'JAI'
      when token_out = '0x020f16358a99c3839686014617de3d039d2cdeec' then 'PAGOL'
      when token_out = '0x77f77926c6596c78f285d230cd0dc8dc3540e3a6' then 'USDC'
      when token_out = '0x07aabd925866e8353407e67c1d157836f7ad923e' then 'sMON'
      when token_out = '0x62534e4bbd6d9ebac0ac99aeaa0aa48e56372df0' then 'USDC'
      when token_out = '0x0f0bdebf0f83cd1ee3974779bcb7315f9808c714' then 'DAK'
      when token_out = '0xa296f47e8ff895ed7a092b4a9498bb13c46ac768' then 'wWETH'
      when token_out = '0x878819cec0b61e78fa92fbb5bb258a92852fb3d1' then 'CHARLIE'
      when token_out = '0x6fe981dbd557f81ff66836af0932cba535cbc343' then 'LINK'
      when token_out = '0x6bb379a2056d1304e73012b99338f8f581ee2e18' then 'wBTC'
      when token_out = '0x646be04ae14385f9d483bbe6d90a9da4f4a0141f' then 'GREEN'
      when token_out = '0x88b8e2161dedc77ef4ab7585569d2415a1c1055d' then 'USDT'
      when token_out = '0xe0590015a873bf326bd645c3e1266d4db41c4e6b' then 'CHOG'
      when token_out = '0x43d614b1ba4ba469faeaa4557aeafdec039b8795' then 'MOCKB'
      when token_out = '0xe7b284fd1d6770ef880664e6a109145e21666c58' then 'azUSDT'
      when token_out = '0xcc5b42f9d6144dfdfb6fb3987a2a916af902f5f8' then 'JAI'
      when token_out = '0x37bba84a61a07f2a26e6a31ec3d1fb457987a381' then 'Khalie'
      when token_out = '0xa5e5caf9153c317dcdd003a4e314bc7f9f013b7a' then 'leMONade'
      when token_out = '0x268e4e24e0051ec27b3d27a95977e71ce6875a05' then 'BEAN'
      when token_out = '0x989d38aeed8408452f0273c7d4a17fef20878e62' then 'MUK'
      when token_out = '0x760afe86e5de5fa0ee542fc7b7b713e1c5425701' then 'wMON'
      when token_out = '0xf817257fed379853cde0fa4f97ab987181b1e5ea' then 'USDC'
      when token_out = '0x983a01a0808795b2f9cb93b5d2c82d1b79bdb641' then 'aUSD'
      when token_out = '0xae19bb7168f897d5dc144da1386d42b67c9d3ee2' then 'wMON'
      when token_out = '0xbb444821e159dd6401bb92fb18c2ac0a37113025' then 'fUSD'
      when token_out = '0xb5a30b0fdc5ea94a52fdc42e3e9760cb8449fb37' then 'wETH'
      when token_out = '0x5a8b923a25b97fe24dfc0acab6eea07254d11a78' then 'azWBTC'
      when token_out = '0x7fdf92a43c54171f9c278c67088ca43f2079d09b' then 'LUSD'
      when token_out = '0xfe140e1dce99be9f4f15d657cd9b7bf622270c50' then 'YAKI'
      when token_out = '0xbca648989410ea7f7cdecd0475201086a279e1c2' then 'USDC'
      when token_out = '0x39b1db3af4843618e7912084d4b7b05df65aacef' then 'SORAYA'
      when token_out = '0x5d876d73f4441d5f2438b1a3e2a51771b337f27a' then 'USDC'
      when token_out = '0x8a0cd3d017620c7d0f68f1fa74b2047dd986d0f3' then 'azUSDC'
      when token_out = '0xcf5a6076cfa32686c0df13abada2b40dec133f1d' then 'wBTC'
      when token_out = '0x6593f49ca8d3038ca002314c187b63dd348c2f94' then 'USDT'
      when token_out = '0x836047a99e11f376522b447bffb6e3495dd0637c' then 'ETH'
      when token_out = '0x673cd70fa883394a1f3deb3221937ceb7c2618d7' then 'USDC'
      when token_out = '0xb1c27ef5c51259d6517a01761894143e63969a55' then 'SPI'
      when token_out = '0x6bd0e6a144d4a702292a6a2be438e0a48b37f8c3' then 'HAHA'
      when token_out = '0xae1cbb015687e8d112013e154941567563c702fb' then 'aUSD'
      when token_out = '0xe60c974ed6d3b19b97ca3097ad85181a814c888c' then 'ANAGO'
      when token_out = '0x570ccb877b39fb7fb25eeb933abdbd810e510db4' then 'SHIB'
      when token_out = '0x247c7d1f7754885c2f009b25beb0f3e2cc29acec' then 'BOME'
      when token_out = '0x9a4f7399f4e9d154162ed8a6f9fb9e4ef0628270' then 'ANAD'
      when token_out = '0xa8037dac651bbb919a470cb5e2ad23d29dfe4c86' then 'YPY'
      when token_out = '0xb5481b57ff4e23ea7d2fda70f3137b16d0d99118' then 'CVE'
      when token_out = '0xa39e57996d2649ec4fdc104659b28e8d20265beb' then 'YAKI'
      when token_out = '0x9ebcd0ab11d930964f8ad66425758e65c53a7df1' then 'USDT'
      when token_out = '0xf5eb09ce44ba30283200e39bc42e0a3181f12f29' then 'WETH'
      when token_out = '0x5b54153100e40000f6821a7ea8101dc8f5186c2d' then 'sWETH'
      when token_out = '0xdfce5441cd392a16289c0b3b4a8d3349a40ff207' then 'USDC'
      when token_out = '0x24020ad2109a375cd0588a808c810827a6dbdb37' then 'THUGCAT'
      when token_out = '0xb996af59c449ce106d7ffdd11e638e4945873d9b' then 'LFG'
      when token_out = '0x6ba1e745d458c4393453ef15eae622cb1c520945' then 'EIXA'
      when token_out = '0xcf27f781841484d5cf7e155b44954d7224caf1dd' then 'BUSD'
      when token_out = '0xb41766fd99bdfc27477be59a86e75f4a4d10355b' then 'lhm'
      when token_out = '0xff62ca852a2704ca77b19ab2dd2cf4b49dd567fb' then 'MTU'
      when token_out = '0x2ff8a1480f12f9f491597f60442bdce926befe49' then 'seed'
      when token_out = '0x43000c521670f680c8b118d68d956846afccea27' then 'azPEPE'
      when token_out = '0x0abf3fc7f3aa27b2d42bd0aaf5e8a210642cdd99' then 'test'
      when token_out = '0x0bb0aa6aa3a3fd4f7a43fb5e3d90bf9e6b4ef799' then 'bmBTC'
      when token_out = '0x77dc30f399f684f39ff3a86bfb7dc01c634fecba' then 'NAD'
      when token_out = '0x11f7379c0262913ef5d27439a861ec7f73a5e70e' then 'COFFEE'
      when token_out = '0x0d12ea2aae3cbc118bd98b704fc204d28c6c962a' then 'MOFOR'
      when token_out = '0x0d611b3e62cf506f7b78208aceb09f8c0ab03666' then '0xa'
      when token_out = '0x36f2154c5fea784a99c015ab9a5cf6bb0247639a' then 'JAI'
      when token_out = '0x922f1de43b99dc836f3f769798787d8f58c77fd6' then '16e'
      when token_out = '0x5ccc781bb696d1d476f4046bda298fd092a2b2be' then 'MONX'
      when token_out = '0x6d4bc87e41a544ec75c0d79b1f42b4cc5ee34a89' then 'BONAD'
      when token_out = '0x0cbc18e73c2f52e04945716a2b72393a3eacd081' then 'ME'
      when token_out = '0xcc655f0c0d8602a65315c6f8a83c5e13907957f7' then 'MXT'
      when token_out = '0x3bb8a17d8edcaabc0e064500367efc89f90a6d83' then 'USDC-Zaros'
      when token_out = '0xb2082908eaa742b961fe48d6a26513bf1e15a628' then 'DOGE'
      when token_out = '0xa90b46348e6493268fc3680b161864899c12631e' then 'SALMONAD'
      when token_out = '0xf2cee9d6cf05c392d11372c9f719155f0fda5c38' then 'AMD'
      when token_out = '0x924f1bf31b19a7f9695f3fc6c69c2ba668ea4a0a' then 'USDC'
      when token_out = '0x3b428df09c3508d884c30266ac1577f099313cf6' then 'maamBTC'
      when token_out = '0xa19ab2a4d55fb64b10db6cb6ff565e4d6aeb7f41' then 'KING'
      when token_out = '0x199c0da6f291a897302300aaae4f20d139162916' then 'stMON'
      when token_out = '0xaeef2f6b429cb59c9b2d7bb2141ada993e8571c3' then 'gMON'
      when token_out = '0xb2f82d0f38dc453d596ad40a37799446cc89274a' then 'aprMON'
      when token_out = '0xe1d2439b75fb9746e7bc6cb777ae10aa7f7ef9c5' then 'sMON'
      when token_out = '0x3a98250f98dd388c211206983453837c8365bdc1' then 'shMON'
      else 'unknown'
    end as token_out_symbol,
    amount_out,
    case
      when token_out = '0x70f893f65e3c1d7f82aad72f71615eb220b74d10' then '6'
      when token_out = '0x020f16358a99c3839686014617de3d039d2cdeec' then '18'
      when token_out = '0x77f77926c6596c78f285d230cd0dc8dc3540e3a6' then '6'
      when token_out = '0x07aabd925866e8353407e67c1d157836f7ad923e' then '18'
      when token_out = '0x62534e4bbd6d9ebac0ac99aeaa0aa48e56372df0' then '6'
      when token_out = '0x0f0bdebf0f83cd1ee3974779bcb7315f9808c714' then '18'
      when token_out = '0xa296f47e8ff895ed7a092b4a9498bb13c46ac768' then '18'
      when token_out = '0x878819cec0b61e78fa92fbb5bb258a92852fb3d1' then '18'
      when token_out = '0x6fe981dbd557f81ff66836af0932cba535cbc343' then '18'
      when token_out = '0x6bb379a2056d1304e73012b99338f8f581ee2e18' then '8'
      when token_out = '0x646be04ae14385f9d483bbe6d90a9da4f4a0141f' then '18'
      when token_out = '0x88b8e2161dedc77ef4ab7585569d2415a1c1055d' then '6'
      when token_out = '0xe0590015a873bf326bd645c3e1266d4db41c4e6b' then '18'
      when token_out = '0x43d614b1ba4ba469faeaa4557aeafdec039b8795' then '6'
      when token_out = '0xe7b284fd1d6770ef880664e6a109145e21666c58' then '6'
      when token_out = '0xcc5b42f9d6144dfdfb6fb3987a2a916af902f5f8' then '6'
      when token_out = '0x37bba84a61a07f2a26e6a31ec3d1fb457987a381' then '18'
      when token_out = '0xa5e5caf9153c317dcdd003a4e314bc7f9f013b7a' then '18'
      when token_out = '0x268e4e24e0051ec27b3d27a95977e71ce6875a05' then '18'
      when token_out = '0x989d38aeed8408452f0273c7d4a17fef20878e62' then '18'
      when token_out = '0x760afe86e5de5fa0ee542fc7b7b713e1c5425701' then '18'
      when token_out = '0xf817257fed379853cde0fa4f97ab987181b1e5ea' then '6'
      when token_out = '0x983a01a0808795b2f9cb93b5d2c82d1b79bdb641' then '18'
      when token_out = '0xae19bb7168f897d5dc144da1386d42b67c9d3ee2' then '18'
      when token_out = '0xbb444821e159dd6401bb92fb18c2ac0a37113025' then '18'
      when token_out = '0xb5a30b0fdc5ea94a52fdc42e3e9760cb8449fb37' then '18'
      when token_out = '0x5a8b923a25b97fe24dfc0acab6eea07254d11a78' then '8'
      when token_out = '0x7fdf92a43c54171f9c278c67088ca43f2079d09b' then '18'
      when token_out = '0xfe140e1dce99be9f4f15d657cd9b7bf622270c50' then '18'
      when token_out = '0xbca648989410ea7f7cdecd0475201086a279e1c2' then '18'
      when token_out = '0x39b1db3af4843618e7912084d4b7b05df65aacef' then '18'
      when token_out = '0x5d876d73f4441d5f2438b1a3e2a51771b337f27a' then '6'
      when token_out = '0x8a0cd3d017620c7d0f68f1fa74b2047dd986d0f3' then '6'
      when token_out = '0xcf5a6076cfa32686c0df13abada2b40dec133f1d' then '8'
      when token_out = '0x6593f49ca8d3038ca002314c187b63dd348c2f94' then '18'
      when token_out = '0x836047a99e11f376522b447bffb6e3495dd0637c' then '18'
      when token_out = '0x673cd70fa883394a1f3deb3221937ceb7c2618d7' then '6'
      when token_out = '0x3b428df09c3508d884c30266ac1577f099313cf6' then '8'
      when token_out = '0x924f1bf31b19a7f9695f3fc6c69c2ba668ea4a0a' then '6'
      when token_out = '0xb996af59c449ce106d7ffdd11e638e4945873d9b' then '2'
      when token_out = '0xdfce5441cd392a16289c0b3b4a8d3349a40ff207' then '6'
      else 18
    end as decimal_out,
  from
    swap_table
),
combined as (
  select
    st.block_timestamp,
    st.tx_hash,
    st.user_address,
    st.user,
    ts.token_in,
    token_in_symbol,
    ts.amount_in,
    decimal_in,
    ts.amount_in / pow(10, ts.decimal_in) as amount_in_precise,
    ts.token_out,
    token_out_symbol,
    ts.amount_out,
    decimal_out,
    ts.amount_out / pow(10, ts.decimal_out) as amount_out_precise
  from
    swap_table st
    join token_symbol ts on st.block_timestamp = ts.block_timestamp
    and st.tx_hash = ts.tx_hash
    and st.user_address = ts.user_address
),
cleaned_table as (
  select
    *
  from
    combined
  where
    token_in not ilike '0x0000000000000000000000%'
)
select
  *
from
  cleaned_table
