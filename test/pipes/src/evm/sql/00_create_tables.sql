CREATE TABLE IF NOT EXISTS swaps_raw
(
    timestamp           DateTime CODEC (DoubleDelta, ZSTD),
    amount_a            Float64,
    amount_b            Float64,
    token_a             String,
    token_b             String,
    account             String,
    block_number        UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_index   UInt16,
    log_index           UInt16,
    sign                Int8
) ENGINE = CollapsingMergeTree(sign)
      PARTITION BY toYYYYMM(timestamp) -- DATA WILL BE SPLIT BY MONTH
      ORDER BY (timestamp, block_number, transaction_index, log_index);
