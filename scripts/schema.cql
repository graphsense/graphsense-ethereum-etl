CREATE KEYSPACE IF NOT EXISTS eth_raw
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE eth_raw;

CREATE TABLE block (
    block_id_group int,
    block_id int,
    block_hash blob,
    parent_hash blob,
    nonce blob,
    sha3_uncles blob,
    logs_bloom blob,
    transactions_root blob,
    state_root blob,
    receipts_root blob,
    miner blob,
    difficulty varint,
    total_difficulty varint,
    size int,
    extra_data blob,
    gas_limit int,
    gas_used int,
    timestamp int,
    transaction_count smallint,
    PRIMARY KEY (block_id_group, block_id)
);

CREATE TABLE transaction (
    tx_hash_prefix text,
    tx_hash blob,
    nonce int,
    block_hash blob,
    block_id int,
    transaction_index smallint,
    from_address blob,
    to_address blob,
    value varint, // in wei, 1e18 wei = 1 ether
    gas int,
    gas_price bigint,
    input blob,
    block_timestamp int,
    max_fee_per_gas bigint,
    max_priority_fee_per_gas bigint,
    transaction_type bigint,
    receipt_cumulative_gas_used bigint,
    receipt_gas_used bigint,
    receipt_contract_address blob,
    receipt_root blob,
    receipt_status bigint,
    receipt_effective_gas_price bigint,
    PRIMARY KEY (tx_hash_prefix, tx_hash)
);

CREATE TABLE trace (
    block_id_group int,
    block_id int,
    tx_hash blob,
    transaction_index smallint,
    from_address blob,
    to_address blob,
    value varint,
    input blob,
    output blob,
    trace_type text,
    call_type text,
    reward_type text,
    gas int,
    gas_used bigint,
    subtraces int,
    trace_address text,
    error text,
    status smallint,
    trace_id text,
    PRIMARY KEY (block_id_group, block_id, trace_id)
);

CREATE TABLE receipt (
    transaction_hash_prefix text,
    transaction_hash blob,
    transaction_index smallint,
    block_hash blob,
    block_id int,
    cumulative_gas_used bigint,
    gas_used bigint,
    contract_address blob,
    root blob, // 32 bytes of post-transaction stateroot - pre Byzantium, before block 4_370_000
    status smallint, // either 1 (success) or 0 (failure) - post Byzantium
    PRIMARY KEY (transaction_hash_prefix, transaction_hash)
);

CREATE TABLE genesis_transfer (
    address_id int,
    address blob PRIMARY KEY,
    value varint
);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    fiat_values map<text, float>
);

CREATE TABLE configuration (
    id text PRIMARY KEY,
    block_bucket_size int,
    tx_prefix_length int
);
