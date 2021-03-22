CREATE KEYSPACE IF NOT EXISTS eth_raw
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE eth_raw;

CREATE TABLE block (
    block_group int,
    number int,
    hash blob,
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
    PRIMARY KEY (block_group, number)
);

CREATE TABLE transaction (
    hash_prefix text,
    hash blob,
    nonce int,
    block_hash blob,
    block_number int,
    transaction_index smallint,
    from_address blob,
    to_address blob,
    value varint, // in wei, 1e18 wei = 1 ether
    gas int,
    gas_price bigint,
    input blob,
    block_timestamp int,
    PRIMARY KEY (hash_prefix, hash)
);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    eur float,
    usd float
);
