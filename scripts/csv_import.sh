#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 CSV_DIR CASSANDRA_HOST CASSANDRA_KEYSPACE"
    exit 1
fi

DIR=$1
DB_HOST=$2
DB_KEYSPACE=$3

dsbulk load -c csv -h "$DB_HOST" -k "$DB_KEYSPACE" -t block -url "$DIR" \
    --connector.csv.fileNamePattern '**/block_*.csv.gz' \
    --connector.csv.compression gzip \
    --connector.csv.recursive true

dsbulk load -c csv -h "$DB_HOST" -k "$DB_KEYSPACE" -t transaction -url "$DIR" \
    --connector.csv.fileNamePattern '**/tx_*.csv.gz' \
    --connector.csv.compression gzip \
    --connector.csv.recursive true \
    --connector.csv.maxCharsPerColumn=-1

dsbulk load -c csv -h "$DB_HOST" -k "$DB_KEYSPACE" -t trace -url "$DIR" \
    --connector.csv.fileNamePattern '**/trace_*.csv.gz' \
    --connector.csv.compression gzip \
    --connector.csv.recursive true \
    --connector.csv.maxCharsPerColumn=-1
