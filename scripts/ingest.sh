#!/bin/sh
echo "Ingesting from ${PROVIDER_URI} into Cassandra keyspace ${RAW_KEYSPACE} on ${CASSANDRA_HOST}"

python3 -u /usr/local/bin/eth_cassandra_streaming.py \
    -d "$CASSANDRA_HOST" -k "$RAW_KEYSPACE" \
    -w file:///var/run/jsonrpc.ipc -p
