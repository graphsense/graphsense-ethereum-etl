#!/bin/sh
echo "Ingesting from ${PROVIDER_URL} into Cassandra keyspace ${RAW_KEYSPACE} on ${CASSANDRA_HOST}"

python3 -u /usr/local/bin/eth_ingest.py \
    -d $CASSANDRA_HOST \
    -k $RAW_KEYSPACE \
    -p $PROVIDER_URL \
    -u yesterday