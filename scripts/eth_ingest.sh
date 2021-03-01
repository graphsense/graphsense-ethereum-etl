#!/bin/bash

# settings
ETH_ETL=/usr/local/bin/ethereumetl
DS_BULK=/usr/local/bin/dsbulk
CASSANDRA_HOST=
CASSANDRA_KEYSPACE=eth_raw
START_BLOCK=0
END_BLOCK=
PROVIDER_URI=file:///var/data/geth/geth.ipc

# ingest

## blocks
$ETH_ETL export_blocks_and_transactions \
  --start-block $START_BLOCK \
  --end-block $END_BLOCK \
  --blocks-output - \
  --provider-uri $PROVIDER_URI | \
  $DS_BULK load -c csv -header true \
    -h $CASSANDRA_HOST -k $CASSANDRA_KEYSPACE -t block

## transactions
$ETH_ETL export_blocks_and_transactions \
  --start-block $START_BLOCK \
  --end-block $END_BLOCK \
  --transactions-output - \
  --provider-uri $PROVIDER_URI | \
  $DS_BULK load -c csv -header true \
    -h $CASSANDRA_HOST -k $CASSANDRA_KEYSPACE -t transaction \
    --connector.csv.maxCharsPerColumn=-1
