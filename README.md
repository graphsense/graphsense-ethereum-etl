# A dockerised component to synchronize Ethereum ETL data to Apache Cassandra

## Prerequisites
### Apache Cassandra

Download and install [Apache Cassandra][apache-cassandra] >= 3.11
in `$CASSANDRA_HOME`.

Connect to Cassandra via CQL

    $CASSANDRA_HOME/bin/cqlsh

and test if it is running

    cqlsh> SELECT cluster_name, listen_address FROM system.local;

    cluster_name | listen_address
    --------------+----------------
    Test Cluster |      127.0.0.1

    (1 rows)

## Docker setup

Build the docker image

    docker-compose build

This image includes the patched version of [Ethereum ETL][ethereum-etl] and
the [Datastax bulk loader][dsbulk].

## Ingesting Ethereum blocks and transactions

Start a shell

    docker-compose run graphsense-ethereum-etl /bin/bash

Starting on a freshly installed database, first create a keyspace

    create_keyspace.py -d $CASSANDRA_HOST -k $KEYSPACE -s /opt/graphsense/schema.cql

Then start the ingest. If data exists from a previous ingest, the process
will continue from the latest block.

    eth_ingest.py -d $CASSANDRA_HOST -k $KEYSPACE -u yesterday

To ingest specific block ranges, for block and/or transaction table:

    eth_ingest.py -d $CASSANDRA_HOST -k $KEYSPACE -p $PROVIDER_URI -t block:46147-46150 transaction:46127-46200

Provider URIs might be in the form of

```
PROVIDER_URI=http://127.0.0.1:8545
PROVIDER_URI=file:///home/gethuser/.ethereum/geth.ipc
```

## Exchange rates

For Ethereum the exchange rates are obtained through [CoinMarketCap][coinmarketcap]:

    ~/pandas-venv/bin/python3 /usr/local/bin/ingest_rates_coinmarketcap.py -d $CASSANDRA_HOST -k $KEYSPACE


For additional options, see `scripts/ingest_rates_coinmarketcap.py`.

[ethereum-etl]: https://github.com/graphsense/ethereum-etl
[dsbulk]: https://github.com/datastax/dsbulk
[apache-cassandra]: http://cassandra.apache.org/download
[coinmarketcap]: https://coinmarketcap.com
