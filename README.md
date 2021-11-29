[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# A GraphSense component to synchronize Ethereum ETL data to Apache Cassandra

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

## Local setup

Create and activate a python environment for required dependencies
([ethereum-etl][ethereum-etl] and
[Python client driver for Apache Cassandra][python-cassandra]).

    python3 -m venv venv
    . venv/bin/activate

Install dependencies in local environment

    pip install -r requirements.txt

Starting on a freshly installed database, first create a keyspace

    create_keyspace.py -d $CASSANDRA_HOST -k $KEYSPACE -s /opt/graphsense/schema.cql

Then start the data ingest. If data exists from a previous ingest, the process
will continue from the latest block id found in the `block` table:

    eth_cassandra_streaming.py -d $CASSANDRA_HOST -k $KEYSPACE -w $PROVIDER_URI -p

To ingest specific block ranges use the `-s`/`--startblock` and
`-e`/`--end_block` options (see `eth_cassandra_streaming.py -h`).

Provider URIs might be in the form of

```
PROVIDER_URI=http://127.0.0.1:8545
PROVIDER_URI=file:///opt/openethereum/jsonrpc.ipc
```

Ethereum exchange rates are obtained through [CoinMarketCap][coinmarketcap].
See `scripts/ingest_rates_coinmarketcap.py`.


[ethereum-etl]: https://github.com/blockchain-etl/ethereum-etl
[apache-cassandra]: http://cassandra.apache.org/download
[python-cassandra]: https://github.com/datastax/python-driver
[coinmarketcap]: https://coinmarketcap.com
