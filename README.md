# A dockerised component to synchronize Ethereum ETL data to Apache Cassandra

## Prerequisites
### Apache Cassandra

Download and install [Apache Cassandra][apache-cassandra] >= 3.11
in `$CASSANDRA_HOME`.

Start Cassandra (in the foreground for development purposes):

    $CASSANDRA_HOME/bin/cassandra -f

Connect to Cassandra via CQL

    $CASSANDRA_HOME/bin/cqlsh

and test if it is running

    cqlsh> SELECT cluster_name, listen_address FROM system.local;

    cluster_name | listen_address
    --------------+----------------
    Test Cluster |      127.0.0.1

    (1 rows)



## Docker setup

Build docker image

```
docker build -t ethereum-etl .
```

This image includes the patched version of [Ethereum ETL][ethereum-etl] and the [Datastax bulk loader][dsbulk]. 

Start the container
```
./docker/start.sh ethereum-etl DATA_DIR
```

DATA_DIR is the directory on the host where data is persisted after the docker container is stopped.

The arguments are mapped to the following locations inside the docker container:

- `DATA_DIR`: `/var/data/ethereum-etl`


## Ingesting Ethereum blocks and transactions

Get a shell

```
docker exec -ti ethereum-etl /bin/bash
```

Starting on a freshly installed database, first create a keyspace

```
create_keyspace.py -d $CASSANDRA_HOST -k $KEYSPACE -s /opt/graphsense/schema.cql
```

Then start the ingest. If data exists from a previous ingest, the process will continue from the latest block.

```
eth_ingest.py -d $CASSANDRA_HOST -k $KEYSPACE -u yesterday
```

To ingest specific block ranges, for block and/or transaction table:

```
eth_ingest.py -d $CASSANDRA_HOST -k $KEYSPACE -p $PROVIDER_URI -t block:46147-46150 transaction:46127-46200 
```


[ethereum-etl]: https://github.com/graphsense/ethereum-etl
[dsbulk]: https://github.com/datastax/dsbulk
[apache-cassandra]: http://cassandra.apache.org/download
