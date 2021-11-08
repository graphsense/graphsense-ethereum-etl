#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 CSV_DIR CASSANDRA_HOST CASSANDRA_KEYSPACE"
    exit 1
fi

DIR=$1
DB_HOST=$2
DB_KEYSPACE=$3
DSBULK="dsbulk"

find "$DIR" -name 'block_*.csv.gz' -type f -print | sort |
    while read -r filename; do
        echo "### $filename"
        zcat "$filename" |
            "$DSBULK" load -c csv -header true \
                -h "$DB_HOST" -k "$DB_KEYSPACE" -t block
    done

find "$DIR" -name 'tx_*.csv.gz' -type f -print | sort |
    while read -r filename; do
        echo "### $filename"
        zcat "$filename" |
            "$DSBULK" load -c csv -header true \
                -h "$DB_HOST" -k "$DB_KEYSPACE" -t transaction \
                --connector.csv.maxCharsPerColumn=-1
    done

find "$DIR" -name 'trace_*.csv.gz' -type f -print | sort |
    while read -r filename; do
        echo "### $filename"
        zcat "$filename" |
            "$DSBULK" load -c csv -header true \
                -h "$DB_HOST" -k "$DB_KEYSPACE" -t trace \
                --connector.csv.maxCharsPerColumn=-1
    done
