#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from datetime import datetime, time, timezone, date, timedelta
from subprocess import check_output

from cassandra.cluster import Cluster
from web3 import Web3


def latest_block_ingested(nodes, keyspace):
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)

    result = session.execute(
        f"SELECT block_group FROM {keyspace}.block PER PARTITION LIMIT 1")
    groups = [row.block_group for row in result.current_rows]

    if len(groups) == 0:
        return 0

    latest_block_group = max(groups)

    result = session.execute(f"SELECT MAX(number) AS latest_block FROM {keyspace}.block WHERE block_group={latest_block_group}")
    latest_block = result.current_rows[0].latest_block

    cluster.shutdown()
    return latest_block


def latest_block_available_before(until_date, provider_uri):
    until_unix = until_date.timestamp()

    if provider_uri.lower().startswith("file://"):
        provider = Web3.IPCProvider(provider_uri.replace("file://", "").replace("FILE://", ""))
    else:
        provider = Web3.HTTPProvider(provider_uri)

    w3 = Web3(provider)
    block = w3.eth.getBlock('latest')

    while block["timestamp"] > until_unix:
        block = w3.eth.getBlock(block["parentHash"])

    print("*** Determining latest block before {}:".format(until_date.strftime("%Y-%m-%d %H:%M:%S")))
    print("    block {} at {}".format(block["number"], datetime.utcfromtimestamp(block["timestamp"])))
    return block["number"]


def import_data(tables_to_fill, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    for i in tables_to_fill:
        table, start_block, end_block = i
        output_string = f"{table}s-output"
        additional_arg = "--connector.csv.maxCharsPerColumn=-1" if table == "transaction" else ""

        etl_cmd = f"{etl} export_blocks_and_transactions --start-block {start_block} --end-block {end_block} --{output_string} - --provider-uri '{provider_uri}'"
        dsbulk_cmd = f"{dsbulk} load -logDir {logdir} -c csv -header true -h '{cassandra_hosts}' -k {keyspace} -t {table} {additional_arg}"
        piped = f"{etl_cmd} | {dsbulk_cmd}"

        check_output(piped, shell=True)


def main():
    ETH_ETL = "/usr/local/bin/ethereumetl"
    DS_BULK = "/usr/local/bin/dsbulk"

    parser = ArgumentParser(description='Ingest Ethereum blocks and transactions into Cassandra', epilog='GraphSense - http://graphsense.info')

    parser.add_argument('-d', '--db_nodes', dest='db_nodes', required=True, nargs='+', metavar="DB_NODE", help="list of Cassandra nodes")
    parser.add_argument('-k', '--keyspace', dest='keyspace', default="eth_raw", metavar="eth_raw", help='Cassandra keyspace to use')
    parser.add_argument('-p', '--provider_uri', dest='provider_uri', metavar='file:///var/data/geth/geth.ipc', default='file:///var/data/geth/geth.ipc', help='Ethereum client URI')
    parser.add_argument('-l', '--logs', dest='logdir', metavar='/var/data/ethereum-etl/logs/', default='/var/data/ethereum-etl/logs/', help='directory where all log files will be stored')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-t', '--table_specific', dest='table_specific', nargs='+', metavar="'block:1-10,transaction:5-10'", help='ingest table block from block 1 until block 10, ..')
    group.add_argument('-u', '--update_existing', dest="until_date", metavar='[yesterday|yyyy-mm-dd]', help='update existing keyspace with new data until end of yesterday|yyyy-mm-dd')

    args = parser.parse_args()

    tables_to_fill = []
    if args.until_date:
        if args.until_date == "yesterday":
            until_date = datetime.today()
        else:
            until_date = date.fromisoformat(args.until_date) + timedelta(days=1)

        until_ts = datetime.combine(until_date, time.min).replace(tzinfo=timezone.utc)
        start_block = latest_block_ingested(args.db_nodes, args.keyspace)
        end_block = latest_block_available_before(until_ts, args.provider_uri)

        print(f"*** Latest block ingested: {start_block}")
        print(f"*** Latest block available: {end_block}")
        tables_to_fill.append(("block", start_block, end_block))
        tables_to_fill.append(("transaction", start_block, end_block))
    else:
        for i in args.table_specific:
            table = i.split(":")[0]
            start_block, end_block = i.split(":")[1].split("-")
            tables_to_fill.append((table, start_block, end_block))

    print("*** Starting Ethereum ingest")
    print(f"    ingesting into Cassandra on {args.db_nodes}")

    import_data(tables_to_fill, args.provider_uri, args.db_nodes, args.keyspace, ETH_ETL, DS_BULK, args.logdir)

    # write configuration table
    block_bucket_size = int(1e5)  # as defined in ethereum-etl block_mapper.py
    tx_prefix_length = 4  # as defined in ethereum-etl transaction_mapper.py
    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)
    cql_str = '''INSERT INTO configuration (id, block_bucket_size, tx_prefix_length) VALUES (%s, %s, %s)'''
    session.execute(cql_str, (args.keyspace, block_bucket_size, tx_prefix_length))
    cluster.shutdown()


if __name__ == "__main__":
    main()
