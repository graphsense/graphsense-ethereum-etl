#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import urllib
from argparse import ArgumentParser
from datetime import datetime, time, timezone, date, timedelta
from subprocess import check_output
import os
import json

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from web3 import Web3


BLOCK_BUCKET_SIZE = int(1e5)  # as defined in ethereum-etl block_mapper.py
TX_PREFIX_LENGTH = 4  # as defined in ethereum-etl transaction_mapper.py


def ingest_receipts_for_blockrange(session, table, block_group, from_block, to_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    tempfile = f"{from_block}.txt"

    query = f"SELECT transaction_hash FROM {keyspace}.trace WHERE block_id_group = {block_group} AND block_id >= {from_block} AND block_id <= {to_block} "
    statement = SimpleStatement(query, fetch_size=100_000)

    tx_ids = []

    for row in session.execute(statement):
        if row.transaction_hash is not None:
            tx_ids.append("0x"+row.transaction_hash.hex())

    if len(tx_ids) > 0:
        with open(tempfile, mode='w', encoding='utf-8') as tx_hashes_file:
            tx_hashes_file.write('\n'.join(tx_ids))

        etl_cmd = f"{etl} export_receipts_and_logs -p {provider_uri} -t {tempfile}  --receipts-output -"
        dsbulk_cmd = f"{dsbulk} load -logDir {logdir} -c csv -header true -h '{cassandra_hosts}' -k {keyspace} -t {table} --connector.csv.maxCharsPerColumn=-1"

        piped = f"{etl_cmd} | {dsbulk_cmd}"

        check_output(piped, shell=True)

        os.remove(tempfile)


def latest_block_ingested(nodes, keyspace):
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)

    result = session.execute(
        f"SELECT block_id_group FROM {keyspace}.block PER PARTITION LIMIT 1")
    groups = [row.block_group for row in result.current_rows]

    if len(groups) == 0:
        return 0

    latest_block_group = max(groups)

    result = session.execute(f"SELECT MAX(number) AS latest_block FROM {keyspace}.block WHERE block_id_group={latest_block_group}")
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


def check_table_order(s):
    if "receipt" in s and "transaction" in s and s.index("receipt") < s.index("transaction"):
        raise ValueError(f"transactions must be ingested before receipts, please adjust your argument line {s}")


def import_traces(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    etl_cmd = f"{etl} export_traces --start-block {start_block} --end-block {end_block} --output - --provider-uri '{provider_uri}'"
    dsbulk_cmd = f"{dsbulk} load -logDir {logdir} -c csv -header true -h '{cassandra_hosts}' -k {keyspace} -t {table} --connector.csv.maxCharsPerColumn=-1"
    piped = f"{etl_cmd} | {dsbulk_cmd}"

    check_output(piped, shell=True)


def import_receipts(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(keyspace)

    for block_number in range(start_block, end_block, BLOCK_BUCKET_SIZE):
        from_block = block_number
        to_block = min(end_block, from_block+BLOCK_BUCKET_SIZE-1)
        ingest_receipts_for_blockrange(session, table, int(from_block // BLOCK_BUCKET_SIZE), from_block, to_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir)


def import_blocks_and_transactions(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    output_string = f"{table}s-output"
    additional_arg = "--connector.csv.maxCharsPerColumn=-1" if table == "transaction" else ""

    etl_cmd = f"{etl} export_blocks_and_transactions --start-block {start_block} --end-block {end_block} --{output_string} - --provider-uri '{provider_uri}'"
    dsbulk_cmd = f"{dsbulk} load -logDir {logdir} -c csv -header true -h '{cassandra_hosts}' -k {keyspace} -t {table} {additional_arg}"
    piped = f"{etl_cmd} | {dsbulk_cmd}"

    check_output(piped, shell=True)


def import_data(tables_to_fill, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir):
    for i in tables_to_fill:
        table, start_block, end_block = i
        start_block, end_block = int(start_block), int(end_block)
        if table in ["block", "transaction"]:
            import_blocks_and_transactions(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir)
        if table == "trace":
            import_traces(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir)
        if table == "receipt":
            import_receipts(table, start_block, end_block, provider_uri, cassandra_hosts, keyspace, etl, dsbulk, logdir)


def import_genesis_transfers(session):
    insert = session.prepare("INSERT INTO genesis_transfer (address, value) VALUES (?, ?)")

    with urllib.request.urlopen("https://raw.githubusercontent.com/openethereum/openethereum/main/crates/ethcore/res/chainspec/foundation.json") as url:
        for (k, b) in json.loads(url.read().decode())["accounts"].items():
            if "balance" in b.keys():
                session.execute(insert, (bytearray.fromhex(k[2:]), int(b["balance"], 16)))


def main():
    ETH_ETL = "/usr/local/bin/ethereumetl"
    DS_BULK = "/usr/local/bin/dsbulk"

    parser = ArgumentParser(description='Ingest Ethereum blocks and transactions into Cassandra', epilog='GraphSense - http://graphsense.info')

    parser.add_argument('-d', '--db_nodes', dest='db_nodes', required=True, nargs='+', metavar="DB_NODE", help="list of Cassandra nodes")
    parser.add_argument('-k', '--keyspace', dest='keyspace', default="eth_raw", metavar="eth_raw", help='Cassandra keyspace to use')
    parser.add_argument('-p', '--provider_uri', dest='provider_uri', metavar='file:///var/data/geth/geth.ipc', default='file:///var/data/geth/geth.ipc', help='Ethereum client URI')
    parser.add_argument('-l', '--logs', dest='logdir', metavar='/var/data/ethereum-etl/logs/', default='/var/data/ethereum-etl/logs/', help='directory where all log files will be stored')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-t', '--table_specific', dest='table_specific', nargs='+', metavar="'block:1-10 transaction:5-10 trace:5-10 receipt:5-10'", help='ingest table block from block 1 until block 10, ..')
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
        tables_to_fill.append(("trace", start_block, end_block))
        tables_to_fill.append(("receipt", start_block, end_block))
    else:
        check_table_order(args.table_specific)
        for i in args.table_specific:
            table = i.split(":")[0]
            start_block, end_block = i.split(":")[1].split("-")
            tables_to_fill.append((table, start_block, end_block))

    print("*** Starting Ethereum ingest")
    print(f"    ingesting into Cassandra on {args.db_nodes}")

    import_data(tables_to_fill, args.provider_uri, args.db_nodes, args.keyspace, ETH_ETL, DS_BULK, args.logdir)

    # write configuration table
    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)
    cql_str = '''INSERT INTO configuration (id, block_bucket_size, tx_prefix_length) VALUES (%s, %s, %s)'''
    session.execute(cql_str, (args.keyspace, BLOCK_BUCKET_SIZE, TX_PREFIX_LENGTH))

    import_genesis_transfers(session)  # downloads genesis transfers from openethereum repo

    cluster.shutdown()


if __name__ == "__main__":
    main()
