#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.enrich import enrich_transactions
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator \
    import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from web3 import Web3


class InMemoryItemExporter:
    def __init__(self, item_types):
        self.item_types = item_types
        self.items = {}

    def open(self):
        for item_type in self.item_types:
            self.items[item_type] = []

    def export_item(self, item):
        item_type = item.get('type', None)
        if item_type is None:
            raise ValueError(f'type key is not found in item {item}')
        #item.pop('type')
        self.items[item_type].append(item)

    def close(self):
        pass

    def get_items(self, item_type):
        return self.items[item_type]


class EthStreamerAdapter:
    def __init__(
            self,
            batch_web3_provider,
            batch_size=190,
            max_workers=5):
        self.batch_web3_provider = batch_web3_provider
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

    def export_blocks_and_transactions(
            self,
            start_block,
            end_block,
            export_blocks=True,
            export_transactions=True):
        blocks_and_transactions_item_exporter = \
            InMemoryItemExporter(item_types=['block', 'transaction'])
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=export_blocks,
            export_transactions=export_transactions)

        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items('block')
        transactions = blocks_and_transactions_item_exporter \
            .get_items('transaction')
        return blocks, transactions

    def export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=['receipt', 'log'])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction['hash'] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=True,
            export_logs=False)

        job.run()
        receipts = exporter.get_items('receipt')
        logs = exporter.get_items('log')
        return receipts, logs

    def export_traces(self, start_block, end_block, genesis_traces=False):
        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
            include_genesis_traces=genesis_traces,
        )
        job.run()
        traces = exporter.get_items('trace')
        return traces


def hex_to_bytearray(hex_str):
    return bytearray.fromhex(hex_str[2:]) if hex_str is not None else None

def build_cql_insert_stmt(columns, table):
    return 'INSERT INTO %s (%s) VALUES (%s);' % \
        (table, ', '.join(columns), ('?,' * len(columns))[:-1])

def get_last_synced_block(batch_web3_provider):
    return int(Web3(batch_web3_provider).eth.getBlock('latest').number)

def get_last_ingested_block(session, keyspace):
    result = session.execute(
        f'SELECT block_id_group FROM {keyspace}.block PER PARTITION LIMIT 1')
    groups = [row.block_id_group for row in result.current_rows]

    if len(groups) == 0:
        return 0

    max_block_group = max(groups)

    result = session.execute(
        f'''SELECT MAX(block_id) AS max_block
            FROM {keyspace}.block
            WHERE block_id_group={max_block_group}''')
    max_block = result.current_rows[0].max_block

    return max_block

def cassandra_ingest(session, prepared_stmt, parameters, concurrency=100):
    results = execute_concurrent_with_args(
        session=session,
        statement=prepared_stmt,
        parameters=parameters,
        concurrency=concurrency)
    for (i, (success, _)) in enumerate(results):
        if not success:
            try:
                session.execute(prepared_stmt, parameters[i])
            except Exception as e:
                print(e)
                continue
            break

def ingest_blocks(items, session, table='block', block_bucket_size=100_000):
    columns = [
        'block_id_group',
        'block_id',
        'block_hash',
        'parent_hash',
        'nonce',
        'sha3_uncles',
        'logs_bloom',
        'transactions_root',
        'state_root',
        'receipts_root',
        'miner',
        'difficulty',
        'total_difficulty',
        'size',
        'extra_data',
        'gas_limit',
        'gas_used',
        'timestamp',
        'transaction_count'
    ]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    blob_colums = ['block_hash', 'parent_hash', 'nonce', 'sha3_uncles',
                   'logs_bloom', 'transactions_root', 'state_root',
                   'receipts_root', 'miner', 'extra_data']
    for item in items:
        # remove column
        item.pop('type')
        # rename/add columns
        item['block_id'] = item.pop('number')
        item['block_id_group'] = item['block_id'] // block_bucket_size
        item['block_hash'] = item.pop('hash')
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(session, prepared_stmt, items)

def ingest_txs(items, session, table='transaction', tx_hash_prefix_len=4):
    columns = [
        'tx_hash_prefix',
        'tx_hash',
        'nonce',
        'transaction_index',
        'from_address',
        'to_address',
        'value',
        'gas',
        'gas_price',
        'input',
        'block_timestamp',
        'block_id',
        'block_hash',
        'max_fee_per_gas',
        'max_priority_fee_per_gas',
        'transaction_type',
        'receipt_cumulative_gas_used',
        'receipt_gas_used',
        'receipt_contract_address',
        'receipt_root',
        'receipt_status',
        'receipt_effective_gas_price'
    ]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    blob_colums = ['tx_hash', 'from_address', 'to_address', 'input',
                   'block_hash', 'receipt_contract_address', 'receipt_root']
    for item in items:
        # remove column
        item.pop('type')
        # rename/add columns
        item['tx_hash'] = item.pop('hash')
        item['tx_hash_prefix'] = item['tx_hash'][2:(2 + tx_hash_prefix_len)]
        item['block_id'] = item.pop('block_number')
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(session, prepared_stmt, items)

def ingest_traces(items, session, table='trace', block_bucket_size = 100_000):
    columns = [
        'block_id_group',
        'block_id',
        'trace_id',
        'tx_hash',
        'transaction_index',
        'from_address',
        'to_address',
        'value',
        'input',
        'output',
        'trace_type',
        'call_type',
        'reward_type',
        'gas',
        'gas_used',
        'subtraces',
        'trace_address',
        'error',
        'status',
    ]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    blob_colums = ['tx_hash', 'from_address', 'to_address', 'input', 'output']
    for item in items:
        # remove column
        item.pop('type')
        # rename, modify and add columns
        item['tx_hash'] = item.pop('transaction_hash')
        item['block_id'] = item.pop('block_number')
        item['block_id_group'] = item['block_id'] // block_bucket_size
        item['trace_address'] = ','.join(str(item['trace_address'])) \
            if item['trace_address'] is not None else None
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(session, prepared_stmt, items)

def create_parser():
    parser = ArgumentParser(
        description='ethereum-etl ingest into Apache Cassandra',
        epilog='GraphSense - http://graphsense.info')
    #parser.add_argument('--concurrency', dest='concurrency',
    #                    type=int, default=100,
    #                    help='Cassandra concurrency parameter (default 100)')
    #parser.add_argument('--continue', action='store_true',
    #                    dest='continue_ingest',
    #                    help='continue ingest from last block')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default=['localhost'], metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost")')
    parser.add_argument('-i', '--info', action='store_true',
                        help='display block information and exit')
    parser.add_argument('-k', '--keyspace',
                        dest='keyspace', required=True,
                        help='Cassandra keyspace')
    #parser.add_argument('-p', '--previous_day', dest='prev_day',
    #                    action='store_true',
    #                    help='only ingest blocks up to the previous day, '
    #                         'since currency exchange rates might not be '
    #                         'available for the current day')
    parser.add_argument('-w', '--web3-provider-uri',
                        dest='provider_uri', required=True,
                        help='Web3 provider URI')
    parser.add_argument('-s', '--start-block', dest='start_block',
                        type=int, default=0,
                        help='start block (default 0)')
    parser.add_argument('-e', '--end-block', dest='end_block',
                        type=int, default=None,
                        help='end block (default: last available block)')
    return parser

def main():

    parser = create_parser()
    args = parser.parse_args()

    BLOCK_BUCKET_SIZE = 100_000
    TX_HASH_PREFIX_LEN = 4

    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(args.provider_uri, timeout=180, batch=True)
    )

    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = get_last_ingested_block(session, args.keyspace)
    print(f'Last synced block: {last_synced_block:,}')
    print(f'Last ingested block: {last_ingested_block:,}')
    if args.info:
        cluster.shutdown()
        raise SystemExit(0)

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = args.start_block
    end_block = last_synced_block if args.end_block is None else args.end_block

    BATCH_SIZE = 10

    time1 = datetime.now()
    count = 0

    print(f'[{time1}] Ingesting block range '
          f'{start_block:,}:{end_block:,} '
          f'into Cassandra nodes {args.db_nodes}')

    excluded_call_types = ['delegatecall', 'callcode', 'staticcall']

    for block_id in range(start_block, end_block + 1, BATCH_SIZE):

        current_end_block = min(end_block, block_id + BATCH_SIZE - 1)

        blocks, txs = adapter.export_blocks_and_transactions(
             block_id, current_end_block)
        receipts, _ = adapter.export_receipts_and_logs(txs)
        traces = adapter.export_traces(block_id, current_end_block, True)

        # filter traces relevent for balance calculation
        filtered_traces = [elem for elem in traces
                           if (elem['status'] == 1) and
                              (not elem['call_type'] or
                               elem['call_type'] not in excluded_call_types)]
        enriched_txs = enrich_transactions(txs, receipts)

        # ingest into Cassandra
        ingest_blocks(blocks, session, 'block', BLOCK_BUCKET_SIZE)
        ingest_txs(enriched_txs, session, 'transaction', TX_HASH_PREFIX_LEN)
        ingest_traces(filtered_traces, session, 'trace', BLOCK_BUCKET_SIZE)

        count += BATCH_SIZE

        if count % 1000 == 0:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            print(f'[{time2}] '
                  f'Last processed block: {current_end_block:,} '
                  f'({count/time_delta:.1f} blocks/s)')
            time1 = time2
            count = 0


    print(f'[{datetime.now().isoformat()}] Processed block range '
          f'{start_block:,}:{end_block:,}')

    cluster.shutdown()


if __name__ == '__main__':
    main()
