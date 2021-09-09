#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ethereum-etl streaming adapter.

   Ingest blocks, transactions/receipts and traces into Apache Cassandra.
"""

from argparse import ArgumentParser
from datetime import datetime
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from cassandra.cluster import Cluster, PreparedStatement, Session
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


BLOCK_BUCKET_SIZE = 100_000
TX_HASH_PREFIX_LEN = 4


class InMemoryItemExporter:
    """In-memory item exporter for EthStreamerAdapter export jobs."""

    def __init__(self, item_types: Iterable) -> None:
        self.item_types = item_types
        self.items: Dict[str, List] = {}

    def open(self) -> None:
        """Open item exporter."""
        for item_type in self.item_types:
            self.items[item_type] = []

    def export_item(self, item) -> None:
        """Export single item."""
        item_type = item.get('type', None)
        if item_type is None:
            raise ValueError(f'type key is not found in item {item}')
        self.items[item_type].append(item)

    def close(self) -> None:
        """Close item exporter."""

    def get_items(self, item_type) -> Iterable:
        """Get items from exporter."""
        return self.items[item_type]


class EthStreamerAdapter:
    """Ethereum streaming adapter to export blocks, transactions,
       receipts, logs amd traces."""

    def __init__(
            self,
            batch_web3_provider: ThreadLocalProxy,
            batch_size: int = 100,
            max_workers: int = 5
           ) -> None:
        self.batch_web3_provider = batch_web3_provider
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

    def export_blocks_and_transactions(
            self,
            start_block: int,
            end_block: int,
            export_blocks: bool = True,
            export_transactions: bool = True
           ) -> Tuple[Iterable, Iterable]:
        """Export blocks and transactions for specified block range."""

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

    def export_receipts_and_logs(
            self,
            transactions: Iterable
           ) -> Tuple[Iterable, Iterable]:
        """Export receipts and logs for specified transaction hashes."""

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

    def export_traces(
            self,
            start_block: int,
            end_block: int,
            include_genesis_traces: bool = False,
            include_daofork_traces: bool = False
           ) -> Iterable[Dict]:
        """Export traces for specified block range."""

        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
            include_genesis_traces=include_genesis_traces,
            include_daofork_traces=include_daofork_traces
        )
        job.run()
        traces = exporter.get_items('trace')
        return traces


def hex_to_bytearray(hex_str: str) -> Optional[bytearray]:
    """Convert hexstring (starting with 0x) to bytearray."""

    return bytearray.fromhex(hex_str[2:]) if hex_str is not None else None


def build_cql_insert_stmt(columns: Sequence[str], table: str) -> str:
    """Create CQL insert statement for specified columns and table name."""

    return 'INSERT INTO %s (%s) VALUES (%s);' % \
        (table, ', '.join(columns), ('?,' * len(columns))[:-1])


def get_last_synced_block(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last synchronized block from Ethereum client."""

    return int(Web3(batch_web3_provider).eth.getBlock('latest').number)


def get_last_ingested_block(
        session: Session,
        keyspace: str
       ) -> Optional[int]:
    """Return last ingested block ID from block table."""

    result = session.execute(
        f'SELECT block_id_group FROM {keyspace}.block PER PARTITION LIMIT 1')
    groups = [row.block_id_group for row in result.current_rows]

    if len(groups) == 0:
        return None

    max_block_group = max(groups)

    result = session.execute(
        f'''SELECT MAX(block_id) AS max_block
            FROM {keyspace}.block
            WHERE block_id_group={max_block_group}''')
    max_block = result.current_rows[0].max_block

    return max_block


def get_prepared_statement(
        session: Session,
        keyspace: str,
        table: str
       ) -> PreparedStatement:
    """Build prepared CQL INSERT statement for specified table."""

    cql_str = f'''SELECT column_name FROM system_schema.columns
                  WHERE keyspace_name = \'{keyspace}\'
                  AND table_name = \'{table}\';'''
    result_set = session.execute(cql_str)
    columns = [elem.column_name for elem in result_set._current_rows]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    return prepared_stmt


def cassandra_ingest(
        session: Session,
        prepared_stmt: PreparedStatement,
        parameters,
        concurrency: int = 100
       ) -> None:
    """Concurrent ingest into Apache Cassandra."""

    while True:
        try:
            results = execute_concurrent_with_args(
                session=session,
                statement=prepared_stmt,
                parameters=parameters,
                concurrency=concurrency)

            for (i, (success, _)) in enumerate(results):
                if not success:
                    while True:
                        try:
                            session.execute(prepared_stmt, parameters[i])
                        except Exception as exception:
                            print(exception)
                            continue
                        break
            break

        except Exception as exception:
            print(exception)
            time.sleep(1)
            continue


def ingest_blocks(
        items: Iterable,
        session: Session,
        prepared_stmt: PreparedStatement,
        block_bucket_size: int = 100_000
       ) -> None:
    """Ingest blocks into Apache Cassandra."""

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


def ingest_transactions(
        items: Iterable,
        session: Session,
        prepared_stmt: PreparedStatement,
        tx_hash_prefix_len: int = 4
       ) -> None:
    """Ingest transactions into Apache Cassandra."""

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


def ingest_traces(
        items: Iterable,
        session: Session,
        prepared_stmt: PreparedStatement,
        block_bucket_size: int = 100_000
       ) -> None:
    """Ingest traces into Apache Cassandra."""

    blob_colums = ['tx_hash', 'from_address', 'to_address', 'input', 'output']
    for item in items:
        # remove column
        item.pop('type')
        # rename/add columns
        item['tx_hash'] = item.pop('transaction_hash')
        item['block_id'] = item.pop('block_number')
        item['block_id_group'] = item['block_id'] // block_bucket_size
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    cassandra_ingest(session, prepared_stmt, items)


def create_parser():
    """Create command-line argument parser."""

    parser = ArgumentParser(
        description='ethereum-etl ingest into Apache Cassandra',
        epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-b', '--batch-size', dest='batch_size',
                        type=int, default=100,
                        help='number of blocks to export at a time '
                             '(default 100)')
    parser.add_argument('-c', '--continue', action='store_true',
                        dest='continue_ingest',
                        help='continue ingest from last block')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default=['localhost'], metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost")')
    parser.add_argument('-i', '--info', action='store_true',
                        help='display block information and exit')
    parser.add_argument('-k', '--keyspace',
                        dest='keyspace', required=True,
                        help='Cassandra keyspace')
    # parser.add_argument('-p', '--previous_day', dest='prev_day',
    #                     action='store_true',
    #                     help='only ingest blocks up to the previous day, '
    #                          'since currency exchange rates might not be '
    #                          'available for the current day')
    parser.add_argument('-w', '--web3-provider-uri',
                        dest='provider_uri', required=True,
                        help='Web3 provider URI')
    parser.add_argument('-s', '--start-block', dest='start_block',
                        type=int, default=0,
                        help='start block (default 0)')
    parser.add_argument('-e', '--end-block', dest='end_block',
                        type=int, default=None,
                        help='end block (default: last available block)')
    parser.add_argument('-t', '--timeout', dest='timeout',
                        type=int, default=3600,
                        help='Web3 API timeout in seconds (default: 3600s')
    return parser


def main() -> None:

    parser = create_parser()
    args = parser.parse_args()

    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(
                    args.provider_uri, timeout=args.timeout, batch=True)
    )

    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = get_last_ingested_block(session, args.keyspace)
    print(f'Last synced block: {last_synced_block:,}')
    if last_ingested_block is None:
        print('Last ingested block: None')
    else:
        print(f'Last ingested block: {last_ingested_block:,}')

    if args.info:
        cluster.shutdown()
        raise SystemExit(0)

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = args.start_block
    if args.continue_ingest:
        if last_ingested_block is None:
            start_block = 0
        else:
            start_block = last_ingested_block + 1

    end_block = last_synced_block if args.end_block is None else args.end_block

    if start_block > end_block:
        print('No blocks to ingest')
        raise SystemExit(0)

    time1 = datetime.now()
    count = 0

    print(f'[{time1}] Ingesting block range '
          f'{start_block:,}:{end_block:,} '
          f'into Cassandra nodes {args.db_nodes}')

    excluded_call_types = ['delegatecall', 'callcode', 'staticcall']

    prep_stmt = {elem: get_prepared_statement(session, args.keyspace, elem)
                 for elem in ['trace', 'transaction', 'block']}

    for block_id in range(start_block, end_block + 1, args.batch_size):

        current_end_block = min(end_block, block_id + args.batch_size - 1)

        blocks, txs = adapter.export_blocks_and_transactions(
             block_id, current_end_block)
        receipts, _ = adapter.export_receipts_and_logs(txs)
        traces = adapter.export_traces(block_id, current_end_block, True, True)

        enriched_txs = enrich_transactions(txs, receipts)

        # ingest into Cassandra
        ingest_traces(
            traces,
            session,
            prep_stmt['trace'],
            BLOCK_BUCKET_SIZE)
        ingest_transactions(
            enriched_txs,
            session,
            prep_stmt['transaction'],
            TX_HASH_PREFIX_LEN)
        ingest_blocks(
            blocks,
            session,
            prep_stmt['block'],
            BLOCK_BUCKET_SIZE)

        count += args.batch_size

        if count % 1000 == 0:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            print(f'[{time2}] '
                  f'Last processed block: {current_end_block:,} '
                  f'({count/time_delta:.1f} blocks/s)')
            time1 = time2
            count = 0

    print(f'[{datetime.now()}] Processed block range '
          f'{start_block:,}:{end_block:,}')

    # store configuration details
    cql_str = '''INSERT INTO configuration
                 (id, block_bucket_size, tx_prefix_length)
                 VALUES (%s, %s, %s)'''
    session.execute(
        cql_str,
        (args.keyspace, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN))
    )

    cluster.shutdown()


if __name__ == '__main__':
    main()
