#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ethereum-etl streaming adapter.

   Ingest blocks, transactions/receipts and traces into Apache Cassandra.
"""
import warnings
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from cassandra.cluster import (
    Cluster,
    Session,
)
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import PreparedStatement, SimpleStatement, UNSET_VALUE
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_service import EthService
from ethereumetl.streaming.enrich import enrich_transactions
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import (
    EthItemTimestampCalculator,
)
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from web3 import Web3
from typing import Union


BLOCK_BUCKET_SIZE = 1_000
TX_HASH_PREFIX_LEN = 5


def none_to_unset(items: Union[dict, tuple, list]):
    """Sets all None value to UNSET
    See https://stackoverflow.com/questions/34637680/how-insert-in-cassandra-without-null-value-in-column

    Args:
        items (Union[dict, tuple, list]): items to insert

    Returns:
        None: -

    Raises:
        Exception: If datatype of items is not supported (list,tuple,dict)
    """
    if type(items) == dict:
        return {k: (UNSET_VALUE if v is None else v) for k, v in items.items()}
    elif type(items) == tuple:
        return tuple([UNSET_VALUE if v is None else v for v in list(items)])
    elif type(items) == list:
        return [(UNSET_VALUE if v is None else v) for v in items]
    else:
        raise Exception(
            f"Can't auto unset for type {type(items)} please assign "
            "cassandra.query.UNSET_VALUE manually."
        )


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
        item_type = item.get("type", None)
        if item_type is None:
            raise ValueError(f"type key is not found in item {item}")
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
        max_workers: int = 5,
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
        export_transactions: bool = True,
    ) -> Tuple[Iterable, Iterable]:
        """Export blocks and transactions for specified block range."""

        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=["block", "transaction"]
        )
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=export_blocks,
            export_transactions=export_transactions,
        )

        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items("block")
        transactions = blocks_and_transactions_item_exporter.get_items("transaction")
        return blocks, transactions

    def export_receipts_and_logs(
        self, transactions: Iterable
    ) -> Tuple[Iterable, Iterable]:
        """Export receipts and logs for specified transaction hashes."""

        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["hash"] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=True,
            export_logs=True,
        )

        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def export_traces(
        self,
        start_block: int,
        end_block: int,
        include_genesis_traces: bool = False,
        include_daofork_traces: bool = False,
    ) -> Iterable[Dict]:
        """Export traces for specified block range."""

        exporter = InMemoryItemExporter(item_types=["trace"])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
            include_genesis_traces=include_genesis_traces,
            include_daofork_traces=include_daofork_traces,
        )
        job.run()
        traces = exporter.get_items("trace")
        return traces


def hex_to_bytearray(hex_str: str) -> Optional[bytearray]:
    """Convert hexstring (starting with 0x) to bytearray."""

    return bytearray.fromhex(hex_str[2:]) if hex_str is not None else None


def build_cql_insert_stmt(columns: Sequence[str], table: str) -> str:
    """Create CQL insert statement for specified columns and table name."""

    return "INSERT INTO %s (%s) VALUES (%s);" % (
        table,
        ", ".join(columns),
        ("?," * len(columns))[:-1],
    )


def get_last_block_yesterday(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last block number of previous day from Ethereum client."""

    web3 = Web3(batch_web3_provider)
    eth_service = EthService(web3)

    date = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    print(
        f"Determining latest block before {date.isoformat()}: ",
        end="",
        flush=True,
    )
    prev_date = datetime.date(datetime.today()) - timedelta(days=1)
    _, end_block = eth_service.get_block_range_for_date(prev_date)
    print(f"{end_block:,}")
    return end_block


def get_last_synced_block(batch_web3_provider: ThreadLocalProxy) -> int:
    """Return last synchronized block number from Ethereum client."""

    return int(Web3(batch_web3_provider).eth.getBlock("latest").number)


def get_last_ingested_block(session: Session, table="block") -> Optional[int]:
    """Return last ingested block ID from block table."""

    cql_str = f"""SELECT block_id_group FROM {session.keyspace}.{table}
                  PER PARTITION LIMIT 1"""
    simple_stmt = SimpleStatement(cql_str, fetch_size=None)
    result = session.execute(simple_stmt)
    groups = [row.block_id_group for row in result.current_rows]

    if len(groups) == 0:
        return None

    max_block_group = max(groups)

    result = session.execute(
        f"""SELECT MAX(block_id) AS max_block FROM {session.keyspace}.{table}
            WHERE block_id_group={max_block_group}"""
    )
    max_block = result.current_rows[0].max_block

    return max_block


def get_prepared_statement(
    session: Session, keyspace: str, table: str
) -> PreparedStatement:
    """Build prepared CQL INSERT statement for specified table."""

    cql_str = f"""SELECT column_name FROM system_schema.columns
                  WHERE keyspace_name = \'{keyspace}\'
                  AND table_name = \'{table}\';"""
    result_set = session.execute(cql_str)
    columns = [elem.column_name for elem in result_set._current_rows]
    cql_str = build_cql_insert_stmt(columns, table)
    prepared_stmt = session.prepare(cql_str)
    return prepared_stmt


def cassandra_ingest(
    session: Session,
    prepared_stmt: PreparedStatement,
    parameters,
    concurrency: int = 100,
    retry_thsh: int = 1000,
) -> None:
    """Concurrent ingest into Apache Cassandra."""
    ctr = 0
    while True:
        try:
            results = execute_concurrent_with_args(
                session=session,
                statement=prepared_stmt,
                parameters=parameters,
                concurrency=concurrency,
            )

            for (i, (success, _)) in enumerate(results):
                if not success:
                    while True:
                        try:
                            session.execute(prepared_stmt, parameters[i])
                        except Exception as exception:
                            ctr += 1
                            if ctr > retry_thsh:
                                raise exception
                            print(exception)
                            continue
                        ctr = 0
                        break
            break

        except Exception as exception:
            ctr += 1
            if ctr > retry_thsh:
                raise exception
            print(exception)
            time.sleep(1)
            continue
        ctr = 0


def ingest_configuration(
    session: Session,
    keyspace: str,
    block_bucket_size: int,
    tx_hash_prefix_len: int,
) -> None:
    """Store configuration details in Cassandra table."""

    cql_str = """INSERT INTO configuration
                 (id, block_bucket_size, tx_prefix_length)
                 VALUES (%s, %s, %s)"""
    session.execute(
        cql_str,
        (keyspace, int(block_bucket_size), int(tx_hash_prefix_len)),
    )


def ingest_logs(
    items: Iterable,
    session: Session,
    prepared_stmt: PreparedStatement,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    blob_colums = [
        "block_hash",
        "address",
        "data",
        "topic0",
        "tx_hash",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size

        tpcs = item["topics"]

        if tpcs is None:
            tpcs = []

        if "topic0" not in item:
            # bugfix do not use None for topic0 but 0x, None
            # gets converted to UNSET which is not allowed for
            # key columns in cassandra and can not be filtered
            item["topic0"] = tpcs[0] if len(tpcs) > 0 else "0x"

        item["topics"] = [hex_to_bytearray(t) for t in tpcs]

        if "transaction_hash" in item:
            item.pop("transaction_hash")

        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    items = [none_to_unset(row) for row in items]

    cassandra_ingest(session, prepared_stmt, items)


def ingest_blocks(
    items: Iterable,
    session: Session,
    prepared_stmt: PreparedStatement,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest blocks into Apache Cassandra."""

    blob_colums = [
        "block_hash",
        "parent_hash",
        "nonce",
        "sha3_uncles",
        "logs_bloom",
        "transactions_root",
        "state_root",
        "receipts_root",
        "miner",
        "extra_data",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["block_id"] = item.pop("number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["block_hash"] = item.pop("hash")
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    items = [none_to_unset(row) for row in items]

    cassandra_ingest(session, prepared_stmt, items)


def ingest_transactions(
    items: Iterable,
    session: Session,
    prepared_stmt: PreparedStatement,
    tx_hash_prefix_len: int = 4,
) -> None:
    """Ingest transactions into Apache Cassandra."""

    blob_colums = [
        "tx_hash",
        "from_address",
        "to_address",
        "input",
        "block_hash",
        "receipt_contract_address",
        "receipt_root",
    ]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("hash")
        hash_slice = slice(2, 2 + tx_hash_prefix_len)
        item["tx_hash_prefix"] = item["tx_hash"][hash_slice]
        item["block_id"] = item.pop("block_number")
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    items = [none_to_unset(row) for row in items]

    cassandra_ingest(session, prepared_stmt, items)


def ingest_traces(
    items: Iterable,
    session: Session,
    prepared_stmt: PreparedStatement,
    block_bucket_size: int = 1_000,
) -> None:
    """Ingest traces into Apache Cassandra."""

    blob_colums = ["tx_hash", "from_address", "to_address", "input", "output"]
    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["trace_address"] = (
            ",".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )
        # convert hex strings to byte arrays (blob in Cassandra)
        for elem in blob_colums:
            item[elem] = hex_to_bytearray(item[elem])

    items = [none_to_unset(row) for row in items]

    cassandra_ingest(session, prepared_stmt, items)


def create_parser() -> ArgumentParser:
    """Create command-line argument parser."""

    parser = ArgumentParser(
        description="ethereum-etl ingest into Apache Cassandra",
        epilog="GraphSense - http://graphsense.info",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        dest="batch_size",
        type=int,
        default=10,
        help="number of blocks to export at a time (default: 10)",
    )
    parser.add_argument(
        "-d",
        "--db_nodes",
        dest="db_nodes",
        nargs="+",
        default=["localhost"],
        metavar="DB_NODE",
        help="list of Cassandra nodes; default: 'localhost')",
    )
    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help="display block information and exit",
    )
    parser.add_argument(
        "-k",
        "--keyspace",
        dest="keyspace",
        required=True,
        help="Cassandra keyspace",
    )
    parser.add_argument(
        "-p",
        "--previous_day",
        dest="prev_day",
        action="store_true",
        help="only ingest blocks up to the previous day, "
        "since currency exchange rates might not be "
        "available for the current day",
    )
    parser.add_argument(
        "-w",
        "--web3-provider-uri",
        dest="provider_uri",
        required=True,
        help="Web3 provider URI",
    )
    parser.add_argument(
        "-s",
        "--start-block",
        dest="start_block",
        type=int,
        default=None,
        help="start block (default: continue from last ingested block)",
    )
    parser.add_argument(
        "-e",
        "--end-block",
        dest="end_block",
        type=int,
        default=None,
        help="end block (default: last available block)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=int,
        default=3600,
        help="Web3 API timeout in seconds (default: 3600s)",
    )
    return parser


def print_block_info(
    last_synced_block: int, last_ingested_block: Optional[int]
) -> None:
    """Display information about number of synced/ingested blocks."""

    print(f"Last synced block: {last_synced_block:,}")
    if last_ingested_block is None:
        print("Last ingested block: None")
    else:
        print(f"Last ingested block: {last_ingested_block:,}")


def main() -> None:
    """Main function."""

    warnings.warn("graphsense-ethereum-etl is deprecated. Please use https://github.com/graphsense/graphsense-lib -> graphsense-cli ingest which provides the same functionality", DeprecationWarning)

    args = create_parser().parse_args()

    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(
            args.provider_uri, timeout=args.timeout, batch=True
        )
    )

    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)

    last_synced_block = get_last_synced_block(thread_proxy)
    last_ingested_block = get_last_ingested_block(session)
    print_block_info(last_synced_block, last_ingested_block)

    if args.info:
        cluster.shutdown()
        raise SystemExit(0)

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = 0
    if args.start_block is None:
        if last_ingested_block is not None:
            start_block = last_ingested_block + 1
    else:
        start_block = args.start_block

    end_block = last_synced_block
    if args.end_block is not None:
        end_block = args.end_block
    if args.prev_day:
        end_block = get_last_block_yesterday(thread_proxy)

    if start_block > end_block:
        print("No blocks to ingest")
        raise SystemExit(0)

    time1 = datetime.now()
    count = 0

    print(
        f"[{time1}] Ingesting block range "
        f"{start_block:,}:{end_block:,} "
        f"into Cassandra nodes {args.db_nodes}"
    )

    prep_stmt = {
        elem: get_prepared_statement(session, args.keyspace, elem)
        for elem in ["trace", "transaction", "block", "log"]
    }

    for block_id in range(start_block, end_block + 1, args.batch_size):
        current_end_block = min(end_block, block_id + args.batch_size - 1)

        blocks, txs = adapter.export_blocks_and_transactions(
            block_id, current_end_block
        )
        receipts, logs = adapter.export_receipts_and_logs(txs)
        traces = adapter.export_traces(block_id, current_end_block, True, True)

        enriched_txs = enrich_transactions(txs, receipts)

        # ingest into Cassandra
        ingest_logs(logs, session, prep_stmt["log"], BLOCK_BUCKET_SIZE)
        ingest_traces(traces, session, prep_stmt["trace"], BLOCK_BUCKET_SIZE)
        ingest_transactions(
            enriched_txs, session, prep_stmt["transaction"], TX_HASH_PREFIX_LEN
        )
        ingest_blocks(blocks, session, prep_stmt["block"], BLOCK_BUCKET_SIZE)

        count += args.batch_size

        if count % 1000 == 0:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            print(
                f"[{time2}] "
                f"Last processed block: {current_end_block:,} "
                f"({count/time_delta:.1f} blocks/s)"
            )
            time1 = time2
            count = 0

    print(f"[{datetime.now()}] Processed block range " f"{start_block:,}:{end_block:,}")

    # store configuration details
    ingest_configuration(
        session, args.keyspace, int(BLOCK_BUCKET_SIZE), int(TX_HASH_PREFIX_LEN)
    )

    cluster.shutdown()


if __name__ == "__main__":
    main()
