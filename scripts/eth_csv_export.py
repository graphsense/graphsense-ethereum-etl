#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ethereum-etl CSV streaming adapter.

Exports blocks, transactions/receipts and traces to CSV files.
"""

import warnings
from argparse import ArgumentParser
from csv import DictWriter, QUOTE_NONE
from datetime import datetime, timedelta, timezone
import gzip
import pathlib
import re
from typing import Dict, Iterable, List, Tuple

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


TX_HASH_PREFIX_LEN = 5

BLOCK_HEADER = [
    "parent_hash",
    "nonce",
    "sha3_uncles",
    "logs_bloom",
    "transactions_root",
    "state_root",
    "receipts_root",
    "miner",
    "difficulty",
    "total_difficulty",
    "size",
    "extra_data",
    "gas_limit",
    "gas_used",
    "timestamp",
    "transaction_count",
    "base_fee_per_gas",
    "block_id",
    "block_id_group",
    "block_hash",
]

TX_HEADER = [
    "nonce",
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "gas",
    "gas_price",
    "input",
    "block_timestamp",
    "block_hash",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "transaction_type",
    "receipt_cumulative_gas_used",
    "receipt_gas_used",
    "receipt_contract_address",
    "receipt_root",
    "receipt_status",
    "receipt_effective_gas_price",
    "tx_hash",
    "tx_hash_prefix",
    "block_id",
]

TRACE_HEADER = [
    "transaction_index",
    "from_address",
    "to_address",
    "value",
    "input",
    "output",
    "trace_type",
    "call_type",
    "reward_type",
    "gas",
    "gas_used",
    "subtraces",
    "trace_address",
    "error",
    "status",
    "trace_id",
    "trace_index",
    "tx_hash",
    "block_id",
    "block_id_group",
]

LOGS_HEADER = [
    "block_id_group",
    "block_id",
    "block_hash",
    "address",
    "data",
    "topics",
    "topic0",
    "tx_hash",
    "log_index",
    "transaction_index",
]


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


def format_blocks(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format blocks."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["block_id"] = item.pop("number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["block_hash"] = item.pop("hash")

    return items


def format_transactions(
    items: Iterable,
    tx_hash_prefix_len: int = 4,
) -> None:
    """Format transactions."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("hash")
        hash_slice = slice(2, 2 + tx_hash_prefix_len)
        item["tx_hash_prefix"] = item["tx_hash"][hash_slice]
        item["block_id"] = item.pop("block_number")

    return items


def format_traces(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format traces."""

    for item in items:
        # remove column
        item.pop("type")
        # rename/add columns
        item["tx_hash"] = item.pop("transaction_hash")
        item["block_id"] = item.pop("block_number")
        item["block_id_group"] = item["block_id"] // block_bucket_size
        item["trace_address"] = (
            "|".join(map(str, item["trace_address"]))
            if item["trace_address"] is not None
            else None
        )

    return items


def format_logs(
    items: Iterable,
    block_bucket_size: int = 1_000,
) -> None:
    """Format logs."""

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
            item["topic0"] = tpcs[0] if len(tpcs) > 0 else None

        qt = ",".join([f'"{t}"' for t in tpcs])

        item["topics"] = f"[{qt}]"

        if "transaction_hash" in item:
            item.pop("transaction_hash")

    return items


def write_csv(
    filename: str, data: Iterable, header: Iterable, delimiter: str = ",", quoting=None
) -> None:
    """Write list of dicts to compresses CSV file."""

    with gzip.open(filename, "wt") as csv_file:
        if quoting is None:
            csv_writer = DictWriter(csv_file, delimiter=delimiter, fieldnames=header)
        else:
            csv_writer = DictWriter(
                csv_file,
                delimiter=delimiter,
                fieldnames=header,
                quoting=quoting,
                quotechar="",
            )
        csv_writer.writeheader()
        for row in data:
            csv_writer.writerow(row)


def create_parser() -> ArgumentParser:
    """Create command-line argument parser."""

    parser = ArgumentParser(
        description="ethereum-etl export to text files",
        epilog="GraphSense - http://graphsense.info",
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        default=10,
        help="number of blocks to export from client at a time (default: 10)",
    )
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_export",
        action="store_true",
        help="continue from export from position",
    )
    parser.add_argument(
        "-d",
        "--directory",
        dest="dir",
        help="name of base output directory",
    )
    parser.add_argument(
        "--file-batch-size",
        dest="file_batch_size",
        type=int,
        default=1000,
        help="number of blocks to export to a CSV file (default: 1000)",
    )
    parser.add_argument(
        "--partition-batch-size",
        dest="partition_batch_size",
        type=int,
        default=1_000_000,
        help="number of blocks to export in partition (default: 1_000_000)",
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
        help="start block (default: 0)",
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


def main() -> None:
    """Main function."""

    warnings.warn("graphsense-ethereum-etl is deprecated. Please use https://github.com/graphsense/graphsense-lib -> graphsense-cli ingest which provides the same functionality", DeprecationWarning)

    args = create_parser().parse_args()

    thread_proxy = ThreadLocalProxy(
        lambda: get_provider_from_uri(
            args.provider_uri, timeout=args.timeout, batch=True
        )
    )

    adapter = EthStreamerAdapter(thread_proxy, batch_size=50)

    start_block = 0
    if args.start_block is None:
        if args.continue_export:
            block_files = sorted(pathlib.Path(args.dir).rglob("block*"))
            if block_files:
                last_file = block_files[-1].name
                print(f"Last exported file: {block_files[-1]}")
                start_block = int(re.match(r".*-(\d+)", last_file).group(1)) + 1
    else:
        start_block = args.start_block

    end_block = get_last_synced_block(thread_proxy)
    print(f"Last synced block: {end_block:,}")
    if args.end_block is not None:
        end_block = args.end_block
    if args.prev_day:
        end_block = get_last_block_yesterday(thread_proxy)

    time1 = datetime.now()
    count = 0

    block_bucket_size = args.file_batch_size
    if args.file_batch_size % args.batch_size != 0:
        print("Error: file_batch_size is not a multiple of batch_size")
        raise SystemExit(1)
    if args.partition_batch_size % args.file_batch_size != 0:
        print("Error: partition_batch_size is not a multiple of file_batch_size")
        raise SystemExit(1)

    rounded_start_block = start_block // block_bucket_size * block_bucket_size
    rounded_end_block = (end_block + 1) // block_bucket_size * block_bucket_size - 1

    if rounded_start_block > rounded_end_block:
        print("No blocks to export")
        raise SystemExit(0)

    block_range = (
        rounded_start_block,
        rounded_start_block + block_bucket_size - 1,
    )

    path = pathlib.Path(args.dir)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except (PermissionError, NotADirectoryError) as exception:
        print(exception)
        raise SystemExit(1) from exception

    block_file = "block_%08d-%08d.csv.gz" % block_range
    tx_file = "tx_%08d-%08d.csv.gz" % block_range
    trace_file = "trace_%08d-%08d.csv.gz" % block_range
    logs_file = "logs_%08d-%08d.csv.gz" % block_range

    print(
        f"[{time1}] Processing block range "
        f"{rounded_start_block:,}:{rounded_end_block:,}"
    )

    block_list = []
    tx_list = []
    trace_list = []
    logs_list = []

    for block_id in range(rounded_start_block, rounded_end_block + 1, args.batch_size):

        current_end_block = min(end_block, block_id + args.batch_size - 1)

        blocks, txs = adapter.export_blocks_and_transactions(
            block_id, current_end_block
        )
        receipts, logs = adapter.export_receipts_and_logs(txs)
        traces = adapter.export_traces(block_id, current_end_block, True, True)
        enriched_txs = enrich_transactions(txs, receipts)

        block_list.extend(format_blocks(blocks))
        tx_list.extend(format_transactions(enriched_txs, TX_HASH_PREFIX_LEN))
        trace_list.extend(format_traces(traces))
        logs_list.extend(format_logs(logs))

        count += args.batch_size

        if count >= 1000:
            time2 = datetime.now()
            time_delta = (time2 - time1).total_seconds()
            print(
                f"[{time2}] Last processed block {current_end_block} "
                f"({count/time_delta:.1f} blocks/s)"
            )
            time1 = time2
            count = 0

        if (block_id + args.batch_size) % block_bucket_size == 0:
            time3 = datetime.now()
            partition_start = block_id - (block_id % args.partition_batch_size)
            partition_end = partition_start + args.partition_batch_size - 1
            sub_dir = f"{partition_start:08d}-{partition_end:08d}"
            full_path = path / sub_dir
            full_path.mkdir(parents=True, exist_ok=True)

            write_csv(full_path / trace_file, trace_list, TRACE_HEADER)
            write_csv(full_path / tx_file, tx_list, TX_HEADER)
            write_csv(full_path / block_file, block_list, BLOCK_HEADER)
            write_csv(
                full_path / logs_file,
                logs_list,
                LOGS_HEADER,
                delimiter="|",
                quoting=QUOTE_NONE,
            )

            print(
                f"[{time3}] " f"Exported blocks: {block_range[0]:,}:{block_range[1]:,} "
            )

            block_range = (
                block_id + args.batch_size,
                block_id + args.batch_size + block_bucket_size - 1,
            )
            block_file = "block_%08d-%08d.csv.gz" % block_range
            tx_file = "tx_%08d-%08d.csv.gz" % block_range
            trace_file = "trace_%08d-%08d.csv.gz" % block_range
            logs_file = "logs_%08d-%08d.csv.gz" % block_range

            block_list.clear()
            tx_list.clear()
            trace_list.clear()
            logs_list.clear()

    print(
        f"[{datetime.now()}] Processed block range "
        f"{rounded_start_block:,}:{rounded_end_block:,}"
    )


if __name__ == "__main__":
    main()
