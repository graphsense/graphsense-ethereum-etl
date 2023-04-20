# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [-] - unreleased
### Removed
- removed deprecated exchange rates scripts
- unused genesis_transfer.csv

## [23.03/1.4.0] - 2023-03-29
### Changed
- Updated ethereum-etl version

## [23.01/1.3.0] - 2023-12-30
### Added
- Standard dev Makefile
### Deprecated
- Coindesk and CoinMarketCap exchange rate script (now in graphsense-lib)

## [22.11] 2022-11-24
### Added
- Added log CSV export and logs ingest

## [22.10] 2022-10-11
### Changed
- Updated Python dependencies
- Set `None` values to `UNSET` to avoid tombstones in Cassandra

## [1.0.1] 2022-08-26
### Changed
- Updated Python dependencies
- Improved `get_last_block_yesterday` function

## [1.0.0] 2022-07-08
### Changed
- Cassandra schema
### Added
- CSV export of `logs`

## [0.5.2] 2022-03-17
### Changed
- Update CSV export/import (new field `trace_index`)
- Updated Python dependencies
- Minor improvements

## [0.5.1] 2021-11-29
### Changed
- Use streaming adapter interface for data ingest into Cassandra
### Added
- Added `trace` table
- CSV export/import scripts

## [0.5.0] 2021-05-31
### Changed
- Initial release
