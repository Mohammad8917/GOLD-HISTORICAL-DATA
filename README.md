# GOLD-HISTORICAL-DATA

Independent historical market-data archive for **XAU/USD (Gold)**.

This repository is separate from the GOLD signal-generation project. It stores historical price data and reproducible data-quality reports only. It does not connect to a broker or exchange and does not execute trades.

## Directory layout

- `XAUUSD_M1/` — one-minute bars
- `XAUUSD_M5/` — five-minute bars
- `XAUUSD_M15/` — fifteen-minute bars
- `XAUUSD_H1/` — hourly bars
- `quality_reports/` — validation reports, coverage summaries, and checksums

## Data admission rules

Do not treat a dataset as verified merely because a provider advertises its coverage or quality. Before accepting data, record:

1. Original provider and exact download URL or method.
2. Instrument and feed identity (XAU/USD spot, CFD, or another contract); never imply that different feeds are identical.
3. Timeframe, timezone, timestamp convention (bar open or bar close), date range, and download date.
4. File format, row count, file size, and SHA-256 checksum.
5. Missing/duplicate timestamps, ordering, interval gaps, invalid OHLC values, zero/flat bars, and any known session/weekend behavior.
6. Whether higher timeframes are provider-native or aggregated from lower-timeframe bars.

Keep raw provider files unchanged. Put normalized or derived files in clearly named subfolders and document the transformation. Never silently fill gaps or remove suspicious rows.

## Recommended CSV columns

At minimum:

`timestamp,open,high,low,close`

Include `volume` only when its meaning is documented by the provider. Preserve bid/ask fields when supplied. Use ISO-8601 timestamps with an explicit timezone (prefer UTC) and document whether timestamps mark bar opening or closing.

## Quality reports

For each dataset, add a report under `quality_reports/` containing the metadata above, validation results, known limitations, and checksum. A passing structural check does not prove that prices match a universal or centralized gold market; XAU/USD is a distributed OTC market and feeds can differ.

## Current status

The repository currently contains placeholders only. **No historical market-data files have been downloaded, inspected, or verified in this repository yet.**

## Safety and scope

This repository is an archival and research resource. It is not a trading system, broker connection, investment recommendation, or guarantee of future signal accuracy.
