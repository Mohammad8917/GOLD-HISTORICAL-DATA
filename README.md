# GOLD-HISTORICAL-DATA

Historical market-data pipeline and archive for **XAU/USD (Gold)** using the Dukascopy feed.

> **Scope:** this repository downloads and validates historical market data. It does not connect to a broker or exchange account, place orders, or generate trading signals. Keep it separate from the GOLD signal-generation project.

## 1. What the pipeline does

`gold_pipeline.py`:

1. Downloads Dukascopy tick CSV files through `dukascopy-node`.
2. Preserves the downloaded provider CSVs and records file sizes and SHA-256 hashes.
3. Parses timestamps as UTC, restricts rows to the requested half-open date range, and checks quote validity.
4. Reports duplicate rows, invalid timestamps/prices, crossed bid/ask quotes, conflicting timestamps, possible price spikes, and time gaps.
5. Builds bid/ask OHLC candles for M1, M5, M15 and H1 from the tick feed.
6. Compares M5/M15/H1 OHLC values with rollups from M1.
7. Writes a machine-readable quality report and supports staging verification and controlled local promotion to the archive.

A successful download, a successful staging-integrity check, and a quality `pass` are three different things. Do not treat them as interchangeable.

## 2. Requirements

- Windows 11 / PowerShell is supported for the current operator workflow.
- Python 3.10 or newer (the current development environment is Python 3.13).
- Node.js and npm/npx.
- Python packages: `pandas`, `pyarrow`.

Install the Python dependencies:

```powershell
python -m pip install --upgrade pandas pyarrow
```

Check the tools:

```powershell
python --version
node --version
npx --version
npx --yes dukascopy-node --help
```

## 3. Commands

Run commands from the repository root. Dates and all pipeline boundaries are interpreted in **UTC**. The requested end date is exclusive internally.

```powershell
# One UTC calendar day
python .\\gold_pipeline.py day 2026-11-11

# One month
python .\\gold_pipeline.py month 2026-11

# Inclusive range of months (both endpoints included)
python .\\gold_pipeline.py range 2026-01 2026-03

# Rebuild an existing staging batch from a fresh download
python .\\gold_pipeline.py day 2026-11-11 --redo

# Verify staging file integrity (does not approve data quality)
python .\\gold_pipeline.py verify 2026-11-11

# Verify a published archive batch
python .\\gold_pipeline.py verify-archive 2026-11-11

# Promote only a batch whose report status is pass
python .\\gold_pipeline.py promote 2026-11-11

# Promote all eligible pass batches
python .\\gold_pipeline.py promote --all
```

The script also exposes `promote LABEL --accept-review`. This is an explicit manual override: it can publish a batch with quality status `review`. Use it only after recording who reviewed the report, why each warning is acceptable, and the evidence used. Never use `--accept-review` as a shortcut to make a batch appear approved.

## 4. Dukascopy XAU/USD session calendar

### Regular weekly schedule

The current code models the regular XAU/USD session in the IANA timezone **`America/New_York`**:

- Sunday: opens at 18:00 New York time.
- Monday–Thursday: trades 18:00–17:00 next day, with a daily maintenance break from 17:00 to 18:00.
- Friday: closes at 17:00 New York time.
- Saturday: closed.
- Sunday before 18:00 New York time: closed.

The code converts this calendar to UTC using the timezone database. Do **not** hard-code a permanent UTC close/open hour: the New York daylight-saving transition changes the UTC mapping. The EU and US DST transition dates are not always the same, so Berlin local time must not be used to calculate the provider session.

For the regular daily maintenance break, the expected closed UTC hour is generally 22:00–23:00 in US winter time and 21:00–22:00 in US summer time. The Sunday open and Friday close also move with US DST.

### Holidays, early closes and exceptional closures — important limitation

**The current pipeline does not yet ingest or apply a dated Dukascopy holiday/early-close calendar.** Its report explicitly marks `holiday_schedule_applied: false`. The regular weekly calendar is therefore not a complete holiday calendar.

Dukascopy's published instrument-hours notice says XAU/USD has additional non-tradable hours on US national holidays: 17:00–21:00 during US summer time and 18:00–22:00 during US winter time. Dukascopy also publishes date-specific notices for holidays and special market breaks. These exceptions are not equivalent to simply marking the whole US public holiday as closed.

Operational rules until a verified holiday feed/override implementation exists:

1. Before processing a date range, check Dukascopy's current **Trading Hours / Trading Breaks** information and its dated market-closure notices for every holiday in that range.
2. Record the exact instrument (XAU/USD), date, timezone, closure start/end, source URL, retrieval date, and whether the notice describes a full closure, a shortened session, or reduced liquidity.
3. If the provider publishes no exact schedule for a date, do not invent one from a generic US bank-holiday list. Mark the affected batch `review` and investigate its observed ticks.
4. Do not reinterpret missing ticks during an unconfigured holiday as a scheduled closure. The current code may flag those hours; this is safer than silently masking a real data outage.
5. Do not promote a holiday-affected batch until the schedule has been independently checked and the report warning has been resolved or explicitly accepted with evidence.
6. Keep reduced-liquidity periods distinct from full closures. Low tick counts or wide spreads are not proof that a market was closed.

Official provider references (check the live pages before each relevant historical run; schedules can be updated):

- Dukascopy instrument trading hours and regular XAU/USD breaks: https://www.dukascopy.com/swiss/english/forex/forex-trading-accounts/link/
- Dukascopy dated market notices and DST announcements: https://www.dukascopy.com/swiss/english/about/ournews/
- Dukascopy Europe notices (additional dated holiday notices): https://www.dukascopy.com/europe/deutsch/about/ournews/

A future holiday-calendar implementation should use a version-controlled, source-attributed exception file with explicit UTC intervals (or timezone + local intervals), support full closures and early closes separately, reject overlapping/conflicting entries, record the calendar version/hash in each report, and have tests for holiday boundaries and DST. It must not infer XAU/USD closures solely from generic stock-exchange, futures, or bank holiday calendars.

## 5. Data quality: what the statuses mean

### `pass`

All checks currently implemented by this version passed the configured thresholds. This is a structural screening result, **not** proof that every expected market tick exists or that the prices match another broker, venue, or consolidated gold benchmark.

### `review`

At least one configured check raised a warning. Inspect the complete JSON report and the corresponding raw observations. Do not promote automatically.

### Staging integrity

`verify LABEL` checks that expected Parquet files exist, their hashes match the report, and their row counts are readable and consistent. It prints the separate quality status. **Staging integrity PASSED does not mean quality status is pass.**

### Checks currently reported

- Invalid timestamps and non-positive/non-finite bid/ask prices.
- Exact duplicate rows and conflicting timestamp groups.
- Crossed quotes (bid greater than ask).
- Large tick-to-tick mid-price changes (possible spikes).
- Long gaps between observed ticks, classified against the regular session calendar.
- Expected active trading-hour/day coverage, including missing and unusually low-count periods.
- Spread median, 99th percentile and maximum.
- Parquet file readability, row counts and SHA-256 integrity.
- M1-to-M5/M15/H1 OHLC rollup consistency.

Thresholds in the source are screening defaults, not calibrated universal market rules. Review and version any threshold changes; do not loosen them merely to obtain a `pass`.

### Interpretation safeguards

- Dukascopy is one specific OTC/provider feed. XAU/USD is not one centralized exchange-traded market.
- A provider tick feed is not a consolidated market tape and may differ from another broker's quote stream.
- Wide spreads are retained and reported, not silently deleted.
- Suspicious ticks are reported; the pipeline must not silently interpolate, forward-fill, or fabricate observations.
- Higher-timeframe candles are derived from the downloaded ticks. They are not independent confirmation from a second data source.
- A missing scheduled trading hour must remain visible. A daily maintenance break or weekend must never hide a missing open-session hour.
- A zero-mismatch rollup validates internal aggregation consistency only; it does not validate the source prices.
- A partial final candle at the requested range boundary is possible. Interpret it using the UTC range and observed tick coverage, not as a complete interval by default.

## 6. Local output layout

The current script writes working data under:

- `data/staging/raw/<label>/` — provider CSV files, download manifest and completion marker.
- `data/staging/ticks/` — cleaned tick Parquet files.
- `data/staging/candles/M1/`, `M5/`, `M15/`, `H1/` — derived bid/ask candles.
- `data/staging/reports/<label>.json` — quality report.
- `data/archive/<label>/` — published batch with `ticks.parquet`, `candles/*.parquet`, `report.json` and `manifest.json`.

The top-level `XAUUSD_M1/`, `XAUUSD_M5/`, `XAUUSD_M15/`, `XAUUSD_H1/` and `quality_reports/` directories are placeholders; this pipeline version does not populate them.

Keep raw files and manifests. Do not edit archived Parquet files manually. If a batch needs correction, rebuild it in staging and preserve the previous report and review history before replacing any approved dataset.

## 7. Batch review and promotion checklist

For every batch:

- [ ] Confirm instrument is XAU/USD and the requested UTC start/end boundaries are correct.
- [ ] Confirm raw download manifest exists and every CSV checksum verifies.
- [ ] Confirm tick count, first/last tick timestamps and actual covered interval.
- [ ] Inspect invalid rows, duplicates, conflicting timestamps and crossed quotes.
- [ ] Inspect every spike example and every unexpected/long gap.
- [ ] Review expected trading hours and days; investigate every missing or low-coverage active period.
- [ ] Check provider holiday notices and special trading breaks for the entire date range.
- [ ] Inspect spread distribution and extreme spread observations.
- [ ] Confirm M1/M5/M15/H1 candle row counts are plausible for the actual open session.
- [ ] Confirm all M1 rollup mismatch counts are zero.
- [ ] Run `verify LABEL` and separately confirm report status is `pass`, or document a formal manual review.
- [ ] Only then promote. After promotion, run `verify-archive LABEL`.

Never promote a batch just because its Parquet checksums are correct.

## 8. Known limitations / work still required

- No automatic Dukascopy holiday, early-close or exceptional-session calendar ingestion yet.
- No automatic provider-outage detection independent of tick-gap/coverage checks.
- No cross-provider price comparison or independent benchmark validation.
- No corporate-action concept is relevant to spot gold, but provider symbol/specification changes still need provenance review.
- Screening thresholds need calibration against a longer, manually reviewed sample across normal sessions, DST transitions, holidays and volatile periods.
- Automated tests and CI must be run and kept green on the supported Python versions before treating a code change as validated.
- The current archive is local to the machine unless the operator separately backs it up. Do not commit raw market data or credentials to GitHub.

## 9. Current repository/data status

The session-calendar changes are being reviewed on the feature branch `fix/session-aware-quality-calendar` and draft PR #1. They are not considered production-approved until the automated tests run successfully and the sample batches are reprocessed and reviewed.

Previously reviewed local samples (11–13 November 2026) are operator-reported test results, not archived datasets in this repository. No data batch should be described as published unless the archive directory and its manifest are actually present and verified.

## 10. Safety and scope

This is a historical-data collection and research utility only. It is not a trading system, investment recommendation, broker connection, or guarantee of data completeness, execution quality, or future performance.
