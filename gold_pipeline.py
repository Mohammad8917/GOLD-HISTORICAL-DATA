#!/usr/bin/env python3
"""
GOLD-HISTORICAL-DATA pipeline: XAU/USD tick data from Dukascopy.

Requirements:
  Node.js + npx
  Python 3.10+
  pip install pandas pyarrow

Examples:
  python gold_pipeline.py day 2024-01-02
  python gold_pipeline.py month 2024-01
  python gold_pipeline.py range 2024-01 2024-12
  python gold_pipeline.py verify 2024-01
  python gold_pipeline.py promote 2024-01
  python gold_pipeline.py promote 2024-01 --accept-review
  python gold_pipeline.py promote --all
  python gold_pipeline.py verify-archive 2024-01

All timestamps are UTC. Suspicious ticks are reported, not silently deleted.
This program downloads data only: no broker, exchange, or order execution.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

INSTRUMENT = "xauusd"
SYMBOL = "XAUUSD"
TIMEFRAMES = {"M1": "1min", "M5": "5min", "M15": "15min", "H1": "1h"}

STAGING = Path("data/staging")
ARCHIVE = Path("data/archive")
RAW = STAGING / "raw"
TICKS = STAGING / "ticks"
CANDLES = STAGING / "candles"
REPORTS = STAGING / "reports"

# Screening thresholds; calibrate against real feed history before large runs.
MAX_INVALID_FRAC = 0.001
MAX_CONFLICT_TS_FRAC = 0.001
SPIKE_PCT = 0.5
GAP_INFO_MIN = 10
UNEXPECTED_GAP_MIN = 120
LOW_DAY_FRAC = 0.20
LOW_HOUR_FRAC = 0.20
FRIDAY_CLOSE_UTC_HOUR = 20
SUNDAY_OPEN_UTC_HOUR = 23

OHLC_COLUMNS = [
    f"{side}_{field}"
    for side in ("bid", "ask")
    for field in ("open", "high", "low", "close")
]


def die(message: str) -> None:
    raise RuntimeError(message)


def ensure_dirs() -> None:
    for path in (RAW, TICKS, CANDLES, REPORTS, ARCHIVE):
        path.mkdir(parents=True, exist_ok=True)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        die(f"Cannot read {path}: {exc}")
    if not isinstance(value, dict):
        die(f"Invalid JSON object: {path}")
    return value


def parse_day(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        die(f"Invalid date {value}; expected YYYY-MM-DD.")
    if parsed.isoformat() != value:
        die(f"Invalid date format: {value}")
    return parsed


def month_bounds(value: str) -> tuple[date, date]:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError:
        die(f"Invalid month {value}; expected YYYY-MM.")
    if parsed.strftime("%Y-%m") != value:
        die(f"Invalid month format: {value}")
    start = date(parsed.year, parsed.month, 1)
    end = date(parsed.year + (parsed.month == 12),
               1 if parsed.month == 12 else parsed.month + 1, 1)
    return start, end


def iter_months(first: str, last: str):
    start, _ = month_bounds(first)
    _, last_end = month_bounds(last)
    if start > last_end or first > last:
        die("First month must be earlier than or equal to last month.")
    current = start
    while current < last_end:
        label = current.strftime("%Y-%m")
        _, end = month_bounds(label)
        yield label, current, end
        current = end


def raw_dir_for(label: str) -> Path:
    return RAW / label


def report_path(label: str) -> Path:
    return REPORTS / f"{label}.json"


def download(start: date, end: date, label: str, redo: bool = False) -> Path:
    destination = raw_dir_for(label)
    partial = RAW / f"{label}.partial"

    if redo:
        shutil.rmtree(destination, ignore_errors=True)
        shutil.rmtree(partial, ignore_errors=True)

    marker = destination / ".complete"
    manifest_path = destination / "download_manifest.json"
    if marker.exists() and manifest_path.exists():
        verify_download(destination, start, end)
        print(f"  Reusing verified raw download: {destination}")
        return destination

    # A directory without both completion marker and manifest is never trusted.
    shutil.rmtree(destination, ignore_errors=True)
    shutil.rmtree(partial, ignore_errors=True)

    npx = shutil.which("npx")
    if not npx:
        die("npx not found. Install Node.js and reopen PowerShell.")

    partial.mkdir(parents=True)
    command = [
        npx, "--yes", "dukascopy-node",
        "-i", INSTRUMENT,
        "-from", start.isoformat(),
        "-to", end.isoformat(),
        "-t", "tick",
        "-f", "csv",
        "-dir", str(partial),
    ]
    print("  " + " ".join(command))
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(partial, ignore_errors=True)
        die(f"dukascopy-node failed with exit code {exc.returncode}")

    csv_files = sorted(partial.glob("*.csv"))
    if not csv_files:
        die(f"No CSV files were produced in {partial}")

    entries = []
    for path in csv_files:
        if path.stat().st_size == 0:
            die(f"Empty CSV file: {path.name}")
        try:
            columns = {str(c).strip().lower() for c in pd.read_csv(path, nrows=0).columns}
        except Exception as exc:
            die(f"Cannot read CSV header {path.name}: {exc}")
        if not {"timestamp", "bidprice", "askprice"}.issubset(columns):
            die(f"Unexpected CSV header in {path.name}: {sorted(columns)}")
        entries.append({
            "name": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        })

    partial.replace(destination)
    manifest = {
        "instrument": INSTRUMENT,
        "from_inclusive_utc": start.isoformat(),
        "to_exclusive_utc": end.isoformat(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": entries,
    }
    write_json(destination / "download_manifest.json", manifest)
    (destination / ".complete").write_text("complete\n", encoding="utf-8")
    verify_download(destination, start, end)
    return destination


def verify_download(directory: Path, start: date, end: date) -> None:
    marker = directory / ".complete"
    manifest_file = directory / "download_manifest.json"
    if not marker.is_file() or not manifest_file.is_file():
        die(f"Raw download is not marked complete: {directory}")
    manifest = read_json(manifest_file)
    if manifest.get("from_inclusive_utc") != start.isoformat():
        die("Raw download start-date mismatch.")
    if manifest.get("to_exclusive_utc") != end.isoformat():
        die("Raw download end-date mismatch.")
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        die("Download manifest has no file entries.")
    for item in entries:
        path = directory / item["name"]
        if not path.is_file():
            die(f"Missing raw CSV: {path}")
        if path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
            die(f"Raw CSV integrity check failed: {path}")


def load_ticks(directory: Path) -> pd.DataFrame:
    frames = []
    for path in sorted(directory.glob("*.csv")):
        try:
            frame = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            continue
        if frame.empty:
            continue
        frame.columns = [str(c).strip().lower() for c in frame.columns]
        frame = frame.rename(columns={
            "bidprice": "bid", "askprice": "ask",
            "bidvolume": "bid_vol", "askvolume": "ask_vol",
        })
        if not {"timestamp", "bid", "ask"}.issubset(frame.columns):
            die(f"Unexpected columns in {path.name}: {list(frame.columns)}")
        ts = frame["timestamp"]
        if pd.api.types.is_numeric_dtype(ts):
            frame["time"] = pd.to_datetime(
                pd.to_numeric(ts, errors="coerce"), unit="ms", utc=True, errors="coerce"
            )
        else:
            frame["time"] = pd.to_datetime(ts, utc=True, errors="coerce")
        frame["bid"] = pd.to_numeric(frame["bid"], errors="coerce")
        frame["ask"] = pd.to_numeric(frame["ask"], errors="coerce")
        for column in ("bid_vol", "ask_vol"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frames.append(frame.drop(columns=["timestamp"]))
    if not frames:
        return pd.DataFrame(columns=["time", "bid", "ask"])
    return pd.concat(frames, ignore_index=True)


def weekend_gap(previous: pd.Timestamp, current: pd.Timestamp) -> bool:
    # Approximation only; verify against the provider's actual trading calendar.
    friday_evening = previous.weekday() == 4 and previous.hour >= FRIDAY_CLOSE_UTC_HOUR
    saturday_or_sunday = current.weekday() in (5, 6)
    sunday_open = current.weekday() == 6 and current.hour <= SUNDAY_OPEN_UTC_HOUR
    return bool(friday_evening or saturday_or_sunday or sunday_open)


def clean_ticks(frame: pd.DataFrame, start: date, end: date) -> tuple[pd.DataFrame, dict]:
    report: dict[str, Any] = {
        "rows_raw": int(len(frame)),
        "outside_window": 0,
        "duplicates": 0,
        "invalid": 0,
        "rows_clean": 0,
    }
    if frame.empty:
        return frame, report
    if not {"time", "bid", "ask"}.issubset(frame.columns):
        die("Loaded data is missing time/bid/ask.")

    report["was_sorted"] = bool(frame["time"].is_monotonic_increasing)
    frame = frame.sort_values("time", kind="stable").reset_index(drop=True)
    lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")
    inside = frame["time"].notna() & (frame["time"] >= lo) & (frame["time"] < hi)
    report["outside_window"] = int((~inside).sum())
    frame = frame.loc[inside].copy()

    duplicate = frame.duplicated(keep="first")
    report["duplicates"] = int(duplicate.sum())
    frame = frame.loc[~duplicate].copy()

    finite = (
        frame["bid"].map(lambda x: math.isfinite(float(x)) if pd.notna(x) else False)
        & frame["ask"].map(lambda x: math.isfinite(float(x)) if pd.notna(x) else False)
    )
    invalid = ~finite | (frame["bid"] <= 0) | (frame["ask"] <= 0)
    report["invalid"] = int(invalid.sum())
    frame = frame.loc[~invalid].sort_values("time", kind="stable").reset_index(drop=True)
    report["rows_clean"] = int(len(frame))
    if frame.empty:
        return frame, report

    report["first_tick_utc"] = frame["time"].iloc[0].isoformat()
    report["last_tick_utc"] = frame["time"].iloc[-1].isoformat()

    same = frame[frame["time"].duplicated(keep=False)]
    conflicts = 0
    if not same.empty:
        grouped = same.groupby("time")[["bid", "ask"]].nunique()
        conflicts = int(((grouped["bid"] > 1) | (grouped["ask"] > 1)).sum())
    unique_times = max(int(frame["time"].nunique()), 1)
    report["conflicting_timestamp_groups"] = conflicts
    report["conflicting_timestamps_frac"] = conflicts / unique_times
    report["crossed_bid_gt_ask"] = int((frame["bid"] > frame["ask"]).sum())

    mid = (frame["bid"] + frame["ask"]) / 2
    spikes = mid.pct_change().abs().mul(100) > SPIKE_PCT
    report["spikes"] = int(spikes.sum())
    report["spike_examples_utc"] = frame.loc[spikes, "time"].head(20).astype(str).tolist()

    delta = frame["time"].diff().dt.total_seconds()
    gap_indices = delta[delta > GAP_INFO_MIN * 60].index
    unexpected = []
    weekend_count = 0
    for index in gap_indices:
        previous, current = frame.loc[index - 1, "time"], frame.loc[index, "time"]
        seconds = float(delta.loc[index])
        if weekend_gap(previous, current):
            weekend_count += 1
        elif seconds > UNEXPECTED_GAP_MIN * 60:
            unexpected.append({
                "after_utc": previous.isoformat(),
                "before_utc": current.isoformat(),
                "seconds": seconds,
            })
    report["gaps"] = {
        "count_over_info_threshold": int(len(gap_indices)),
        "weekend_approximation_count": weekend_count,
        "unexpected_threshold_min": UNEXPECTED_GAP_MIN,
        "unexpected_count": len(unexpected),
        "largest_unexpected": sorted(unexpected, key=lambda x: x["seconds"], reverse=True)[:10],
    }

    daily = frame.set_index("time").resample("1D").size()
    expected_days = pd.date_range(lo, hi, inclusive="left", freq="D", tz="UTC")
    weekdays = [day for day in expected_days if day.weekday() < 5]
    weekday_counts = daily[daily.index.weekday < 5]
    median_day = float(weekday_counts.median()) if len(weekday_counts) else 0.0
    missing_days = [day.isoformat() for day in weekdays if daily.get(day, 0) == 0]
    low_days = [
        day.isoformat() for day, count in weekday_counts.items()
        if median_day > 0 and count < LOW_DAY_FRAC * median_day
    ]
    report["daily_coverage"] = {
        "weekdays_expected": len(weekdays),
        "weekdays_with_ticks": len(weekdays) - len(missing_days),
        "missing_weekdays": missing_days,
        "median_ticks_per_weekday": median_day,
        "low_days": low_days,
    }

    hourly = frame.set_index("time").resample("1h").size()
    weekday_hours = hourly[hourly.index.weekday < 5]
    median_hour = float(weekday_hours.median()) if len(weekday_hours) else 0.0
    low_hours = [
        {"hour_utc": hour.isoformat(), "ticks": int(count)}
        for hour, count in weekday_hours.items()
        if median_hour > 0 and count < LOW_HOUR_FRAC * median_hour
    ]
    report["hourly_coverage"] = {
        "median_ticks_per_weekday_hour": median_hour,
        "low_hour_count": len(low_hours),
        "low_hour_examples": low_hours[:40],
    }

    spread = frame["ask"] - frame["bid"]
    report["spread"] = {
        "median": float(spread.median()),
        "p99": float(spread.quantile(0.99)),
        "max": float(spread.max()),
    }
    return frame, report


def build_candles(ticks: pd.DataFrame, rule: str) -> pd.DataFrame:
    indexed = ticks.set_index("time")
    options = {"label": "left", "closed": "left"}
    parts = []
    for side in ("bid", "ask"):
        values = indexed[side].resample(rule, **options).ohlc()
        values.columns = [f"{side}_{column}" for column in values.columns]
        parts.append(values)
    spread = (indexed["ask"] - indexed["bid"]).resample(rule, **options).agg(["mean", "max"])
    spread.columns = ["spread_mean", "spread_max"]
    parts.extend([
        spread,
        indexed["bid"].resample(rule, **options).count().rename("ticks"),
    ])
    result = pd.concat(parts, axis=1)
    return result[result["ticks"] > 0].reset_index()


def aggregate_m1(m1: pd.DataFrame, rule: str) -> pd.DataFrame:
    indexed = m1.set_index("time")
    aggregation = {}
    for side in ("bid", "ask"):
        aggregation[f"{side}_open"] = "first"
        aggregation[f"{side}_high"] = "max"
        aggregation[f"{side}_low"] = "min"
        aggregation[f"{side}_close"] = "last"
    return indexed.resample(rule, label="left", closed="left").agg(aggregation).dropna()


def rollup_mismatches(m1: pd.DataFrame, direct: pd.DataFrame, rule: str) -> int:
    if m1.empty or direct.empty:
        return 1
    rolled = aggregate_m1(m1, rule)
    target = direct.set_index("time")[OHLC_COLUMNS]
    extra = len(rolled.index.difference(target.index))
    aligned = rolled.reindex(target.index)
    bad = (
        (aligned[OHLC_COLUMNS] - target).abs().gt(1e-9).any(axis=1)
        | aligned[OHLC_COLUMNS].isna().any(axis=1)
    )
    return int(bad.sum()) + int(extra)


def evaluate(report: dict) -> list[str]:
    reasons = []
    if report.get("rows_clean", 0) <= 0:
        return ["No valid ticks after cleaning."]
    invalid_fraction = report.get("invalid", 0) / max(report.get("rows_raw", 0), 1)
    if invalid_fraction > MAX_INVALID_FRAC:
        reasons.append("Invalid-row fraction exceeds threshold.")
    if report.get("conflicting_timestamps_frac", 0) > MAX_CONFLICT_TS_FRAC:
        reasons.append("Conflicting timestamp groups exceed threshold.")
    if report.get("crossed_bid_gt_ask", 0):
        reasons.append("Crossed bid/ask quotes detected.")
    if report.get("spikes", 0):
        reasons.append("Possible price spikes detected; inspect examples.")
    if report.get("gaps", {}).get("unexpected_count", 0):
        reasons.append("Unexpected gaps detected outside approximate weekend closure.")
    coverage = report.get("daily_coverage", {})
    if coverage.get("missing_weekdays"):
        reasons.append("Weekdays with no ticks detected.")
    if coverage.get("low_days"):
        reasons.append("Weekdays with unusually low tick counts.")
    if report.get("hourly_coverage", {}).get("low_hour_count", 0):
        reasons.append("Hours with unusually low tick counts.")
    if any(report.get("m1_rollup_mismatches", {}).values()):
        reasons.append("M1 roll-up does not match direct higher-timeframe candles.")
    return reasons


def process(start: date, end: date, label: str, redo: bool = False) -> None:
    ensure_dirs()
    if (ARCHIVE / label).exists():
        print(f"[{label}] Already archived; skipping.")
        return

    rp = report_path(label)
    if redo:
        for path in (raw_dir_for(label), RAW / f"{label}.partial"):
            shutil.rmtree(path, ignore_errors=True)
        (TICKS / f"{SYMBOL}_ticks_{label}.parquet").unlink(missing_ok=True)
        for name in TIMEFRAMES:
            (CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet").unlink(missing_ok=True)
        rp.unlink(missing_ok=True)
    elif rp.exists():
        print(f"[{label}] Already processed; use --redo to rebuild staging.")
        return

    print(f"\n[{label}] {start} to {end} (end exclusive, UTC)")
    raw_directory = download(start, end, label)
    ticks, report = clean_ticks(load_ticks(raw_directory), start, end)

    report.update({
        "label": label,
        "instrument": INSTRUMENT,
        "symbol": SYMBOL,
        "timezone": "UTC",
        "raw_csv_bytes": sum(p.stat().st_size for p in raw_directory.glob("*.csv")),
        "raw_csv_sha256": {p.name: sha256(p) for p in sorted(raw_directory.glob("*.csv"))},
    })

    if not ticks.empty:
        tick_path = TICKS / f"{SYMBOL}_ticks_{label}.parquet"
        tick_path.parent.mkdir(parents=True, exist_ok=True)
        ticks.to_parquet(tick_path, compression="zstd", index=False)
        report["ticks_parquet_bytes"] = tick_path.stat().st_size
        report["ticks_parquet_sha256"] = sha256(tick_path)

        candles = {}
        for name, rule in TIMEFRAMES.items():
            candle = build_candles(ticks, rule)
            candles[name] = candle
            path = CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            candle.to_parquet(path, compression="zstd", index=False)

        report["candle_rows"] = {name: int(len(value)) for name, value in candles.items()}
        report["candle_sha256"] = {
            name: sha256(CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet")
            for name in TIMEFRAMES
        }
        report["m1_rollup_mismatches"] = {
            name: rollup_mismatches(candles["M1"], candles[name], TIMEFRAMES[name])
            for name in ("M5", "M15", "H1")
        }
    else:
        report["candle_rows"] = {}
        report["candle_sha256"] = {}
        report["m1_rollup_mismatches"] = {}

    report["reasons"] = evaluate(report)
    report["status"] = "pass" if not report["reasons"] else "review"
    write_json(rp, report)

    print(f"Status: {report['status']}")
    print(f"Ticks: {report['rows_clean']:,}; duplicates: {report['duplicates']}; invalid: {report['invalid']}")
    for reason in report["reasons"]:
        print(f"  REVIEW: {reason}")
    print(f"Report: {rp}")


def verify_staging(label: str) -> bool:
    rp = report_path(label)
    if not rp.is_file():
        print(f"[{label}] Staging report not found.")
        return False
    report = read_json(rp)
    if report.get("rows_clean", 0) <= 0:
        print(f"[{label}] No valid ticks.")
        return False

    tick_path = TICKS / f"{SYMBOL}_ticks_{label}.parquet"
    if not tick_path.is_file() or sha256(tick_path) != report.get("ticks_parquet_sha256"):
        print(f"[{label}] Tick file missing or checksum mismatch.")
        return False
    try:
        if pq.ParquetFile(tick_path).metadata.num_rows != report["rows_clean"]:
            print(f"[{label}] Tick row count mismatch.")
            return False
    except Exception as exc:
        print(f"[{label}] Tick Parquet unreadable: {exc}")
        return False

    for name in TIMEFRAMES:
        path = CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet"
        if not path.is_file() or sha256(path) != report.get("candle_sha256", {}).get(name):
            print(f"[{label}] {name} candle file missing or checksum mismatch.")
            return False
        try:
            if pq.ParquetFile(path).metadata.num_rows != report["candle_rows"][name]:
                print(f"[{label}] {name} candle row count mismatch.")
                return False
        except Exception as exc:
            print(f"[{label}] {name} Parquet unreadable: {exc}")
            return False

    print(f"[{label}] Staging integrity PASSED; quality status={report.get('status')}.")
    for reason in report.get("reasons", []):
        print(f"  REVIEW: {reason}")
    return True


def promote(label: str, accept_review: bool = False) -> bool:
    if not verify_staging(label):
        return False
    rp = report_path(label)
    report = read_json(rp)
    if report.get("status") != "pass" and not accept_review:
        print(f"[{label}] Status is {report.get('status')}; promotion refused.")
        print("Manual review is required before --accept-review.")
        return False
    if report.get("status") not in ("pass", "review"):
        print(f"[{label}] Unknown status; promotion refused.")
        return False

    final_dir = ARCHIVE / label
    temp_dir = ARCHIVE / f".{label}.publishing"
    if final_dir.exists() or temp_dir.exists():
        print(f"[{label}] Archive destination or publishing directory already exists.")
        print("Inspect it manually; nothing was overwritten.")
        return False

    try:
        temp_dir.mkdir(parents=True)
        shutil.copy2(TICKS / f"{SYMBOL}_ticks_{label}.parquet", temp_dir / "ticks.parquet")
        candle_dir = temp_dir / "candles"
        candle_dir.mkdir()
        for name in TIMEFRAMES:
            source = CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet"
            shutil.copy2(source, candle_dir / f"{name}.parquet")

        copied = [temp_dir / "ticks.parquet"] + [
            candle_dir / f"{name}.parquet" for name in TIMEFRAMES
        ]
        file_manifest = {}
        for path in copied:
            metadata = pq.ParquetFile(path).metadata
            file_manifest[str(path.relative_to(temp_dir))] = {
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
                "rows": metadata.num_rows,
            }

        if report["status"] == "review":
            report["accepted_after_review"] = True
        report["promoted_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_json(temp_dir / "report.json", report)
        write_json(temp_dir / "manifest.json", {
            "label": label,
            "instrument": INSTRUMENT,
            "symbol": SYMBOL,
            "published_at_utc": datetime.now(timezone.utc).isoformat(),
            "quality_status": report["status"],
            "files": file_manifest,
        })

        # Directory rename is atomic when source and destination are on same filesystem.
        temp_dir.replace(final_dir)
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"[{label}] Promotion failed: {exc}")
        return False

    # Keep staging until publication has succeeded.
    for path in (
        TICKS / f"{SYMBOL}_ticks_{label}.parquet",
        rp,
    ):
        path.unlink(missing_ok=True)
    for name in TIMEFRAMES:
        (CANDLES / name / f"{SYMBOL}_{name}_{label}.parquet").unlink(missing_ok=True)

    print(f"[{label}] Published successfully: {final_dir}")
    return True


def verify_archive(label: str) -> bool:
    directory = ARCHIVE / label
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        print(f"[{label}] Archive manifest missing.")
        return False
    manifest = read_json(manifest_path)
    for relative, info in manifest.get("files", {}).items():
        path = directory / relative
        if not path.is_file():
            print(f"[{label}] Missing archive file: {relative}")
            return False
        if path.stat().st_size != info["bytes"] or sha256(path) != info["sha256"]:
            print(f"[{label}] Archive integrity failure: {relative}")
            return False
        try:
            if pq.ParquetFile(path).metadata.num_rows != info["rows"]:
                print(f"[{label}] Archive row-count mismatch: {relative}")
                return False
        except Exception as exc:
            print(f"[{label}] Invalid Parquet {relative}: {exc}")
            return False
    print(f"[{label}] Archive verification PASSED.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    p = commands.add_parser("day")
    p.add_argument("date")
    p.add_argument("--redo", action="store_true")

    p = commands.add_parser("month")
    p.add_argument("month")
    p.add_argument("--redo", action="store_true")

    p = commands.add_parser("range")
    p.add_argument("first")
    p.add_argument("last")
    p.add_argument("--redo", action="store_true")

    p = commands.add_parser("verify")
    p.add_argument("label")

    p = commands.add_parser("verify-archive")
    p.add_argument("label")

    p = commands.add_parser("promote")
    p.add_argument("label", nargs="?")
    p.add_argument("--all", action="store_true")
    p.add_argument("--accept-review", action="store_true")

    args = parser.parse_args()
    ensure_dirs()

    if args.command == "day":
        start = parse_day(args.date)
        process(start, start + timedelta(days=1), args.date, args.redo)
    elif args.command == "month":
        start, end = month_bounds(args.month)
        process(start, end, args.month, args.redo)
    elif args.command == "range":
        for label, start, end in iter_months(args.first, args.last):
            process(start, end, label, args.redo)
    elif args.command == "verify":
        if not verify_staging(args.label):
            sys.exit(1)
    elif args.command == "verify-archive":
        if not verify_archive(args.label):
            sys.exit(1)
    elif args.command == "promote":
        if args.all:
            if args.accept_review:
                parser.error("--accept-review cannot be combined with --all.")
            for path in sorted(REPORTS.glob("*.json")):
                promote(path.stem)
        elif args.label:
            if not promote(args.label, args.accept_review):
                sys.exit(1)
        else:
            parser.error("Provide a label or use --all.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
