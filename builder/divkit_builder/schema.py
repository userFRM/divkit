"""Parquet year-sharding and SHA-256 manifest writing for dividend rows."""

from __future__ import annotations

import datetime
import glob
import hashlib
import json
import os
from collections import defaultdict
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from .frames import Row, _merge_prefer_declared

# ---------------------------------------------------------------------------
# Arrow schema — exact column order and types per spec
# ---------------------------------------------------------------------------
_SCHEMA = pa.schema([
    pa.field("cik", pa.uint32(), nullable=False),
    pa.field("ticker", pa.string(), nullable=True),
    pa.field("period_start", pa.date32(), nullable=False),
    pa.field("period_end", pa.date32(), nullable=False),
    pa.field("amount", pa.float64(), nullable=False),
    pa.field("concept", pa.string(), nullable=False),
    pa.field("accn", pa.string(), nullable=False),
    pa.field("form", pa.string(), nullable=True),
])


def _date_from_str(s: str) -> datetime.date:
    """Parse "YYYY-MM-DD" → datetime.date."""
    return datetime.date(int(s[:4]), int(s[5:7]), int(s[8:10]))


def write_year_shards(
    rows: Iterable[Row],
    cik_ticker: dict[int, str],
    out_dir: str,
) -> list[str]:
    """Deduplicate, join tickers, group by period_end year, write parquet shards.

    Returns sorted list of written file paths.
    """
    all_rows: list[Row] = list(rows)

    # Dedup by (cik, period_end) preferring Declared
    deduped = _merge_prefer_declared(all_rows)

    # Sort by (cik, period_end) before grouping/writing
    deduped.sort(key=lambda r: (r.cik, r.period_end))

    # Group by year of period_end
    by_year: dict[int, list[Row]] = defaultdict(list)
    for row in deduped:
        year = int(row.period_end[:4])
        by_year[year].append(row)

    os.makedirs(out_dir, exist_ok=True)
    written: list[str] = []

    for year in sorted(by_year):
        year_rows = by_year[year]

        # Build column arrays explicitly — no getattr shortcuts
        cik_arr = pa.array([r.cik for r in year_rows], type=pa.uint32())
        ticker_arr = pa.array(
            [cik_ticker.get(r.cik) for r in year_rows],
            type=pa.string(),
        )
        period_start_arr = pa.array(
            [_date_from_str(r.period_start) for r in year_rows],
            type=pa.date32(),
        )
        period_end_arr = pa.array(
            [_date_from_str(r.period_end) for r in year_rows],
            type=pa.date32(),
        )
        amount_arr = pa.array([r.amount for r in year_rows], type=pa.float64())
        concept_arr = pa.array([r.concept for r in year_rows], type=pa.string())
        accn_arr = pa.array([r.accn for r in year_rows], type=pa.string())
        form_arr = pa.array([r.form for r in year_rows], type=pa.string())

        table = pa.table(
            {
                "cik": cik_arr,
                "ticker": ticker_arr,
                "period_start": period_start_arr,
                "period_end": period_end_arr,
                "amount": amount_arr,
                "concept": concept_arr,
                "accn": accn_arr,
                "form": form_arr,
            },
            schema=_SCHEMA,
        )

        path = os.path.join(out_dir, f"dividends-{year}.parquet")
        pq.write_table(table, path)
        written.append(path)

    return sorted(written)


def write_manifest(out_dir: str) -> None:
    """SHA-256 each dividends-*.parquet in out_dir and write manifest.json.

    Output is a flat JSON object mapping filename → "sha256:<hexdigest>".
    The Rust fetcher reads this as HashMap<String,String> and strips the prefix.
    """
    pattern = os.path.join(out_dir, "dividends-*.parquet")
    parquet_files = sorted(glob.glob(pattern))

    manifest: dict[str, str] = {}
    for path in parquet_files:
        filename = os.path.basename(path)
        digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
        manifest[filename] = f"sha256:{digest}"

    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
