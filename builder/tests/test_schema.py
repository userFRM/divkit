"""Tests for schema.write_year_shards and schema.write_manifest."""

from __future__ import annotations

import datetime
import hashlib
import json
import os

import pyarrow.parquet as pq

from divkit_builder.frames import Row
from divkit_builder import schema


def _make_rows() -> list[Row]:
    """4 rows spanning 2 years; row3 is a duplicate of row1 by (cik, period_end) — CashPaid loses."""
    return [
        # year 2023
        Row(cik=1, period_start="2023-01-01", period_end="2023-03-31",
            amount=0.50, concept="Declared", accn="0001-01-2023", form="10-Q"),
        Row(cik=2, period_start="2023-04-01", period_end="2023-06-30",
            amount=0.25, concept="CashPaid", accn="0002-02-2023", form=None),
        # year 2024
        Row(cik=3, period_start="2024-01-01", period_end="2024-03-31",
            amount=1.00, concept="Declared", accn="0003-01-2024", form="10-K"),
        # Duplicate of row1 key (cik=1, period_end=2023-03-31) — CashPaid should lose to Declared
        Row(cik=1, period_start="2023-01-01", period_end="2023-03-31",
            amount=0.45, concept="CashPaid", accn="0001-01-2023-dup", form=None),
        # year 2024 row for cik=4 which is unmapped (ticker → null)
        Row(cik=4, period_start="2024-07-01", period_end="2024-09-30",
            amount=0.75, concept="CashPaid", accn="0004-03-2024", form="10-Q"),
    ]


def test_write_year_shards_files_exist(tmp_path):
    rows = _make_rows()
    cik_ticker = {1: "APLE", 2: "BANA", 3: "CHRY"}  # cik=4 intentionally absent
    paths = schema.write_year_shards(rows, cik_ticker, str(tmp_path))

    assert os.path.exists(str(tmp_path / "dividends-2023.parquet")), "2023 shard missing"
    assert os.path.exists(str(tmp_path / "dividends-2024.parquet")), "2024 shard missing"
    assert sorted(paths) == sorted([
        str(tmp_path / "dividends-2023.parquet"),
        str(tmp_path / "dividends-2024.parquet"),
    ])


def test_write_year_shards_schema_and_values(tmp_path):
    rows = _make_rows()
    cik_ticker = {1: "APLE", 2: "BANA", 3: "CHRY"}
    schema.write_year_shards(rows, cik_ticker, str(tmp_path))

    import pyarrow as pa

    # Check 2023 shard
    table_2023 = pq.read_table(str(tmp_path / "dividends-2023.parquet"))
    cols = table_2023.schema.names
    assert cols == ["cik", "ticker", "period_start", "period_end", "amount", "concept", "accn", "form"], \
        f"Column order mismatch: {cols}"

    # Type checks
    s = table_2023.schema
    assert s.field("cik").type == pa.uint32()
    assert s.field("ticker").type == pa.string()
    assert s.field("period_start").type == pa.date32()
    assert s.field("period_end").type == pa.date32()
    assert s.field("amount").type == pa.float64()
    assert s.field("concept").type == pa.string()
    assert s.field("accn").type == pa.string()
    assert s.field("form").type == pa.string()

    # 2023 should have 2 rows (cik=1 Declared wins over CashPaid dup; cik=2)
    assert table_2023.num_rows == 2, f"Expected 2 rows in 2023 shard, got {table_2023.num_rows}"

    d = table_2023.to_pydict()
    # cik=1 row should have Declared + amount 0.50
    idx_cik1 = d["cik"].index(1)
    assert d["amount"][idx_cik1] == 0.50, "Declared should win dedup"
    assert d["ticker"][idx_cik1] == "APLE"
    assert d["concept"][idx_cik1] == "Declared"
    assert d["period_start"][idx_cik1] == datetime.date(2023, 1, 1)
    assert d["period_end"][idx_cik1] == datetime.date(2023, 3, 31)

    # Check 2024 shard — cik=4 has no ticker (should be null)
    table_2024 = pq.read_table(str(tmp_path / "dividends-2024.parquet"))
    assert table_2024.num_rows == 2, f"Expected 2 rows in 2024 shard, got {table_2024.num_rows}"
    d24 = table_2024.to_pydict()
    idx_cik4 = d24["cik"].index(4)
    assert d24["ticker"][idx_cik4] is None, "Unmapped CIK should have null ticker"


def test_write_manifest_flat_format(tmp_path):
    rows = _make_rows()
    cik_ticker = {1: "APLE", 2: "BANA", 3: "CHRY"}
    schema.write_year_shards(rows, cik_ticker, str(tmp_path))
    schema.write_manifest(str(tmp_path))

    manifest_path = tmp_path / "manifest.json"
    assert manifest_path.exists(), "manifest.json not written"

    with open(manifest_path) as f:
        data = json.load(f)

    # Must be a flat dict, not nested
    assert isinstance(data, dict), "manifest must be a flat dict"
    assert "dividends-2023.parquet" in data
    assert "dividends-2024.parquet" in data

    for filename, digest_str in data.items():
        assert isinstance(digest_str, str), f"digest for {filename} must be a string"
        assert digest_str.startswith("sha256:"), f"digest must start with sha256:, got {digest_str!r}"
        hex_part = digest_str[len("sha256:"):]
        # Verify digest against actual file
        file_path = tmp_path / filename
        expected = hashlib.sha256(file_path.read_bytes()).hexdigest()
        assert hex_part == expected, f"digest mismatch for {filename}: got {hex_part}, expected {expected}"
