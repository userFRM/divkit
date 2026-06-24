"""Tests for the validate subcommand of divkit-builder."""

from __future__ import annotations

import json
import pytest

from divkit_builder.frames import Row
from divkit_builder import schema as _schema
from divkit_builder.build import run_validate


_ROWS = [
    Row(
        cik=1,
        period_start="2024-01-01",
        period_end="2024-03-31",
        amount=0.50,
        concept="Declared",
        accn="0001-01-2024",
        form="10-Q",
    ),
    Row(
        cik=2,
        period_start="2024-04-01",
        period_end="2024-06-30",
        amount=0.75,
        concept="CashPaid",
        accn="0002-02-2024",
        form=None,
    ),
]

_CIK_TICKER = {1: "AAPL", 2: "MSFT"}


def _write_good_data(out_dir: str) -> None:
    """Write valid shards + manifest to *out_dir*."""
    _schema.write_year_shards(_ROWS, _CIK_TICKER, out_dir)
    _schema.write_manifest(out_dir)


def test_validate_passes_on_good_data(tmp_path):
    """Good shards + correct manifest exits cleanly (no SystemExit)."""
    _write_good_data(str(tmp_path))
    # Must not raise
    run_validate(str(tmp_path))


def test_validate_fails_on_tampered_manifest(tmp_path):
    """A manifest with a wrong digest causes SystemExit(1)."""
    _write_good_data(str(tmp_path))

    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path) as fh:
        data = json.load(fh)

    # Corrupt the first entry's digest
    first_key = next(iter(data))
    data[first_key] = "sha256:" + "0" * 64

    with open(manifest_path, "w") as fh:
        json.dump(data, fh)

    with pytest.raises(SystemExit) as exc_info:
        run_validate(str(tmp_path))
    assert exc_info.value.code != 0


def test_validate_fails_on_missing_manifest(tmp_path):
    """Missing manifest.json causes SystemExit(1)."""
    _schema.write_year_shards(_ROWS, _CIK_TICKER, str(tmp_path))
    # Deliberately do NOT write manifest

    with pytest.raises(SystemExit) as exc_info:
        run_validate(str(tmp_path))
    assert exc_info.value.code != 0


def test_validate_fails_when_file_absent_from_manifest(tmp_path):
    """A parquet file not mentioned in manifest.json causes SystemExit(1)."""
    _write_good_data(str(tmp_path))

    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path) as fh:
        data = json.load(fh)

    # Remove the first entry so one file is unrecorded
    first_key = next(iter(data))
    del data[first_key]

    with open(manifest_path, "w") as fh:
        json.dump(data, fh)

    with pytest.raises(SystemExit) as exc_info:
        run_validate(str(tmp_path))
    assert exc_info.value.code != 0


def test_validate_empty_dir_passes(tmp_path):
    """An empty data directory (no parquet files) is considered valid."""
    # Still needs a manifest-less directory to be handled gracefully
    run_validate(str(tmp_path))
