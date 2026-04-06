#!/usr/bin/env python3
"""Scan parquet files and remove corrupted ones (optionally)."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import sys

import pyarrow.parquet as pq


SNAPSHOT_RE = re.compile(r"^snapshot_(\d{8})_(\d{6})\.parquet$")


def is_parquet_valid(path: Path) -> tuple[bool, str | None]:
    """Validate parquet by opening it and reading all row groups."""
    try:
        parquet_file = pq.ParquetFile(path)

        # Strict validation: read each row group to catch data-page corruption,
        # not only footer/metadata corruption.
        num_row_groups = parquet_file.metadata.num_row_groups
        for row_group_idx in range(num_row_groups):
            parquet_file.read_row_group(row_group_idx)

        return True, None
    except Exception as exc:  # noqa: BLE001 - we want to report any parquet parsing failure
        return False, f"{type(exc).__name__}: {exc}"


def parse_snapshot_timestamp(path: Path) -> datetime | None:
    """Parse timestamp from snapshot filename (snapshot_YYYYMMDD_HHMMSS.parquet)."""
    match = SNAPSHOT_RE.match(path.name)
    if not match:
        return None

    date_part, time_part = match.groups()
    try:
        return datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def scan_and_cleanup(root: Path, delete: bool) -> int:
    parquet_files = sorted(root.rglob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found under: {root}")
        return 0

    checked = 0
    corrupted = 0
    duplicates = 0
    deleted = 0
    corrupted_files: set[Path] = set()
    duplicate_files: set[Path] = set()

    # Detect duplicates by snapshot minute: a file is considered duplicate if it has
    # the same minute as its immediate previous snapshot after timestamp sorting.
    timestamped_files: list[tuple[datetime, Path]] = []
    invalid_name_pattern = 0
    for parquet_path in parquet_files:
        snapshot_dt = parse_snapshot_timestamp(parquet_path)
        if snapshot_dt is None:
            invalid_name_pattern += 1
            continue
        timestamped_files.append((snapshot_dt, parquet_path))

    timestamped_files.sort(key=lambda item: (item[0], item[1].as_posix()))

    previous_minute = None
    previous_path = None
    for snapshot_dt, parquet_path in timestamped_files:
        current_minute = snapshot_dt.strftime("%Y%m%d_%H%M")
        if previous_minute == current_minute:
            duplicates += 1
            duplicate_files.add(parquet_path)
            print(f"DUPLICATE: {parquet_path}")
            print(f"  Reason: same minute as previous snapshot ({previous_path})")
        previous_minute = current_minute
        previous_path = parquet_path

    for parquet_path in parquet_files:
        checked += 1
        ok, error = is_parquet_valid(parquet_path)
        if ok:
            continue

        corrupted += 1
        corrupted_files.add(parquet_path)
        print(f"CORRUPTED: {parquet_path}")
        print(f"  Reason: {error}")

    files_to_delete = sorted(corrupted_files | duplicate_files)
    if delete:
        for parquet_path in files_to_delete:
            try:
                parquet_path.unlink()
                deleted += 1
                print(f"DELETE: {parquet_path}")
            except OSError as delete_error:
                print(f"DELETE FAILED: {parquet_path} ({delete_error})")

    mode = "DELETE" if delete else "DRY-RUN"
    print("\nSummary")
    print(f"  Mode: {mode}")
    print(f"  Root: {root}")
    print(f"  Files checked: {checked}")
    print(f"  Corrupted files: {corrupted}")
    print(f"  Duplicate files: {duplicates}")
    print(f"  Non-standard snapshot names: {invalid_name_pattern}")
    print(f"  Deleted files: {deleted}")

    # In dry-run mode, return non-zero if issues are found to make it CI/cron-friendly.
    if not delete and (corrupted > 0 or duplicates > 0):
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan parquet files and optionally delete corrupted files.",
    )
    parser.add_argument(
        "--root",
        default="/opt/airflow/data_lake/bronze/velib",
        help="Root directory to scan recursively (default: /opt/airflow/data_lake/bronze/velib)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete corrupted parquet files. Without this flag, runs in dry-run mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)

    if not root.exists() or not root.is_dir():
        print(f"Invalid root directory: {root}")
        return 1

    return scan_and_cleanup(root=root, delete=args.delete)


if __name__ == "__main__":
    sys.exit(main())
