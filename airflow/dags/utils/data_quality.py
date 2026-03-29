"""Data quality checks for Velib snapshots."""

from datetime import datetime, timezone
import logging
from typing import Dict, List, Tuple

import pandas as pd
import pytz

logger = logging.getLogger(__name__)


class VelibDataQuality:
    """Run a standard suite of quality checks on a Velib snapshot DataFrame."""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results: list[tuple[str, bool, str]] = []

    def check_schema(self, expected_columns: List[str]) -> Tuple[bool, str]:
        """Validate presence of all expected columns."""
        missing_cols = set(expected_columns) - set(self.df.columns)
        if missing_cols:
            msg = f"Missing columns: {sorted(missing_cols)}"
            self.results.append(("schema", False, msg))
            return False, msg

        msg = f"Schema is valid ({len(expected_columns)} columns)"
        self.results.append(("schema", True, msg))
        return True, msg

    def check_nulls(
        self,
        critical_columns: List[str],
        threshold: float = 0.05,
    ) -> Tuple[bool, str]:
        """Validate null percentages for critical columns are below threshold."""
        issues = []

        for col in critical_columns:
            if col not in self.df.columns:
                continue

            null_pct = self.df[col].isnull().sum() / len(self.df)
            if null_pct > threshold:
                issues.append(f"{col}: {null_pct * 100:.2f}%")

        if issues:
            msg = f"Too many missing values: {', '.join(issues)}"
            self.results.append(("nulls", False, msg))
            return False, msg

        msg = f"Missing values are within threshold (< {threshold * 100:.1f}%)"
        self.results.append(("nulls", True, msg))
        return True, msg

    def check_duplicates(self, key_columns: List[str]) -> Tuple[bool, str]:
        """Validate there are no duplicates on key columns."""
        if not all(col in self.df.columns for col in key_columns):
            msg = "Cannot check duplicates because key columns are missing"
            self.results.append(("duplicates", False, msg))
            return False, msg

        duplicates = self.df.duplicated(subset=key_columns).sum()
        dup_pct = duplicates / len(self.df) * 100

        if duplicates > 0:
            msg = f"Detected {duplicates} duplicates ({dup_pct:.2f}%)"
            self.results.append(("duplicates", False, msg))
            return False, msg

        msg = "No duplicates detected"
        self.results.append(("duplicates", True, msg))
        return True, msg

    def check_data_freshness(
        self,
        timestamp_col: str,
        max_age_hours: int = 24,
    ) -> Tuple[bool, str]:
        """Validate latest timestamp is recent enough."""
        if timestamp_col not in self.df.columns:
            msg = f"Timestamp column '{timestamp_col}' not found"
            self.results.append(("freshness", False, msg))
            return False, msg

        latest_timestamp = pd.to_datetime(self.df[timestamp_col]).max()

        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.tz_localize("UTC")
        else:
            latest_timestamp = latest_timestamp.tz_convert("UTC")

        now = datetime.now(timezone.utc)
        age_hours = (now - latest_timestamp).total_seconds() / 3600

        if age_hours > max_age_hours:
            msg = f"Data is too old: {age_hours:.1f}h (max={max_age_hours}h)"
            self.results.append(("freshness", False, msg))
            return False, msg

        msg = f"Data freshness is valid ({age_hours:.1f}h old)"
        self.results.append(("freshness", True, msg))
        return True, msg

    def check_numeric_ranges(self, ranges: Dict[str, Tuple[float, float]]) -> Tuple[bool, str]:
        """Validate numeric values fall within configured ranges."""
        issues = []

        for col, (min_val, max_val) in ranges.items():
            if col not in self.df.columns:
                continue

            out_of_range = ((self.df[col] < min_val) | (self.df[col] > max_val)).sum()
            if out_of_range > 0:
                issues.append(f"{col}: {out_of_range} values out of [{min_val}, {max_val}]")

        if issues:
            msg = f"Out-of-range values found: {'; '.join(issues)}"
            self.results.append(("ranges", False, msg))
            return False, msg

        msg = "All numeric values are within expected ranges"
        self.results.append(("ranges", True, msg))
        return True, msg

    def check_data_consistency(self) -> Tuple[bool, str]:
        """Validate numbikesavailable + numdocksavailable equals capacity."""
        required = ["numbikesavailable", "numdocksavailable", "capacity"]
        if not all(col in self.df.columns for col in required):
            msg = "Cannot check consistency because required columns are missing"
            self.results.append(("consistency", False, msg))
            return False, msg

        computed_capacity = self.df["numbikesavailable"] + self.df["numdocksavailable"]
        inconsistent = (computed_capacity != self.df["capacity"]).sum()

        if inconsistent > 0:
            msg = (
                f"Detected {inconsistent} inconsistent rows "
                "(numbikesavailable + numdocksavailable != capacity)"
            )
            self.results.append(("consistency", False, msg))
            return False, msg

        msg = "Data consistency check passed"
        self.results.append(("consistency", True, msg))
        return True, msg

    def run_all_checks(self) -> Dict:
        """Execute all checks and return a structured report."""
        logger.info("Starting data quality checks")

        expected_columns = [
            "stationcode",
            "name",
            "is_installed",
            "capacity",
            "numdocksavailable",
            "numbikesavailable",
            "mechanical",
            "ebike",
            "is_renting",
            "is_returning",
            "duedate",
            "nom_arrondissement_communes",
            "code_insee_commune",
            "lon",
            "lat",
            "station_opening_hours",
            "ingestion_timestamp",
            "snapshot_id",
        ]

        critical_columns = [
            "stationcode",
            "name",
            "lon",
            "lat",
            "capacity",
            "numdocksavailable",
            "numbikesavailable",
        ]

        numeric_ranges = {
            "capacity": (0, 110),
            "numbikesavailable": (0, 110),
            "numdocksavailable": (0, 110),
        }

        self.check_schema(expected_columns)
        self.check_nulls(critical_columns)
        self.check_duplicates(["stationcode", "ingestion_timestamp"])
        self.check_data_freshness("ingestion_timestamp", max_age_hours=1)
        self.check_numeric_ranges(numeric_ranges)
        self.check_data_consistency()

        passed = sum(1 for _, status, _ in self.results if status)
        failed = len(self.results) - passed

        report = {
            "timestamp": datetime.now(pytz.timezone("Europe/Paris")).isoformat(),
            "total_rows": len(self.df),
            "tests_passed": passed,
            "tests_failed": failed,
            "success_rate": passed / len(self.results) * 100 if self.results else 0,
            "details": [
                {"test": test, "passed": status, "message": msg}
                for test, status, msg in self.results
            ],
        }

        logger.info("Tests passed: %s/%s", passed, len(self.results))
        logger.info("Tests failed: %s/%s", failed, len(self.results))

        return report
