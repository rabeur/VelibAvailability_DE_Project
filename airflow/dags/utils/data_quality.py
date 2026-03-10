"""
Data Quality checks for Vélib data
"""
from datetime import datetime, timezone
import pandas as pd
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class VelibDataQuality:
    """
    Velib data quality checks suite
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results = []

    def check_schema(self, expected_columns: List[str]) -> Tuple[bool, str]:
        """Check if all expected columns are present"""
        missing_cols = set(expected_columns) - set(self.df.columns)

        if missing_cols:
            msg = f"X Columns missing: {missing_cols}"
            self.results.append(('schema', False, msg))
            return False, msg

        msg = f"V Schema valid ({len(expected_columns)} columns)"
        self.results.append(('schema', True, msg))
        return True, msg

    def check_nulls(self, critical_columns: List[str], threshold: float = 0.05) -> Tuple[bool, str]:
        """
        Check if missing values in critical columns are below the threshold
        threshold: value missing percentage(5% by default)
        """
        issues = []

        for col in critical_columns:
            if col not in self.df.columns:
                continue

            null_pct = self.df[col].isnull().sum() / len(self.df)

            if null_pct > threshold:
                issues.append(f"{col}: {null_pct*100:.2f}%")

        if issues:
            msg = f"/!\   Too many missing values: {', '.join(issues)}"
            self.results.append(('nulls', False, msg))
            return False, msg

        msg = f"V Missing values OK (< {threshold*100}%)"
        self.results.append(('nulls', True, msg))
        return True, msg

    def check_duplicates(self, key_columns: List[str]) -> Tuple[bool, str]:
        """Check duplicates on key columns"""
        if not all(col in self.df.columns for col in key_columns):
            msg = "/!\   Impossible to check duplicates (missing columns)"
            self.results.append(('duplicates', False, msg))
            return False, msg

        duplicates = self.df.duplicated(subset=key_columns).sum()
        dup_pct = duplicates / len(self.df) * 100

        if duplicates > 0:
            msg = f"/!\   {duplicates} duplicates detected ({dup_pct:.2f}%)"
            self.results.append(('duplicates', False, msg))
            return False, msg

        msg = f"V No duplicates"
        self.results.append(('duplicates', True, msg))
        return True, msg

    def check_data_freshness(self, timestamp_col: str, max_age_hours: int = 24) -> Tuple[bool, str]:
        """Check data freshness"""
        if timestamp_col not in self.df.columns:
            msg = f"/!\   Timestamp column '{timestamp_col}' not found"
            self.results.append(('freshness', False, msg))
            return False, msg

        latest_timestamp = pd.to_datetime(self.df[timestamp_col]).max()
        # Convert to UTC if timezone-naive
        if hasattr(latest_timestamp, 'tz_localize'):
            # if naive, localize to UTC
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.tz_localize('UTC')
            else:
                # if already timezone-aware, convert to UTC
                latest_timestamp = latest_timestamp.tz_convert('UTC')

        now = datetime.now(timezone.utc)
        age_hours = (now - latest_timestamp).total_seconds() / 3600

        if age_hours > max_age_hours:
            msg = f"/!\   Data too old: {age_hours:.1f}h (max: {max_age_hours}h)"
            self.results.append(('freshness', False, msg))
            return False, msg

        msg = f"V Data fresh ({age_hours:.1f}h)"
        self.results.append(('freshness', True, msg))
        return True, msg

    def check_numeric_ranges(self, ranges: Dict[str, Tuple[float, float]]) -> Tuple[bool, str]:
        """
        Check that numeric values are within acceptable ranges
        ranges: {'column_name': (min_value, max_value)}
        """
        issues = []

        for col, (min_val, max_val) in ranges.items():
            if col not in self.df.columns:
                continue

            out_of_range = ((self.df[col] < min_val) | (self.df[col] > max_val)).sum()

            if out_of_range > 0:
                issues.append(f"{col}: {out_of_range} values out of range [{min_val}, {max_val}]")

        if issues:
            msg = f"/!\   Aberrant values: {'; '.join(issues)}"
            self.results.append(('ranges', False, msg))
            return False, msg

        msg = f"V Numeric values within expected ranges"
        self.results.append(('ranges', True, msg))
        return True, msg

    def check_data_consistency(self) -> Tuple[bool, str]:
        """
        Check data consistency
        Ex: numbikesavailable + numdocksavailable = capacity
        """
        # To adapt based on your actual columns
        if all(col in self.df.columns for col in ['numbikesavailable', 'numdocksavailable', 'capacity']):
            self.df['computed_capacity'] = self.df['numbikesavailable'] + self.df['numdocksavailable']
            inconsistent = (self.df['computed_capacity'] != self.df['capacity']).sum()

            if inconsistent > 0:
                msg = f"/!\  {inconsistent} inconsistencies (bikes + docks =/= capacity)"
                self.results.append(('consistency', False, msg))
                return False, msg

            msg = f"V Data consistency validated"
            self.results.append(('consistency', True, msg))
            return True, msg

        msg = "/!\  Impossible to check consistency (missing columns)"
        self.results.append(('consistency', False, msg))
        return False, msg

    def run_all_checks(self) -> Dict:
        """Execute all checks and compile a report"""
        logger.info("🔍 Starting data quality checks...")

        # tests configuration
        expected_columns = [
            'stationcode','name','is_installed','capacity',
            'numdocksavailable','numbikesavailable','mechanical',
            'ebike','is_renting','is_returning','duedate',
            'nom_arrondissement_communes','code_insee_commune',
            'lon','lat','station_opening_hours',
            'ingestion_timestamp','snapshot_id'
        ]

        critical_columns = ['stationcode', 'name', 'lon', 'lat','capacity', 'numdocksavailable', 'numbikesavailable']

        numeric_ranges = {
            'capacity': (0, 110),
            'numbikesavailable': (0, 110),
            'numdocksavailable': (0, 110)
        }

        # tests execution
        self.check_schema(expected_columns)
        self.check_nulls(critical_columns)
        self.check_duplicates(['stationcode', 'ingestion_timestamp'])
        self.check_data_freshness('ingestion_timestamp', max_age_hours=1)
        self.check_numeric_ranges(numeric_ranges)
        self.check_data_consistency()

        # report compilation
        passed = sum(1 for _, status, _ in self.results if status)
        failed = len(self.results) - passed

        report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(self.df),
            'tests_passed': passed,
            'tests_failed': failed,
            'success_rate': passed / len(self.results) * 100 if self.results else 0,
            'details': [
                {'test': test, 'passed': status, 'message': msg}
                for test, status, msg in self.results
            ]
        }

        # Logging results
        logger.info(f"✅ Tests passed: {passed}/{len(self.results)}")
        logger.info(f"❌ Tests failed: {failed}/{len(self.results)}")

        return report