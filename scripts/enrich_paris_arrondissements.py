#!/usr/bin/env python3
from __future__ import annotations

"""
Backfill Paris arrondissement labels into `silver.stations` from station coordinates.

Usage:
    python3 scripts/enrich_paris_arrondissements.py --dry-run
    python3 scripts/enrich_paris_arrondissements.py

The script reads the current `silver.stations` table through the PostgreSQL Docker container,
computes the arrondissement for each Paris station using the official boundary GeoJSON,
and updates `district_municipality_names` with labels such as `Paris 11e Arrondissement`.
"""

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spark_jobs.paris_arrondissement_utils import get_paris_arrondissement_label


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_psql(sql: str, *, container: str, db_user: str, db_name: str, tuples_only: bool) -> str:
    command = ["docker", "exec", "-i", container, "psql", "-U", db_user, "-d", db_name]
    if tuples_only:
        command.extend(["-At", "-F", "\t"])
    command.extend(["-c", sql])

    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def fetch_paris_stations(*, container: str, db_user: str, db_name: str) -> list[dict]:
    sql = """
        SELECT
            station_id,
            latitude,
            longitude,
            COALESCE(district_municipality_names, '') AS district_municipality_names
        FROM silver.stations
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND lower(COALESCE(district_municipality_names, '')) LIKE 'paris%'
        ORDER BY station_id;
    """

    output = run_psql(
        sql,
        container=container,
        db_user=db_user,
        db_name=db_name,
        tuples_only=True,
    )

    stations = []
    for line in output.splitlines():
        station_id, latitude, longitude, district_name = line.split("\t")
        stations.append(
            {
                "station_id": station_id,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "district_name": district_name,
            }
        )

    return stations


def build_updates(stations: list[dict]) -> tuple[list[tuple[str, str]], list[str]]:
    updates: list[tuple[str, str]] = []
    unresolved: list[str] = []

    for station in stations:
        arrondissement = get_paris_arrondissement_label(
            latitude=station["latitude"],
            longitude=station["longitude"],
        )

        if arrondissement is None:
            unresolved.append(station["station_id"])
            continue

        if arrondissement != station["district_name"]:
            updates.append((station["station_id"], arrondissement))

    return updates, unresolved


def chunked(items: list[tuple[str, str]], chunk_size: int) -> list[list[tuple[str, str]]]:
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def apply_updates(
    updates: list[tuple[str, str]], *, container: str, db_user: str, db_name: str
) -> None:
    for batch in chunked(updates, chunk_size=200):
        case_lines = "\n".join(
            f"    WHEN {sql_quote(station_id)} THEN {sql_quote(arrondissement)}"
            for station_id, arrondissement in batch
        )
        ids = ", ".join(sql_quote(station_id) for station_id, _ in batch)

        sql = f"""
            UPDATE silver.stations
            SET district_municipality_names = CASE station_id
{case_lines}
                ELSE district_municipality_names
            END,
            updated_at = CURRENT_TIMESTAMP
            WHERE station_id IN ({ids});
        """

        run_psql(
            sql,
            container=container,
            db_user=db_user,
            db_name=db_name,
            tuples_only=False,
        )


def print_summary(*, container: str, db_user: str, db_name: str) -> None:
    sql = """
        SELECT district_municipality_names, COUNT(*)
        FROM silver.stations
        WHERE lower(COALESCE(district_municipality_names, '')) LIKE 'paris%'
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC;
    """
    print(run_psql(sql, container=container, db_user=db_user, db_name=db_name, tuples_only=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Paris stations with arrondissement labels")
    parser.add_argument("--container", default="velib_postgres", help="PostgreSQL Docker container name")
    parser.add_argument("--db-user", default="velib", help="Database user")
    parser.add_argument("--db-name", default="velib_dw", help="Database name")
    parser.add_argument("--dry-run", action="store_true", help="Preview the number of rows to update")
    args = parser.parse_args()

    stations = fetch_paris_stations(
        container=args.container,
        db_user=args.db_user,
        db_name=args.db_name,
    )
    updates, unresolved = build_updates(stations)

    print(f"Paris stations checked: {len(stations)}")
    print(f"Stations to update: {len(updates)}")
    print(f"Stations unresolved: {len(unresolved)}")

    if unresolved:
        print("Unresolved station_ids:", ", ".join(unresolved[:20]))

    if args.dry_run:
        return

    if updates:
        apply_updates(
            updates,
            container=args.container,
            db_user=args.db_user,
            db_name=args.db_name,
        )
        print("\n✅ Paris arrondissement enrichment applied to silver.stations\n")
    else:
        print("\nℹ️ No station required an update\n")

    print_summary(container=args.container, db_user=args.db_user, db_name=args.db_name)


if __name__ == "__main__":
    main()
