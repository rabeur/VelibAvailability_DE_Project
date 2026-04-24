# syntax=docker/dockerfile:1.6
FROM ghcr.io/dbt-labs/dbt-postgres:1.9.latest

# Add the BigQuery adapter alongside dbt-postgres so a single dbt container
# can serve both the local (`prod`) and cloud (`bigquery_cloud`) targets
# declared in velib_dbt/profiles.yml. Pinned to the 1.9 minor line to stay
# ABI-compatible with the dbt-core version shipped by the base image.
RUN pip install --no-cache-dir "dbt-bigquery>=1.9,<1.10"
