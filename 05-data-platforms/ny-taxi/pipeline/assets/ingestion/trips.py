"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips 

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# columns:
#   - name: TODO_col1
#     type: TODO_type
#     description: TODO

# columns:
#   - name: pickup_datetime
#     type: timestamp
#   - name: dropoff_datetime
#     type: timestamp
#   - name: passenger_count
#     type: integer
#   - name: trip_distance
#     type: numeric
#   - name: payment_type
#     type: integer
#   - name: fare_amount
#     type: numeric
#   - name: total_amount
#     type: numeric
#   - name: extracted_at
#     type: timestamp
@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

import os
import json
import requests
import pandas as pd
import io
import datetime
from typing import List


def _months_between(start_date: datetime.date, end_date: datetime.date) -> List[tuple]:
    months = []
    y, m = start_date.year, start_date.month
    while (y, m) <= (end_date.year, end_date.month):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _default_date_window():
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month_end = first - datetime.timedelta(days=1)
    start = last_month_end.replace(day=1)
    end = last_month_end
    return start, end


def materialize():
    """Fetch monthly TLC taxi files and return a pandas DataFrame."""
    start_env = os.getenv("BRUIN_START_DATE")
    end_env = os.getenv("BRUIN_END_DATE")
    
    if start_env and end_env:
        start_date = datetime.datetime.strptime(start_env, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_env, "%Y-%m-%d").date()
    else:
        start_date, end_date = _default_date_window()

    vars_json = os.getenv("BRUIN_VARS", "{}")
    try:
        vars_obj = json.loads(vars_json)
    except Exception:
        vars_obj = {}

    taxi_types = vars_obj.get("taxi_types", ["green"]) or ["green"]

    dfs: List[pd.DataFrame] = []
    months = _months_between(start_date, end_date)
    
    for taxi in taxi_types:
        for (y, m) in months:
            ym = f"{y}-{m:02d}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{ym}.parquet"
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200 and resp.content:
                    try:
                        df = pd.read_parquet(io.BytesIO(resp.content))
                    except Exception:
                        try:
                            df = pd.read_csv(io.BytesIO(resp.content))
                        except Exception:
                            print(f"Failed to parse file for {taxi} {ym} (parquet/csv)")
                            continue
                            
                    # Add metadata columns
                    df["extracted_at"] = datetime.datetime.utcnow().isoformat()
                    df["taxi_type"] = taxi
                    
                    dfs.append(df)
                    print(f"Fetched {taxi} {ym}: {len(df)} rows")
                else:
                    print(f"File not found or empty: {url} (status {resp.status_code})")
            except Exception as e:
                print(f"Error fetching {url}: {e}")

    # Fail loudly if no data was fetched, enforcing our strict ELT pattern
    if not dfs:
        raise ValueError(f"No data was fetched for the specified date range and taxi types ({taxi_types}).")

    result = pd.concat(dfs, ignore_index=True, sort=False)
    return result