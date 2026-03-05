"""Pipeline to ingest NYC taxi data from the Data Engineering Zoomcamp REST API.

Uses dlt REST API source with offset pagination (1,000 records per page).
Stops when an empty page is returned.
Run with: python taxi_pipeline.py
"""

import dlt
from dlt.sources.rest_api import rest_api_source


def taxi_trips_source():
    """
    Create a dlt REST API source for NYC taxi data.

    API returns paginated JSON (1,000 records per page).
    Pagination stops when an empty page is returned.
    """
    return rest_api_source({
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "",
                    "params": {
                        "limit": 1000,
                    },
                    "data_selector": "$",
                    "paginator": {
                        "type": "offset",
                        "limit": 1000,
                        "offset": 0,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            },
        ],
    })


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
        progress="log",
    )

    load_info = pipeline.run(taxi_trips_source())
    print(load_info)
