"""Pipeline to ingest data from the Open Library API (books endpoint).

Uses dlt REST API source. No incremental loading; full replace on each run.
Run with: python open_library_pipeline.py
"""

import dlt
from dlt.sources.rest_api import rest_api_source


def open_library_source(query: str = "harry potter"):
    """
    Create a dlt REST API source for the Open Library Search API (books).

    Args:
        query: Search query string (default: "harry potter").
    """
    return rest_api_source({
        "client": {
            "base_url": "https://openlibrary.org",
            "headers": {
                "User-Agent": "dlt-open-library-pipeline (data-engineering)",
            },
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": query,
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": "num_found",
                    },
                },
            },
        ],
    })


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
        dataset_name="open_library_data",
        progress="log",
    )

    load_info = pipeline.run(open_library_source(query="harry potter"))
    print(load_info)
