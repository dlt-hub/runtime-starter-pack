"""Jaffle Shop ingestion pipeline.

Loads raw data from the Jaffle Shop REST API (customers, products, orders) and
a local payments parquet file into a local DuckDB. All resources use replace
mode -- every run is a full refresh. The ingestion destination is local-only;
transformations read from it and write to the remote warehouse.
"""

import pathlib

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.filesystem import filesystem, read_parquet
from dlt.hub import run
from dlt.hub.run import trigger


# REST API resources: customers, products, orders (all replace)
jaffle_rest_resources: list = rest_api_resources(
    {
        "client": {
            "base_url": "https://jaffle-shop.dlthub.com/api/v1",
            "paginator": {"type": "header_link"},
        },
        "resources": [
            "customers",
            "products",
            "orders",
        ],
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "params": {
                    "start_date": "2017-01-01",
                    "end_date": "2017-01-15",
                },
            },
        },
    }
)

# local payments parquet file (replace)
files_directory = pathlib.Path(__file__).parent
payments_files_resource = filesystem(
    bucket_url=str(files_directory), file_glob="*payments.parquet"
)
payments_resource = (payments_files_resource | read_parquet).with_name("payments")
payments_resource.write_disposition = "replace"


@dlt.source
def jaffle_shop_raw_data():
    """Raw data about the Jaffle Shop operations."""
    return (*jaffle_rest_resources, payments_resource)


jaffle_ingest_pipe = dlt.pipeline(
    "jaffle_ingest",
    destination="jaffleshop_transformation_destination",
    dataset_name="ingest_data"
)


@run.pipeline(
    jaffle_ingest_pipe,
    trigger=trigger.schedule("0 * * * *"),
    execute={"timeout": "6h"},
    expose={"display_name": "Jaffle Shop ingest"},
)
def ingest_jaffle():
    """Load raw Jaffle Shop data into local DuckDB."""
    load_info = jaffle_ingest_pipe.run(jaffle_shop_raw_data())
    print(load_info)


if __name__ == "__main__":
    ingest_jaffle()
