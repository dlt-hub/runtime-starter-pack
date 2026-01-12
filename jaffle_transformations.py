import dlt
import pathlib
import typing

import ibis
from ibis import ir

from dlt.sources.rest_api import rest_api_resources
from dlt.sources.filesystem import filesystem, read_parquet

if __name__ == "__main__":
    # get jaffle shop raw data from the rest api
    jaffle_rest_resources: list = rest_api_resources(
        {
            "client": {
                "base_url": "https://jaffle-shop.dlthub.com/api/v1",
                "paginator": {"type": "header_link"},
            },
            "resources": [  # individual resources
                "customers",
                "products",
                "orders",
            ],
            # set the time range for all resources
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "start_date": "2017-01-01",
                        "end_date": "2017-01-15",
                    },
                },
            },
        }
    )

    # get local payments file as resource
    files_directory = pathlib.Path(__file__).parent
    payments_files_resource = filesystem(
        bucket_url=str(files_directory), file_glob="*payments.parquet"
    )
    payments_resource = (payments_files_resource | read_parquet).with_name("payments")

    # combine the rest api resources and the local payments file into a single source
    @dlt.source
    def jaffle_shop_raw_data():
        """Raw data about the Jaffle Shop operations."""
        return (*jaffle_rest_resources, payments_resource)

    # load into local duckdb, jaffle_ingest will not be queryable on runtime
    destination = dlt.destinations.duckdb("local_jaffle.duckdb")
    jaffle_ingest_pipe = dlt.pipeline("jaffle_ingest", destination=destination)
    ingest_load_info = jaffle_ingest_pipe.run(jaffle_shop_raw_data())
    print(ingest_load_info)

    # aggregate customer orders
    @dlt.hub.transformation
    def customer_orders(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
        """Aggregate statistics about previous customer orders"""
        orders = dataset.table("orders").to_ibis()
        yield orders.group_by("customer_id").aggregate(
            first_order=orders.ordered_at.min(),
            most_recent_order=orders.ordered_at.max(),
            number_of_orders=orders.id.count(),
        )

    # aggregate customer payments
    @dlt.hub.transformation
    def customer_payments(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
        """Customer order and payment info"""
        orders = dataset.table("orders").to_ibis()
        payments = dataset.table("payments").to_ibis()
        yield (
            payments.left_join(orders, payments.order_id == orders.id)
            .group_by(orders.customer_id)
            .aggregate(total_amount=ibis._.amount.sum())
        )

    # combine the customer orders and payments into a single source
    @dlt.source
    def customers_metrics(raw_dataset: dlt.Dataset) -> list:
        return [
            customer_orders(raw_dataset),
            customer_payments(raw_dataset),
        ]

    # load into remote motherduck destination (duckdb for local runs)
    jaffle_transform_pipe = dlt.pipeline(
        "jaffle_transform", destination="jaffleshop_transformation_destination"
    )
    transform_load_info = jaffle_transform_pipe.run(
        customers_metrics(jaffle_ingest_pipe.dataset())
    )
    print(transform_load_info)
