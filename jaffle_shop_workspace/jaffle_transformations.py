"""Jaffle Shop transformation pipeline.

Reads raw data from the local ingestion DuckDB, computes customer-level
aggregations using Ibis transformations, and loads the results into the
remote warehouse destination. Triggered automatically after ingestion succeeds.
"""

import typing

import dlt
import ibis
from ibis import ir
from dlt.hub import run

from jaffle_ingestion import ingest_jaffle, jaffle_ingest_pipe


@dlt.hub.transformation(write_disposition="replace")
def customer_orders(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Aggregate statistics about previous customer orders."""
    orders = dataset.table("orders").to_ibis()
    yield orders.group_by("customer_id").aggregate(
        first_order=orders.ordered_at.min(),
        most_recent_order=orders.ordered_at.max(),
        number_of_orders=orders.id.count(),
    )


@dlt.hub.transformation(write_disposition="replace")
def customer_payments(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Total payment amount per customer."""
    orders = dataset.table("orders").to_ibis()
    payments = dataset.table("payments").to_ibis()
    yield (
        payments.left_join(orders, payments.order_id == orders.id)
        .group_by(orders.customer_id)
        .aggregate(total_amount=ibis._.amount.sum())
    )


@dlt.source
def customers_metrics(raw_dataset: dlt.Dataset) -> list:
    """Combine customer aggregations into a single source."""
    return [
        customer_orders(raw_dataset),
        customer_payments(raw_dataset),
    ]


@run.pipeline(
    "jaffle_transform",
    trigger=ingest_jaffle.success,
    execute={"timeout": {"timeout": 7200, "grace_period": 60}},
    expose={"display_name": "Jaffle Shop transform"},
)
def transform_jaffle():
    """Transform raw Jaffle data into customer metrics."""
    jaffle_transform_pipe = dlt.pipeline(
        "jaffle_transform",
        destination="jaffleshop_transformation_destination",
    )
    load_info = jaffle_transform_pipe.run(
        customers_metrics(jaffle_ingest_pipe.dataset())
    )
    print(load_info)


if __name__ == "__main__":
    transform_jaffle()
