"""MCP server exposing Jaffle Shop data tools.

Provides a `row_counts` tool that returns the number of rows in every table
across both the ingestion and transformation datasets. Runs as an interactive
job with SSE transport.
"""

import dlt
from fastmcp import FastMCP

mcp = FastMCP(
    "jaffle-shop-data",
    instructions="Tools for querying Jaffle Shop pipeline data",
)


@mcp.tool
def row_counts() -> dict:
    """Return row counts for all tables in the Jaffle Shop datasets."""
    results = {}

    # ingestion dataset (local DuckDB)
    ingest_pipe = dlt.pipeline(
        "jaffle_ingest",
        destination=dlt.destinations.duckdb("local_jaffle.duckdb"),
        dataset_name="ingest_data",
    )
    try:
        ingest_ds = ingest_pipe.dataset()
        for table_name in ingest_ds.schema.tables:
            if table_name.startswith("_dlt"):
                continue
            count = ingest_ds(f"SELECT COUNT(*) AS cnt FROM {table_name}").df()["cnt"][0]
            results[f"ingest.{table_name}"] = int(count)
    except Exception as e:
        results["ingest._error"] = str(e)

    # transformation dataset (warehouse destination)
    transform_pipe = dlt.pipeline(
        "jaffle_transform",
        destination="jaffleshop_transformation_destination",
    )
    try:
        transform_ds = transform_pipe.dataset()
        for table_name in transform_ds.schema.tables:
            if table_name.startswith("_dlt"):
                continue
            count = transform_ds(f"SELECT COUNT(*) AS cnt FROM {table_name}").df()["cnt"][0]
            results[f"transform.{table_name}"] = int(count)
    except Exception as e:
        results["transform._error"] = str(e)

    return results
