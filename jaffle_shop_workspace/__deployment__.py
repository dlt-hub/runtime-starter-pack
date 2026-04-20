"""Jaffle Shop -- ingest and transform customer data"""

import dlt
from dlt.hub import run

from jaffle_ingestion import ingest_jaffle
from jaffle_transformations import transform_jaffle


@run.interactive(
    section="jaffle_mcp",
    interface="mcp",
    idle_timeout="30m",
    expose={"display_name": "Jaffle Shop MCP"},
)
def jaffle_mcp():
    """MCP server exposing a row_counts tool over the Jaffle Shop datasets."""
    from fastmcp import FastMCP

    mcp = FastMCP(
        "jaffle-shop-data",
        instructions="Tools for querying Jaffle Shop pipeline data",
    )

    @mcp.tool
    def row_counts() -> dict:
        """Return row counts for all tables in the Jaffle Shop datasets."""
        results = {}
        for pipeline_name in ("jaffle_ingest", "jaffle_transform"):
            ds = dlt.pipeline(
                pipeline_name,
                destination="jaffleshop_transformation_destination",
            ).dataset()
            counts = ds.row_counts().arrow().to_pydict()
            for table, count in zip(counts["table_name"], counts["row_count"]):
                results[f"{pipeline_name}.{table}"] = int(count)
        return results

    return mcp


__all__ = ["ingest_jaffle", "transform_jaffle", "jaffle_mcp"]
