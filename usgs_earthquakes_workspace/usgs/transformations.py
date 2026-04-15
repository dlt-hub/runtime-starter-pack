"""USGS earthquake transformations (Ibis).

Two `@dlt.hub.transformation` resources that read from the ingestion dataset
and emit Ibis table expressions. Because the source and target pipelines share
the same physical destination, dlt runs the query in-warehouse.
"""

import typing
from typing import Optional, Tuple
from datetime import timezone

import dlt
import ibis
from ibis import ir

from dlt.common import pendulum


@dlt.hub.transformation(
    table_name="earthquake_daily_stats",
    write_disposition="merge",
    primary_key=["day", "region"],
)
def earthquake_daily_stats(
    dataset: dlt.Dataset,
    time_window: Optional[Tuple[pendulum.DateTime, pendulum.DateTime]] = None,
) -> typing.Iterator[ir.Table]:
    """Daily aggregation of earthquakes by region for a time window.

    Args:
        dataset: the ingestion dataset containing the `earthquakes` table.
        time_window: optional (start, end) bounds; typically the scheduler
            interval from `run_context["interval_start"]`/`interval_end`.
            When None, all rows are included (full rebuild on refresh).
    """
    eq = dataset.table("earthquakes").to_ibis()

    if time_window is not None:
        start, end = time_window
        # always use utc - this is how data in database tables will be stored
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
        eq = eq.filter((eq.time >= start) & (eq.time <= end))

    yield (
        eq.mutate(
            day=eq.time.cast("date"),
            region=ibis.coalesce(
                eq.place.split(", ")[-1].nullif(""),
                ibis.literal("Unknown"),
            ),
        )
        .group_by(["day", "region"])
        .aggregate(
            event_count=ibis._.count(),
            avg_magnitude=ibis._.magnitude.mean(),
            max_magnitude=ibis._.magnitude.max(),
            min_magnitude=ibis._.magnitude.min(),
            avg_depth_km=ibis._.depth_km.mean(),
        )
    )


@dlt.hub.transformation(
    table_name="feeds_summary_classified",
    write_disposition="replace",
    primary_key="id",
)
def feeds_summary_classified(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Significant-events feed enriched with magnitude severity + region.

    Source is replace, so the transformation is replace too. No time filter --
    every run rebuilds the full classified snapshot.
    """
    fs = dataset.table("feeds_summary").to_ibis()

    yield fs.mutate(
        severity=ibis.cases(
            (fs.magnitude >= 8, "great"),
            (fs.magnitude >= 7, "major"),
            (fs.magnitude >= 6, "strong"),
            (fs.magnitude >= 5, "moderate"),
            (fs.magnitude >= 4, "light"),
            else_="minor",
        ),
        region=ibis.coalesce(
            fs.place.split(", ")[-1].nullif(""),
            ibis.literal("Unknown"),
        ),
    )
