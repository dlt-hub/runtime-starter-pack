"""
USGS Earthquake source — public, no-auth feed of seismic events from USGS.

Available resources:
    earthquakes      — incremental on event `time` (ISO 8601, ascending)
    feeds_summary    — replace; significant events from the past 30 days
"""

from typing import Any, Dict, Iterable, Sequence

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import FDSN_EVENT_URL, MIN_MAGNITUDE, PAGE_LIMIT, SIGNIFICANT_MONTH_URL


USGS_EPOCH = pendulum.parse("2026-04-01T00:00:00+00:00")


def _flatten_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a GeoJSON earthquake feature into a single record."""
    props = feature.get("properties") or {}
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
    epoch_ms = props.get("time")
    updated_ms = props.get("updated")
    return {
        "id": feature.get("id"),
        "magnitude": props.get("mag"),
        "magnitude_type": props.get("magType"),
        "place": props.get("place"),
        "time": pendulum.from_timestamp(epoch_ms / 1000) if epoch_ms is not None else None,
        "updated_at": (
            pendulum.from_timestamp(updated_ms / 1000) if updated_ms is not None else None
        ),
        "type": props.get("type"),
        "tsunami": props.get("tsunami"),
        "alert": props.get("alert"),
        "status": props.get("status"),
        "url": props.get("url"),
        "longitude": coords[0],
        "latitude": coords[1],
        "depth_km": coords[2],
    }


@dlt.resource(write_disposition="merge", primary_key="id")
def earthquakes(
    time: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "time",
        initial_value=USGS_EPOCH,
        range_end="closed",
    ),
) -> Iterable[TDataItem]:
    """All M2.5+ earthquakes worldwide. Incremental on event `time` (UTC datetime).

    Pushes the cursor down to the FDSN service via `starttime`/`endtime`, so
    backfills and daily loads only fetch the slice they need. The cursor type
    is `pendulum.DateTime` so that `to_sqlglot_filter()` renders typed
    `CAST(... AS TIMESTAMP)` literals downstream.
    """
    params: Dict[str, Any] = {
        "format": "geojson",
        "starttime": time.start_value.to_iso8601_string(),
        "minmagnitude": MIN_MAGNITUDE,
        "orderby": "time-asc",
        "limit": PAGE_LIMIT,
    }
    if time.end_value is not None:
        params["endtime"] = time.end_value.to_iso8601_string()

    payload = requests.get(FDSN_EVENT_URL, params=params).json()
    for feature in payload.get("features", []):
        yield _flatten_feature(feature)


@dlt.resource(write_disposition="replace", primary_key="id")
def feeds_summary() -> Iterable[TDataItem]:
    """Significant earthquakes from the past 30 days. Snapshot, replaced each run."""
    payload = requests.get(SIGNIFICANT_MONTH_URL).json()
    for feature in payload.get("features", []):
        yield _flatten_feature(feature)


@dlt.source
def source() -> Sequence[DltResource]:
    """USGS Earthquake source returning all available resources."""
    return [earthquakes, feeds_summary]
