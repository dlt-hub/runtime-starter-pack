"""
USGS Earthquake source — public, no-auth feed of seismic events from USGS.

Stateless: the source is a function of `(interval_start, interval_end)`.
No cursor state is read from or written to `pipeline.state` — the caller
(typically Runtime's scheduler via `run_context`) supplies the window.

Available resources:
    earthquakes      — window-bound on event `time`, merge by `id`
    feeds_summary    — replace; significant events from the past 30 days
"""

from typing import Any, Dict, Iterable, Sequence
from datetime import datetime, timezone

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import FDSN_EVENT_URL, MIN_MAGNITUDE, PAGE_LIMIT, SIGNIFICANT_MONTH_URL


USGS_EPOCH = "2026-04-01T00:00:00+00:00"


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


@dlt.source
def source(interval_start: datetime, interval_end: datetime) -> Sequence[DltResource]:
    """USGS Earthquake source returning all available resources.

    `interval_start` and `interval_end` define the time window to fetch.
    They are typically the scheduler's `run_context["interval_start"]` /
    `interval_end`, but any caller-supplied bounds work.
    """

    # Both initial_value and end_value are pinned to caller-supplied bounds,
    # so dlt does not fall back to pipeline state — the source is stateless.
    incremental_interval = dlt.sources.incremental(
        "time",
        initial_value=interval_start,
        end_value=interval_end,
        range_end="closed",
    )

    @dlt.resource(write_disposition="merge", primary_key="id")
    def earthquakes(
        time: dlt.sources.incremental[datetime] = incremental_interval,
    ) -> Iterable[TDataItem]:
        """All M2.5+ earthquakes within the caller-supplied [start, end] window.

        The cursor is bound to the outer `interval_start`/`interval_end` args
        (not a persisted state entry). Bounds are pushed down to the FDSN
        service via `starttime`/`endtime`, so only the needed slice is fetched.
        Merge by `id` lets USGS event revisions update existing rows in place.
        """
        params: Dict[str, Any] = {
            "format": "geojson",
            "starttime": time.start_value.astimezone(timezone.utc).isoformat(),
            "minmagnitude": MIN_MAGNITUDE,
            "orderby": "time-asc",
            "limit": PAGE_LIMIT,
        }
        if time.end_value is not None:
            params["endtime"] = time.end_value.astimezone(timezone.utc).isoformat()

        payload = requests.get(FDSN_EVENT_URL, params=params).json()
        for feature in payload.get("features", []):
            yield _flatten_feature(feature)


    @dlt.resource(write_disposition="replace", primary_key="id")
    def feeds_summary() -> Iterable[TDataItem]:
        """Significant earthquakes from the past 30 days. Snapshot, replaced each run."""
        payload = requests.get(SIGNIFICANT_MONTH_URL).json()
        for feature in payload.get("features", []):
            yield _flatten_feature(feature)

    return [earthquakes, feeds_summary]
