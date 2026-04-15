"""USGS earthquake ingestion and transformation pipeline.

Demonstrates scheduler-driven intervals, freshness constraints, a backfill
that cascades a refresh signal without loading data, dependency groups, and
timezone-aware cron scheduling.

Jobs:
    backfill_usgs          -- manual, refresh="always"; cascade originator + initial setup, loads no data
    usgs_daily             -- cron + backfill followup, scheduler-driven interval, refresh overrides start with epoch
    transform_earthquakes  -- cron, gated on daily freshness, consumes run_context interval
    transform_feeds_summary-- cron, gated on daily freshness, replace transform
    clock                  -- detached 1-minute heartbeat
"""

from datetime import timezone, datetime

import dlt
from dlt.hub import run
from dlt.hub.run import trigger, TJobRunContext

from usgs import USGS_EPOCH, source as usgs_source
from usgs.transformations import earthquake_daily_stats, feeds_summary_classified


usgs_ing_pipeline = dlt.pipeline(
    pipeline_name="usgs_ingest_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
    progress="log",
)

usgs_eq_stats_pipeline = dlt.pipeline(
    pipeline_name="usgs_earthquake_stats_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
    progress="log",
)

usgs_feeds_classified_pipeline = dlt.pipeline(
    pipeline_name="usgs_feeds_classified_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
    progress="log",
)


def _load_ingest(interval_start: datetime, interval_end: datetime, resources):
    """Run the USGS ingest pipeline for the given resource names."""
    source = usgs_source(interval_start, interval_end).with_resources(*resources)
    load_info = usgs_ing_pipeline.run(source)
    print(load_info)
    print(usgs_ing_pipeline.last_trace.last_normalize_info)


# ── Ingestion jobs ───────────────────────────────────────────────────


@run.pipeline(
    "usgs_ingest_pipeline",
    expose={"tags": ["backfill"], "display_name": "USGS backfill cascade"},
    refresh="always",
)
def backfill_usgs(epoch = USGS_EPOCH):
    """Cascade a refresh signal to every downstream job and do initial setup.

    `refresh="always"` makes every successful run clear `prev_completed_run`
    on all reachable downstream jobs, so their next run starts with
    `run_context["refresh"] = True` and reprocesses from `epoch`.

    This job does NOT load any data -- it is the cascade originator and the
    place to run one-shot setup (drop sources, initialize destination schema,
    warm caches, etc.). The actual re-ingest happens on downstream jobs.
    """
    print("CLEANING UP FOR EPOCH", epoch)


@run.pipeline(
    usgs_ing_pipeline,
    trigger=["*/3 * * * *", backfill_usgs.success],
    freshness=backfill_usgs.is_fresh,
    require={"timezone": "Europe/Berlin"}
)
def usgs_daily(run_context: TJobRunContext, epoch = USGS_EPOCH):
    """Load earthquakes + refresh the significant feed for the scheduler interval.

    `run_context["interval_start"]` / `interval_end` are supplied by the
    Runtime scheduler based on the cron trigger. The scheduler keeps
    intervals continuous: if prior ticks were missed (freshness gate, app
    downtime, ...) it extends the window back to the last successful end so
    no data is dropped.

    On refresh (cascade from `backfill_usgs`), we drop the source tables and
    override `interval_start` with `epoch` so the full history re-ingests.

    Gated on backfill freshness: won't fire until backfill has completed at
    least once. After that, runs every 3 minutes on its own cron schedule.
    """
    if run_context["refresh"]:
        # wipe source tables; the stateless source has no cursor to clear
        usgs_ing_pipeline.refresh = "drop_sources"
        # scheduler-supplied start is ignored on refresh -- start from epoch
        run_context["interval_start"] = datetime.fromisoformat(epoch)
    
    print("job context with applied epoch", run_context)

    _load_ingest(run_context["interval_start"], run_context["interval_end"], ["earthquakes", "feeds_summary"])


# ── Transformation jobs (require ibis dependency group) ──────────────


@run.pipeline(
    usgs_eq_stats_pipeline,
    trigger=trigger.every("5m"),
    freshness=[usgs_daily.is_fresh],
    require={"dependency_groups": ["ibis"], "timezone": "Europe/Berlin"},
    expose={"tags": ["transform"]},
)
def transform_earthquakes(run_context: TJobRunContext, epoch = USGS_EPOCH):
    """Aggregate the earthquake slice into daily/region stats for the scheduler interval.

    Consumes `run_context["interval_start"]` / `interval_end` directly -- no
    cursor state, no upstream-pipeline lookup. On refresh, drops the output
    table and overrides `interval_start` with `epoch` to re-aggregate the
    full history.
    """

    if run_context["refresh"]:
        usgs_eq_stats_pipeline.refresh = "drop_resources"
        run_context["interval_start"] = datetime.fromisoformat(epoch)
    
    print("job context with applied epoch", run_context)

    time_window = (run_context["interval_start"], run_context["interval_end"])
    print("using interval", time_window)
    load_info = usgs_eq_stats_pipeline.run(
        earthquake_daily_stats(usgs_ing_pipeline.dataset(), time_window)
    )
    print(load_info)
    print(usgs_eq_stats_pipeline.last_trace.last_normalize_info)


@run.pipeline(
    usgs_feeds_classified_pipeline,
    trigger=trigger.every("5m"),
    freshness=[usgs_daily.is_fresh],
    require={"dependency_groups": ["ibis"], "timezone": "Europe/Berlin"},
    expose={"tags": ["transform"]},
)
def transform_feeds_summary(run_context: TJobRunContext):
    """Rebuild the classified significant-events feed (replace, no incremental)."""
    if run_context["refresh"]:
        usgs_feeds_classified_pipeline.refresh = "drop_resources"

    load_info = usgs_feeds_classified_pipeline.run(
        feeds_summary_classified(usgs_ing_pipeline.dataset())
    )
    print(load_info)
    print(usgs_feeds_classified_pipeline.last_trace.last_normalize_info)


# ── Utility ──────────────────────────────────────────────────────────


@run.job(trigger=trigger.every("1m"))
def clock():
    """System clock -- detached heartbeat for liveness monitoring."""
    print("TIKTAK")


if __name__ == "__main__":
    usgs_daily({
        "interval_start": datetime.now(timezone.utc),
        "interval_end": datetime.now(timezone.utc),
        "refresh": False,
    })
    print("ingestion state", usgs_ing_pipeline.state["sources"])
    transform_earthquakes({
        "interval_start": datetime.now(timezone.utc),
        "interval_end": datetime.now(timezone.utc),
        "refresh": False,
    })
    print("transformation state", usgs_eq_stats_pipeline.state["sources"])

