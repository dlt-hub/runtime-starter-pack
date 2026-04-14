"""USGS earthquake ingestion and transformation pipeline.

Demonstrates incremental loading, freshness constraints, backfill with
refresh cascade, and dependency groups for transform jobs.

Jobs:
    backfill_usgs          -- manual, refresh="always", full re-ingest from epoch
    usgs_daily             -- cron + backfill followup, gated on backfill freshness
    transform_earthquakes  -- cron, gated on daily freshness, restores incremental
    transform_feeds_summary-- cron, gated on daily freshness, replace transform
    clock                  -- detached 1-minute heartbeat
"""

import dlt
from dlt.common import pendulum
from dlt.hub import run
from dlt.hub.run import trigger, TJobRunContext

from usgs import USGS_EPOCH, source as usgs_source
from usgs.transformations import earthquake_daily_stats, feeds_summary_classified
from utils import restore_incremental


usgs_ing_pipeline = dlt.pipeline(
    pipeline_name="usgs_ingest_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
)

usgs_eq_stats_pipeline = dlt.pipeline(
    pipeline_name="usgs_earthquake_stats_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
)

usgs_feeds_classified_pipeline = dlt.pipeline(
    pipeline_name="usgs_feeds_classified_pipeline",
    destination="pokeland",
    dataset_name="usgs_data",
)


def _load_ingest(resources):
    """Run the USGS ingest pipeline for the given resource names."""
    source = usgs_source().with_resources(*resources)
    load_info = usgs_ing_pipeline.run(source)
    print(load_info)


# ── Ingestion jobs ───────────────────────────────────────────────────


@run.pipeline(
    "usgs_ingest_pipeline",
    expose={"tags": ["backfill"], "display_name": "USGS backfill cascade"},
    refresh="always",
)
def backfill_usgs(run_context: TJobRunContext):
    """Backfill historical earthquake catalog and refresh the significant feed.

    refresh="always" means every successful run cascades a refresh signal to
    all downstream jobs, clearing their prev_completed_run watermarks.
    """
    usgs_ing_pipeline.refresh = "drop_sources"
    _load_ingest(["earthquakes", "feeds_summary"])


@run.pipeline(
    usgs_ing_pipeline,
    trigger=["*/3 * * * *", backfill_usgs.success],
    freshness=backfill_usgs.is_fresh,
    name="usgs_daily_load",
)
def usgs_daily(run_context: TJobRunContext):
    """Incremental load of new earthquakes plus refresh of the significant feed.

    Gated on backfill freshness: won't fire until backfill has completed at
    least once. After that, runs every 3 minutes on its own cron schedule.
    """
    _load_ingest(["earthquakes", "feeds_summary"])


# ── Transformation jobs (require ibis dependency group) ──────────────


@run.pipeline(
    usgs_eq_stats_pipeline,
    trigger=trigger.every("5m"),
    freshness=[usgs_daily.is_fresh],
    require={"dependency_groups": ["ibis"]},
)
def transform_earthquakes(run_context: TJobRunContext):
    """Aggregate the freshly-loaded earthquake slice into daily/region stats.

    Restores the upstream incremental cursor and passes the time window to the
    Ibis transformation. On refresh, processes the full catalog instead.
    """
    incremental = restore_incremental(
        usgs_ing_pipeline,
        usgs_source().earthquakes,
        dlt.sources.incremental[pendulum.DateTime](
            "time",
            initial_value=USGS_EPOCH,
            range_end="closed",
        ),
    )
    if incremental is None:
        print("no incremental state yet -- skipping (ingestion has not run)")
        return

    if run_context["refresh"]:
        usgs_eq_stats_pipeline.refresh = "drop_resources"
        time_window = None
    else:
        time_window = (incremental.start_value, incremental.last_value)

    load_info = usgs_eq_stats_pipeline.run(
        earthquake_daily_stats(usgs_ing_pipeline.dataset(), time_window)
    )
    print(load_info)


@run.pipeline(
    usgs_feeds_classified_pipeline,
    trigger=trigger.every("5m"),
    freshness=[usgs_daily.is_fresh],
    require={"dependency_groups": ["ibis"]},
)
def transform_feeds_summary(run_context: TJobRunContext):
    """Rebuild the classified significant-events feed (replace, no incremental)."""
    if run_context["refresh"]:
        usgs_feeds_classified_pipeline.refresh = "drop_resources"

    load_info = usgs_feeds_classified_pipeline.run(
        feeds_summary_classified(usgs_ing_pipeline.dataset())
    )
    print(load_info)


# ── Utility ──────────────────────────────────────────────────────────


@run.job(trigger=trigger.every("1m"))
def clock():
    """System clock -- detached heartbeat for liveness monitoring."""
    print("TIKTAK")


if __name__ == "__main__":
    backfill_usgs({"run_id": "local", "trigger": "manual:", "refresh": True})
