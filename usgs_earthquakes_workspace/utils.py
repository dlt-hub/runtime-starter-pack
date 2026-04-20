"""Workspace-shared helpers used by pipeline + transformation jobs."""
from typing import Any, Optional

import dlt


def restore_incremental(
    pipeline: dlt.Pipeline,
    resource: dlt.sources.DltResource,
    incremental: dlt.sources.incremental[Any],
) -> Optional[dlt.sources.incremental[Any]]:
    """Return a copy of `incremental` with persisted state attached.

    `resource` must be a resource taken from a source (so `resource.source_name`
    is set). `incremental` carries all the config (range_start, range_end,
    last_value_func, on_cursor_value_missing, end_value, lag, Generic[T]).

    Returns `None` if the pipeline has never persisted incremental state for
    this resource/cursor combination.
    """
    if resource.source_name is None:
        raise ValueError(
            f"Resource `{resource.name}` is not bound to a source. "
            "Get it via `source.resources[name]` instead."
        )

    pipeline.activate()
    try:
        pipeline.sync_destination()
        if pipeline.first_run:
            return None
        # TODO: if there are no tables in schema (look by source name!) that seen data associated with this resource, exit

        sources = pipeline.state.get("sources", {})
        try:
            state = sources[resource.source_name]["resources"][resource.name][
                "incremental"
            ][incremental.cursor_path]
            print(state)
        except KeyError:
            return None  # never ran with this incremental
    finally:
        pipeline.deactivate()

    incr = incremental.copy()  # preserves range/func/missing/end_value/lag/type
    incr.resource_name = resource.name
    incr._cached_state = state
    incr.start_value = state["start_value"]
    return incr
