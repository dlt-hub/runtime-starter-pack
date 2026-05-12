# Deployment Reference

This document is the full reference for dlt's deployment system: job decorators,
triggers, module conventions, and manifest auto-generation. It covers every
parameter and convention available when defining jobs in Python.

Import the public API from `dlt.hub`:

```python
from dlt.hub import run
from dlt.hub.run import trigger, TJobRunContext
```

---

## Decorator arguments

All three decorators -- `@run.job`, `@run.pipeline`, `@run.interactive` --
share most arguments. Differences are noted per row.

| Argument | Type | Default | Used by | What it does |
|----------|------|---------|---------|--------------|
| `name` | `str` | function name | all | Job name within its section. Combined with `section` to form the job_ref (`jobs.<section>.<name>`). |
| `section` | `str` | module name | all | Config section. Override when the containing module name doesn't match the desired config path (e.g. when defining a job in `__deployment__.py` and wanting a cleaner section name). |
| `trigger` | `str`, `TTrigger`, or list | none | `@job`, `@pipeline` | When to run. Accepts cron strings, `trigger.every("5m")`, `trigger.schedule("0 8 * * *")`, followup references (`upstream.success`), or a list of any of these. |
| `execute` | `TExecuteSpec` | `{}` | all | Execution constraints: `timeout` (string like `"6h"` or `TTimeoutSpec` dict) and `concurrency` (max concurrent runs, default unlimited for batch, 1 for interactive). |
| `expose` | `TJobExposeSpec` | `{}` | all | UI presentation -- see below. |
| `require` | `TRequireSpec` | `{}` | all | Execution-environment resource requirements -- see below. |
| `deliver` | source / resource / `SourceFactory` | none | `@job`, `@pipeline` | Associates the job with a `@dlt.source` so dltHub can show what data the job produces. `@run.pipeline` sets this from the pipeline name automatically. |
| `interval` | `TIntervalSpec` | none | `@job`, `@pipeline` | Overall time range for scheduler-driven intervals: `{"start": "2026-01-01T00:00:00Z"}` with optional `"end"` (defaults to now). Together with a cron trigger, defines the set of discrete windows the job covers. On refresh, the scheduler resets the interval pointer to `start`. |
| `freshness` | constraint string or list | none | `@job`, `@pipeline` | Upstream freshness gates. A gate prevents the job from running until the upstream job's interval is complete. Not a trigger -- the job still runs on its own schedule, but only when the gate is satisfied. Example: `freshness=[upstream.is_fresh]`. |
| `refresh` | `"auto"` / `"always"` / `"block"` | `"auto"` | `@job`, `@pipeline` | Refresh-signal cascade policy. `"always"` originates a refresh on every success. `"auto"` passes through if received. `"block"` stops propagation. |
| `spec` | `BaseConfiguration` subclass | none | all | Optional config spec class. Normally unnecessary -- function arguments with `dlt.config.value` defaults are auto-discovered. |
| `idle_timeout` | `float` or `"24h"` | none | `@interactive` | Recycle the long-running process after this idle duration. |
| `interface` | `"gui"` / `"rest_api"` / `"mcp"` | `"gui"` | `@interactive` | What an interactive job exposes: a web UI, a programmatic API, or an MCP tool server. |

### `expose` -- UI presentation

`expose` is a `TJobExposeSpec` dict controlling how the job appears in the
dltHub dashboard and how it can be triggered:

| Key | Type | Default | What it does |
|-----|------|---------|--------------|
| `display_name` | `str` | function name | Human-friendly label shown in the UI. May contain spaces and punctuation. |
| `tags` | `str` or `list[str]` | `[]` | Group labels for organizing and bulk-triggering jobs. Each tag auto-generates a `tag:<name>` trigger so `dlthub job trigger "tag:..."` fires every job with that tag. |
| `starred` | `bool` | `False` | Pin to the top of the workspace overview UI. |
| `manual` | `bool` | `True` | When `True`, dltHub auto-adds a `manual:` trigger so users can fire the job from the dashboard or via `dlthub run`. Set to `False` to disable manual triggering entirely. |
| `interface` | `"gui"` / `"rest_api"` / `"mcp"` | -- | Set automatically by `@run.interactive(interface=...)`. Rarely set directly. |
| `category` | `"pipeline"` / `"mcp"` / `"dashboard"` / `"notebook"` | auto | Set automatically by the framework detector or decorator. `@run.pipeline` sets `pipeline`; marimo modules `notebook`; FastMCP modules `mcp`; Streamlit modules `dashboard`. Rarely set manually. |

Example:

```python
@run.pipeline(
    "my_pipeline",
    trigger=trigger.schedule("0 8 * * *"),
    expose={
        "display_name": "Daily data sync",
        "tags": ["ingest", "production"],
        "starred": True,
    },
)
def daily_sync():
    ...
```

### `require` -- runtime resource requirements

`require` is a `TRequireSpec` dict telling dltHub how to provision the
execution environment:

| Key | Type | What it does |
|-----|------|--------------|
| `dependency_groups` | `list[str]` | PEP 735 dependency groups (from `pyproject.toml`'s `[dependency-groups]`) installed on top of the workspace's base dependencies. |
| `profile` | `str` | Workspace profile to activate for this job. Overrides the default (`prod` for batch, `access` for interactive). |
| `timezone` | `str` | IANA timezone for cron ticks and intervals (e.g. `"America/New_York"`). Default: UTC. Scheduler computes interval boundaries in this zone; `run_context` datetimes are still UTC. |

Example -- transform jobs that need ibis and run on Berlin time:

```python
@run.pipeline(
    transform_pipeline,
    trigger=trigger.schedule("*/5 * * * *"),
    require={
        "dependency_groups": ["ibis"],
        "timezone": "Europe/Berlin",
    },
)
def transform_data(run_context: TJobRunContext):
    ...
```

```toml
# pyproject.toml
[dependency-groups]
ibis = ["ibis-framework[duckdb]"]
```

### `execute` -- timeout and concurrency

`execute` is a `TExecuteSpec` dict controlling runtime behavior:

| Key | Type | Default | What it does |
|-----|------|---------|--------------|
| `timeout` | `str`, `float`, or `TTimeoutSpec` | none | Max wall-clock duration. String shorthand (`"6h"`, `"30m"`) or a dict with `timeout` (seconds) and `grace_period` (seconds, default 30). |
| `concurrency` | `int` or `None` | batch: `None`, interactive: `1` | Max concurrent runs. `None` = no limit. |

When timeout expires, dltHub sends a termination signal. The grace period
is the window for the job to finish in-flight work before a hard kill.

```python
# string shorthand
@run.job(execute={"timeout": "6h"})
def long_job(): ...

# explicit dict with custom grace period (2h timeout, 1min grace)
@run.pipeline(
    "my_pipeline",
    execute={"timeout": {"timeout": 7200, "grace_period": 60}},
)
def transform(): ...
```

There are no retries at the platform level. Retry logic belongs in your
pipeline code where you know whether a partial load is safe to resume.

---

## Trigger types

All triggers normalize to `"type:expr"` strings stored in the manifest. Use
the constructors from `dlt.hub.run.trigger` for typed parameters and
validation, or pass the bare string when the shorthand suffices.

| Type | Constructor | Manifest form | Notes |
|------|-------------|---------------|-------|
| `schedule` | `trigger.schedule("0 8 * * *")` | `schedule:0 8 * * *` | Cron expression. A bare 5-field cron string passed to `trigger=` is also auto-detected. |
| `every` | `trigger.every("5m")` | `every:5m` | Recurring interval. Accepts `"5m"`, `"1h"`, or seconds as float. |
| `once` | `trigger.once("2026-12-31T23:59:59Z")` | `once:2026-12-31T23:59:59Z` | One-shot at an absolute timestamp. Accepts ISO string, `datetime`, or unix timestamp. |
| `job.success` | `upstream.success` | `job.success:jobs.<section>.<name>` | Fires when the upstream job succeeds. `upstream` is the `JobFactory` returned by the decorator. |
| `job.fail` | `upstream.fail` | `job.fail:jobs.<section>.<name>` | Fires when the upstream job fails. |
| `tag` | `trigger.tag("backfill")` | `tag:backfill` | Broadcast trigger -- fires every job with this tag. Auto-added from `expose.tags`. |
| `manual` | `trigger.manual()` | `manual:jobs.<...>` | User-initiated via CLI or dashboard. Auto-added when `expose.manual=True` (the default). |
| `pipeline_name` | `trigger.pipeline_name("my_pipeline")` | `pipeline_name:my_pipeline` | Fires on completion of a named pipeline run. Auto-added by `@run.pipeline`. |
| `http` | `trigger.http(port=8080, path="/api")` | `http:8080/api` | HTTP endpoint for interactive jobs. Auto-added by `@run.interactive`. |
| `webhook` | `trigger.webhook("/ingest")` | `webhook:/ingest` | Webhook receiver. dltHub exposes the path and forwards POST bodies to the job. *(not yet implemented)* |
| `deployment` | `trigger.deployment()` | `deployment:` | Fires once per code deploy. Useful for migrations or post-deploy validation. *(not yet implemented)* |

### Followup triggers

Any decorated job returns a `JobFactory` with `.success`, `.fail`, and
`.completed` properties that produce trigger strings:

```python
@run.pipeline("ingest_pipeline", trigger=trigger.schedule("0 * * * *"))
def ingest():
    ...

@run.pipeline("transform_pipeline", trigger=ingest.success)
def transform():
    ...

# multiple triggers -- run on schedule OR after ingest succeeds
@run.job(trigger=[trigger.schedule("0 8 * * *"), ingest.success])
def report():
    ...
```

| Property | Fires when |
|----------|------------|
| `job.success` | The job completes successfully |
| `job.fail` | The job fails |
| `job.completed` | A tuple `(success, fail)` -- useful with multiple triggers |

---

## `__deployment__.py` -- the deployment module

The deployment module declares what exists in the workspace. dltHub discovers
jobs by inspecting its contents.

```python
"""My workspace -- ingest and transform customer data"""

__tags__ = ["production", "team:data"]

from my_pipeline import ingest_job, transform_job
import my_notebook
import my_mcp_server

__all__ = ["ingest_job", "transform_job", "my_notebook", "my_mcp_server"]
```

### Import rules

- **Function imports** (`from ... import ingest_job`) produce one job per
  function. The function must be decorated with `@run.job`, `@run.pipeline`,
  or `@run.interactive`.
- **Module imports** (`import my_notebook`) produce one job per module. The
  framework is auto-detected: `marimo.App` -> interactive GUI, `FastMCP`
  instance -> MCP server, `streamlit` usage -> dashboard. A plain module
  with `if __name__ == "__main__"` becomes a batch job.

### Deployment-module dunders

| Dunder | Type | What it does |
|--------|------|--------------|
| `__doc__` | `str` | Workspace description shown in the dltHub dashboard. Only the first non-empty line is used. |
| `__tags__` | `list[str]` | Workspace-level tags applied to the deployment as metadata. |
| `__all__` | `list[str]` | Explicit list of names to deploy. Strongly recommended -- without it the manifest generator scans the full `__dict__` and warns. |

### Defining jobs inline in `__deployment__.py`

You can define decorated jobs directly in the deployment module. Use the
`section=` parameter to give the job a clean config section:

```python
# __deployment__.py
from dlt.hub import run

@run.interactive(
    section="data_tools",
    interface="mcp",
    idle_timeout="30m",
)
def data_mcp():
    from fastmcp import FastMCP
    mcp = FastMCP("data-tools")

    @mcp.tool
    def row_counts() -> dict:
        ...
    return mcp

__all__ = ["data_mcp"]
```

Without `section="data_tools"`, the config section would default to
`__deployment__` (the module name), producing a less readable config path.

---

## Module-level jobs and dunders

Two kinds of modules can be deployed as jobs without any decorator:

1. **Framework-detected modules** -- the manifest generator probes for a
   `marimo.App`, `FastMCP`, or `streamlit` usage at module level and produces
   an interactive job automatically.
2. **Plain Python modules** -- a local `.py` file with no framework usage is
   detected as a batch job that runs the module as `__main__`.

Both kinds honor module-level dunders that override the auto-detected job
definition. Set them at the top of the module file:

| Dunder | Equivalent decorator arg | Effect |
|--------|--------------------------|--------|
| `__doc__` | -- | Module docstring becomes the job description. |
| `__trigger__` | `trigger=` | Extra triggers appended to the detector's defaults. Accepts a single trigger or a list. |
| `__expose__` | `expose=` | Replaces the auto-detected `expose` dict. Use to add `tags`, `starred`, `display_name`, etc. |
| `__require__` | `require=` | Sets the job's `require` spec |

Example -- add a recurring trigger and tags to a marimo notebook:

```python
import marimo
from dlt.hub.run import trigger

__trigger__ = trigger.every("1h")
__expose__ = {"tags": ["report"], "starred": True}

app = marimo.App()
# ... marimo cells ...
```

---

## Auto-set manifest fields

A few fields appear in the deployment manifest without you setting them.
Useful to know when inspecting `dlthub deploy --show-manifest` output:

- **`expose.manual = True`** by default -- adds a `manual:<job_ref>` trigger
  so the job can be launched from the CLI or dashboard. Set
  `expose={"manual": False}` to disable.
- **`expose.category`** -- auto-set by framework detectors (`notebook` for
  marimo, `mcp` for FastMCP, `dashboard` for Streamlit) and by
  `@run.pipeline` (`pipeline`).
- **`expose.tags`** -- each tag automatically adds a `tag:<name>` trigger so
  `dlthub job trigger "tag:..."` works without manual trigger setup.
- **`pipeline_name:`** trigger -- added automatically by `@run.pipeline` so
  `dlthub pipeline run <name>` matches the job.
- **`http:`** trigger -- added automatically by `@run.interactive`.
- **`default_trigger`** -- the manifest generator picks one trigger as the
  primary (prefers `schedule` / `every`; never picks `manual` or
  `deployment`).
- **Workspace dashboard** -- a synthesized `jobs.workspace.dashboard`
  interactive job is auto-included for every `__deployment__` module so the
  workspace is browsable in the dltHub UI.

---

## Job configuration via `dlt.config.value`

Job functions can declare configuration parameters using dlt's standard
config injection -- the same mechanism `dlt.source` / `dlt.resource` use.
Values come from `.dlt/<profile>.config.toml` under the
`[jobs.<section>.<name>]` section.

```python
@run.pipeline(
    my_pipeline,
    interval={"start": DEFAULT_EPOCH},
    trigger=["*/3 * * * *", backfill.success],
)
def daily_ingest(run_context: TJobRunContext, epoch: str = None):
    if run_context["refresh"] and epoch:
        run_context["interval_start"] = datetime.fromisoformat(epoch)
    ...
```

```toml
# .dlt/dev.config.toml -- module-level section applies to all jobs in the module
[jobs.my_module]
epoch = "2026-04-05T00:00:00+00:00"
```

The config section `[jobs.my_module]` matches the **module name** and applies
to every decorated function in `my_module.py`. You can also target individual
jobs with `[jobs.my_module.daily_ingest]`. Per-job sections override
module-level sections.

Both `<section>` and `<name>` default to the module name and function name
respectively, and can be overridden via the `section=` and `name=` decorator
arguments.

---

## Not yet implemented

Several arguments and features are reserved for future use and currently
not implemented in dltHub.

### Decorator arguments

| Argument | Type | Default | Used by | What it does |
|----------|------|---------|---------|--------------|
| `allow_external_schedulers` | `bool` | `False` | `@job`, `@pipeline` | When `True`, dlt incrementals join the runner-provided interval automatically. Use with `interval=` for backfills. |

### Triggers

| Type | Constructor | Manifest form | Notes |
|------|-------------|---------------|-------|
| `webhook` | `trigger.webhook("/ingest")` | `webhook:/ingest` | Webhook receiver -- dltHub exposes the path and forwards POST bodies to the job. |
| `deployment` | `trigger.deployment()` | `deployment:` | Fires once per code deploy. Useful for migrations or post-deploy validation. |

### Require arguments

| Key | Type | What it does |
|-----|------|--------------|
| `provider` | `str` | Infra provider identifier (e.g. `"modal"`). dltHub default when unset. |
| `machine` | `str` | Machine spec identifier (e.g. `"gpu-a100"`, `"2xlarge"`). |
| `region` | `str` | Runner region for placement (e.g. `"us-east-1"`). |
