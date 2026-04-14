# dltHub Runtime Tutorial

This tutorial walks through four workspaces of increasing complexity. Each workspace
is self-contained -- you can jump to any chapter, but the concepts build on each other:

1. **Single Ingestion Pipeline** (`fruitshop_simple_workspace`) -- a plain dlt pipeline deployed with CLI commands
2. **Jobs and Deployments** (`github_ingest_workspace`) -- job decorators, triggers, and manifest-based deployment
3. **Transformation Pipelines** (`jaffle_shop_workspace`) -- connected pipelines with followup jobs
4. **Incremental Pipelines** (`usgs_earthquakes_workspace`) -- freshness constraints, backfill, and refresh cascade

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- A [dltHub Runtime](https://dlthub.com) account
- (Optional) A [MotherDuck](https://motherduck.com) account for cloud destinations


> You must use runtime from this branch:
> You must setup runtime client to use locally running backend
```toml
[runtime]
log_level="WARNING"  # the system log level of dlt
# use the dlthub_telemetry setting to enable/disable anonymous usage data reporting, see https://dlthub.com/docs/reference/telemetry
dlthub_telemetry = false
api_base_url = "https://dlthub.test/api/api"
auth_base_url = "https://dlthub.test/api/auth"
```
> you must install runtime client: **uv pip install -e ../../runtime/cli**

## Workspace Setup

Each workspace is an independent Python project. To work with one:

```sh
cd <workspace_name>
uv sync
source .venv/bin/activate
```

### Credentials

Every workspace uses **named destinations** (e.g. `fruitshop_destination`, `warehouse`)
that resolve to different backends depending on the active **profile**:

| Profile   | Config file           | Secrets file           | Destination | Use case |
|-----------|----------------------|----------------------|-------------|----------|
| `dev`     | `dev.config.toml`    | `secrets.toml`       | DuckDB (local) | Local development |
| `prod`    | `prod.config.toml`   | `prod.secrets.toml`  | MotherDuck   | Batch jobs on Runtime |
| `access`  | `access.config.toml` | `access.secrets.toml`| MotherDuck (read-only) | Interactive notebooks on Runtime |

The `dev` profile is active by default. When you run a batch job on Runtime, it uses
`prod`. When you serve an interactive notebook, it uses `access`.

To configure MotherDuck credentials, create the secrets files in each workspace's
`.dlt/` directory:

**`prod.secrets.toml`** (read/write):
```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-service-token"
```

**`access.secrets.toml`** (read-only):
```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-read-only-token"
```

> Secrets files (`*.secrets.toml`, `secrets.toml`) are gitignored. Never commit them.
> Runtime stores your secrets securely when you sync your configuration.

### Connecting to Runtime

Authenticate once from any workspace:

```sh
uv run dlt runtime login
```

This opens a browser for GitHub OAuth and links your local workspace to your
dltHub Runtime account.

---

## Chapter 1: Single Ingestion Pipeline

**Workspace**: `fruitshop_simple_workspace`

This is the simplest possible dlt workspace. It contains a pipeline that loads
locally-generated data and a marimo notebook for exploring it. There are no
job decorators and no deployment module -- everything is a regular Python script.

This is how you'd deploy an existing dlt project to Runtime without changing any code.

### What's inside

```
fruitshop_simple_workspace/
  fruitshop_pipeline.py      # pipeline that loads fruitshop data
  fruitshop_notebook.py      # marimo notebook for data exploration
  pyproject.toml
  .dlt/
    config.toml              # base config (duckdb destination)
    dev.config.toml          # dev profile
    prod.config.toml         # prod profile (motherduck)
    access.config.toml       # access profile (motherduck, read-only)
```

### The pipeline

`fruitshop_pipeline.py` defines four resources -- `customers`, `inventory_categories`,
`inventory`, and `purchases` -- grouped into a `fruitshop()` source. The pipeline
loads them into a named destination called `fruitshop_destination`:

```python
p = dlt.pipeline(
    pipeline_name="fruitshop",
    destination="fruitshop_destination",
    dataset_name="fruitshop_data",
)
load_info = p.run(fruitshop())
```

The destination name `fruitshop_destination` resolves to DuckDB or MotherDuck
depending on which profile is active.

### Run locally

```sh
cd fruitshop_simple_workspace
uv sync
uv run python fruitshop_pipeline.py
```

This runs with the `dev` profile -- data goes into a local DuckDB file.

### Deploy and run on Runtime

Upload your code and run the pipeline as a batch job:

```sh
uv run dlt runtime launch fruitshop_pipeline.py
```

This single command:
1. Syncs your code and configuration to Runtime
2. Creates a batch job from the script and starts it

The batch job runs with the `prod` profile, so data goes to MotherDuck (or
whichever cloud destination you configured in `prod.config.toml`).

Add `-f` to follow logs in your terminal until the run completes:

```sh
uv run dlt runtime launch fruitshop_pipeline.py -f
```

> **How it works**: `launch` and `serve` accept a Python file name as a
> convenience. Under the hood, the CLI generates a single-job deployment manifest
> from that file and syncs it to Runtime. This is called an **ad-hoc deploy** --
> no `__deployment__` module is needed. When a workspace grows beyond one or two
> scripts, you'll want a proper deployment module instead (see Chapter 2).

### Serve the notebook

Deploy the marimo notebook as an interactive app:

```sh
uv run dlt runtime serve fruitshop_notebook.py
```

This deploys the notebook with the `access` profile (read-only credentials),
waits until it's running, and opens it in your browser.

### Monitor

```sh
# open the Runtime web dashboard
uv run dlt runtime dashboard

# workspace deployment overview
uv run dlt runtime info

# list all jobs
uv run dlt runtime job list

# stream logs for the latest run
uv run dlt runtime logs fruitshop_pipeline -f

# cancel a stuck run
uv run dlt runtime cancel fruitshop_pipeline
```

### How profiles work

When you run a script locally, dlt uses the base `config.toml` merged with
`dev.config.toml` -- the dev profile is the default.

When Runtime runs a **batch job** (via `launch`), it uses the `prod`
profile: `config.toml` + `prod.config.toml` + `prod.secrets.toml`.

When Runtime runs an **interactive job** (via `serve`), it uses the `access` profile:
`config.toml` + `access.config.toml` + `access.secrets.toml`.

This separation ensures batch pipelines have write access while notebooks only
get read-only credentials.

### Limitations of ad-hoc deployment

Using `launch` and `serve` with a script file is the quickest way to get code
running on Runtime. However, it creates jobs one at a time and doesn't support:

- Scheduled triggers (cron, every N minutes)
- Followup jobs (run B after A succeeds)
- Freshness constraints
- Deploying the entire workspace as a single unit

For all of these, you need **job decorators** and a **deployment module** --
which is what the next chapter introduces.

---

## Chapter 2: Jobs and Deployments

**Workspace**: `github_ingest_workspace`

This workspace loads commits and contributors from the GitHub REST API. It
introduces the three building blocks of manifest-based deployment:

- **Job decorators** that attach scheduling and metadata to Python functions
- **`__deployment__.py`** that declares which jobs exist in the workspace
- **`dlt runtime deploy`** that syncs the entire job graph to Runtime in one step

### What's inside

```
github_ingest_workspace/
  github_pipeline.py                  # ingestion job with @pipeline decorator
  github_dq_pipeline.py               # data quality batch job with @job decorator
  github_transformations_notebook.py   # marimo: transformation design workflow
  github_dq_notebook.py               # marimo: interactive DQ exploration
  github_report_notebook.py           # marimo: commit analytics dashboard
  __deployment__.py                   # deployment module
  pyproject.toml
  .dlt/
    config.toml
    dev.config.toml
    prod.config.toml
    access.config.toml
```

### The batch jobs

- **`github_pipeline.py`** (`load_commits`) -- ingests commits and contributors
  from the GitHub REST API into the `warehouse` destination. Runs every 5 minutes
  via `trigger.every("5m")`. All resources use replace mode (full refresh each run).

- **`github_dq_pipeline.py`** (`run_dq_checks`) -- validates the ingested data
  by running a suite of checks (non-null keys, valid contributor types, positive
  contribution counts). The job **fails** if any check has failures, making it
  visible in the Runtime dashboard. Runs hourly via a cron schedule.

### The notebooks

This workspace includes three marimo notebooks, deployed as interactive jobs.
The **transformations notebook** and the **report notebook** form a pipeline:
the transformations notebook produces an analytics-ready `commits` table that
the report notebook reads.

- **`github_transformations_notebook.py`** -- a step-by-step tutorial that
  ingests raw GitHub data, explores the normalized schema, and builds a
  flattened `commits` transformation using Ibis. It runs both the ingest and
  transform pipelines inside the notebook, producing a `github_transform`
  dataset. This notebook is the prerequisite for the report notebook.

- **`github_report_notebook.py`** -- an analytics dashboard built on the
  transformed data from `github_transform`. Shows commit activity over time,
  top contributors, merge ratio trends, commit timing heatmaps, and message
  length distributions. Requires the transformations notebook to have been
  run first.

- **`github_dq_notebook.py`** -- interactive exploration of data quality
  metrics and checks on the raw ingestion data. Demonstrates `dq.with_metrics()`
  for profiling (null rates, unique counts, averages) and `dq.CheckSuite` for
  row-level validation. Works independently of the other two notebooks -- it
  only needs the ingestion pipeline to have run.

### Job decorators

Instead of using `launch` with a file name, you can decorate functions with
`@job` or `@pipeline` from `dlt.hub.run`. The decorator attaches metadata --
triggers, tags, display name -- that Runtime uses to schedule and present the job.

dlt.hub.run provides two decorators for batch jobs:

- **`@job`** -- a general-purpose batch job (any Python function)
- **`@pipeline`** -- a batch job bound to a named `dlt.pipeline`

Both produce the same kind of job. `@pipeline` is a convenience that associates
the job with a specific pipeline name, so Runtime can link telemetry and datasets.

Note: we'll have fine grained retries and other goodies on `@pipeline` soon

Here's the ingestion job in `github_pipeline.py`:

```python
from dlt.hub import run
from dlt.hub.run import trigger

@run.pipeline(
    "github_pipeline",
    trigger=trigger.every("5m"),
    expose={"tags": ["ingest"], "display_name": "GitHub commits ingest"},
)
def load_commits():
    """Load commits and contributors from the GitHub REST API."""
    github_pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )
    load_info = github_pipeline.run(github_rest_api_source())
    print(load_info)
```

And the data quality batch job in `github_dq_pipeline.py`:

```python
from dlt.hub import run
from dlt.hub.run import trigger

@run.job(
    trigger=trigger.schedule("0 * * * *"),
    expose={"display_name": "GitHub data quality"},
)
def run_dq_checks():
    """Run data quality checks on GitHub data. Fails if any check has failures."""
    # ... check logic ...
    if not all_passed:
        raise RuntimeError("Data quality checks failed")
```

### Triggers

A trigger tells Runtime **when** to run a job. You can pass a trigger (or a list
of triggers) to any job decorator.

| Trigger | Meaning |
|---------|---------|
| `trigger.every("5m")` | Run every 5 minutes |
| `trigger.schedule("0 * * * *")` | Cron expression (every hour at :00) |
| `trigger.schedule("0 8 * * *")` | Cron expression (daily at 8am UTC) |
| `"*/5 * * * *"` | Shorthand -- bare cron string, auto-detected |

Triggers declared in code are the **source of truth**. There is no separate
CLI command to add or remove schedules -- change the decorator, redeploy.

### Tags

Tags are labels attached to jobs via the `expose` parameter. They serve two
purposes:

1. **Organization** -- group related jobs in the Runtime dashboard
2. **Bulk operations** -- trigger, list, or cancel all jobs sharing a tag

For example, the ingestion job above has `tags: ["ingest"]`. In a larger
workspace you might tag all your ingestion jobs the same way, then trigger
them all at once:

```sh
# trigger every job tagged "ingest"
uv run dlt runtime trigger "tag:ingest"
```

This is useful for backfill scenarios: tag all your ingestion jobs with
`"backfill"`, then `dlt runtime trigger "tag:backfill"` fires them all in one
command.

### The deployment module

`__deployment__.py` is a Python module that declares everything deployable in
the workspace. Runtime discovers jobs by inspecting its contents:

```python
"""GitHub ingest workspace -- loads and monitors GitHub API data"""

from github_pipeline import load_commits
from github_dq_pipeline import run_dq_checks

import github_transformations_notebook
import github_dq_notebook
import github_report_notebook

__all__ = [
    "load_commits",
    "run_dq_checks",
    "github_transformations_notebook",
    "github_dq_notebook",
    "github_report_notebook",
]
```

The rules are straightforward:

- **Function imports** (`from ... import load_commits`) produce one job per
  function. The function must be decorated with `@job`, `@pipeline`, or
  `@interactive`.
- **Module imports** (`import github_report_notebook`) produce one job per module.
  The framework is auto-detected: marimo notebooks become interactive GUI jobs,
  FastMCP modules become MCP tool servers, Streamlit apps become dashboards.
- **`__all__`** lists exactly which names to deploy. Only listed names are
  included.
- **`__doc__`** (the module docstring) becomes the workspace description visible
  in the Runtime dashboard.

### Deploying with `dlt runtime deploy`

This is the central command for manifest-based deployment. It reads your
`__deployment__.py`, generates a deployment manifest, and syncs it to Runtime:

```sh
cd github_ingest_workspace
uv run dlt runtime deploy
```

The deploy command:
1. Imports `__deployment__.py` and collects all job definitions
2. Generates a deployment manifest (a JSON document describing every job,
   its triggers, entry points, and metadata)
3. Syncs your code and configuration to Runtime
4. Sends the manifest to Runtime for **reconciliation**

#### Reconciliation

Runtime compares the new manifest against the currently deployed jobs and
classifies each one:

| Status | Meaning |
|--------|---------|
| **added** | New job -- will be created |
| **updated** | Job definition changed -- will be updated |
| **unchanged** | No changes -- left as-is |
| **archived** | Job was in the previous manifest but not in this one -- triggers disabled, history preserved |

This is declarative: you describe what should exist, Runtime figures out the diff.
Removing a job from `__deployment__.py` doesn't delete it -- it archives it, preserving
run history and logs.

#### Preview before deploying

```sh
# see what would change without applying
uv run dlt runtime deploy --dry-run

# dump the full expanded manifest as YAML
uv run dlt runtime deploy --show-manifest
```

### Running jobs

After deploying, jobs with triggers run automatically on their schedule. You can
also run them manually:

```sh
# launch a specific job by name
uv run dlt runtime launch load_commits

# launch with log streaming
uv run dlt runtime launch load_commits -f

# trigger all jobs tagged "ingest"
uv run dlt runtime trigger "tag:ingest"

# trigger all jobs that have a schedule trigger
uv run dlt runtime trigger "schedule:*"

# preview which jobs would be triggered (without creating runs)
uv run dlt runtime trigger "tag:ingest" --dry-run

# serve one of the notebooks
uv run dlt runtime serve github_report_notebook
```

The `trigger` command accepts **selectors** -- fnmatch patterns that match
against job triggers and tags. This lets you fire groups of jobs without naming
each one.

### Monitoring and managing jobs

```sh
# list all deployed jobs
uv run dlt runtime job list

# list only jobs tagged "ingest"
uv run dlt runtime job "tag:ingest" list

# detailed info for a specific job
uv run dlt runtime job load_commits info 

# stream logs
uv run dlt runtime logs load_commits -f

# cancel the latest run of a specific job
uv run dlt runtime cancel load_commits

# cancel all running jobs matching a selector
uv run dlt runtime cancel "tag:ingest"

# workspace deployment overview
uv run dlt runtime info
```

### What's next

This workspace runs each job independently -- the DQ checks don't know whether
ingestion has finished. In the next chapter, we connect pipelines with
**followup triggers** so that transformations run automatically after ingestion
succeeds.

---

## Chapter 3: Transformation Pipelines

**Workspace**: `jaffle_shop_workspace`

This workspace demonstrates how to connect multiple pipelines so they run as a
chain. An ingestion pipeline loads raw data from the Jaffle Shop API and a local
parquet file; a transformation pipeline computes customer-level aggregations and
loads them into the remote warehouse. The transformation runs **automatically**
after ingestion succeeds -- no polling, no cron guessing.

### What's inside

```
jaffle_shop_workspace/
  jaffle_ingestion.py         # ingest job: REST API + parquet -> local DuckDB
  jaffle_transformations.py   # transform job: Ibis aggregations -> remote warehouse
  jaffle_mcp.py               # interactive MCP server exposing a row_counts tool
  payments.parquet            # sample payments data
  __deployment__.py
  pyproject.toml
  .dlt/
    config.toml
    dev.config.toml
    prod.config.toml
    access.config.toml
```

### The data flow

```
jaffle_ingestion.py                     jaffle_transformations.py
┌────────────────────┐                  ┌────────────────────────┐
│  REST API           │                  │  customer_orders       │
│  (customers,        │   on success     │  customer_payments     │
│   products, orders) ├─────────────────►│                        │
│  + payments.parquet │                  │  -> remote warehouse   │
│  -> local DuckDB    │                  │     (MotherDuck)       │
└────────────────────┘                  └────────────────────────┘
  schedule: hourly                        trigger: ingest.success
```

Both jobs use **replace** mode -- every run is a full refresh. This keeps the
setup simple: no incremental cursors, no merge keys, no stale-data concerns.

### Followup triggers

The key concept in this workspace is the **followup trigger**. Instead of giving
the transform job its own schedule, we tell it to run whenever ingestion succeeds:

```python
from jaffle_ingestion import ingest_jaffle

@run.pipeline(
    "jaffle_transform",
    trigger=ingest_jaffle.success,
    expose={"display_name": "Jaffle Shop transform"},
)
def transform_jaffle():
    ...
```

`ingest_jaffle.success` is a trigger string that resolves to
`job.success:jobs.jaffle_ingestion.ingest_jaffle`. When Runtime sees ingestion
complete successfully, it immediately fires the transform.

Every decorated job (a `JobFactory` instance) exposes these trigger properties:

| Property | Fires when |
|----------|------------|
| `job.success` | The job completes successfully |
| `job.fail` | The job fails |
| `job.completed` | A tuple `(success, fail)` -- useful with multiple triggers |

### Multiple triggers

A job can have more than one trigger. Pass a list:

```python
@run.pipeline(
    "jaffle_transform",
    trigger=[trigger.schedule("0 8 * * *"), ingest_jaffle.success],
)
def transform_jaffle():
    ...
```

This job runs either on its daily 8am schedule **or** immediately after ingestion
succeeds -- whichever comes first.

### Knowing which trigger fired

When a job has multiple triggers, you might need to know which one caused the
current run. Declare a `run_context` parameter:

```python
from dlt.hub.run import TJobRunContext

@run.pipeline(
    "jaffle_transform",
    trigger=[ingest_jaffle.success, some_other_job.success],
)
def transform_jaffle(run_context: TJobRunContext):
    if run_context["trigger"] == ingest_jaffle.success:
        print("Triggered by ingestion")
    else:
        print("Triggered by something else")
```

`TJobRunContext` is a dict injected by the Runtime launcher. It includes:

| Key | Type | Description |
|-----|------|-------------|
| `run_id` | `str` | Unique identifier for this run |
| `trigger` | `str` | The trigger string that fired (e.g. `job.success:jobs.jaffle_ingestion.ingest_jaffle`) |
| `refresh` | `bool` | Whether this run carries a refresh signal (see Chapter 4) |

### Execution constraints: timeout and grace period

The `execute` parameter controls how Runtime manages a running job. The most
common setting is a **timeout** -- a maximum wall-clock duration after which
Runtime terminates the run.

The ingest job in this workspace has a 6-hour timeout using the string shorthand:

```python
@run.pipeline(
    jaffle_ingest_pipe,
    trigger=trigger.schedule("0 * * * *"),
    execute={"timeout": "6h"},
)
def ingest_jaffle():
    ...
```

The transform job uses the full dict form with a custom grace period (2 hours
timeout, 1 minute grace):

```python
@run.pipeline(
    "jaffle_transform",
    trigger=ingest_jaffle.success,
    execute={"timeout": {"timeout": 7200, "grace_period": 60}},  # 2h, 1min grace
)
def transform_jaffle():
    ...
```

The `timeout` field accepts either a human-readable string (`"6h"`, `"30m"`,
`"90s"`) or a `TTimeoutSpec` dict with `timeout` and `grace_period` in seconds.

When the timeout expires, Runtime sends a termination signal to the job process.
The **grace period** is the window for the job to finish in-flight work (flush
buffers, commit pending loads) before Runtime hard-kills the process. If the job
exits cleanly within the grace period, the run counts as a normal completion.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | `float` | none | Max wall-clock duration in seconds |
| `grace_period` | `float` | 30.0 | Seconds for graceful shutdown before hard kill |

There are no retries at the platform level -- retry logic belongs in your
pipeline code (e.g. via `dlt`'s built-in retry support), where you know whether
a partial load is safe to resume.

### Transformations with `@dlt.hub.transformation`

The transformation functions use the `@dlt.hub.transformation` decorator, which
marks an Ibis expression (or SQL query) as a dlt resource that reads from an
existing dataset:

```python
@dlt.hub.transformation(write_disposition="replace")
def customer_orders(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Aggregate statistics about previous customer orders."""
    orders = dataset.table("orders").to_ibis()
    yield orders.group_by("customer_id").aggregate(
        first_order=orders.ordered_at.min(),
        most_recent_order=orders.ordered_at.max(),
        number_of_orders=orders.id.count(),
    )
```

Multiple transformations can be grouped into a source and run through a pipeline:

```python
@dlt.source
def customers_metrics(raw_dataset: dlt.Dataset) -> list:
    return [
        customer_orders(raw_dataset),
        customer_payments(raw_dataset),
    ]

jaffle_transform_pipe = dlt.pipeline(
    "jaffle_transform",
    destination="jaffleshop_transformation_destination",
)
jaffle_transform_pipe.run(
    customers_metrics(jaffle_ingest_pipe.dataset())
)
```

When source and destination share the same engine (both DuckDB, or both
MotherDuck), dlt executes the query as pure SQL with zero data movement.

### An interactive MCP server

`jaffle_mcp.py` adds a small FastMCP server with a `row_counts` tool that
returns the number of rows in every table across both datasets. A module-level
`FastMCP` instance is auto-detected by the manifest generator -- just import
the module in `__deployment__.py` and it's deployed as an interactive MCP job
with an `http:` trigger. Serve it with `dlt runtime serve jaffle_mcp`.

### Job configuration

Jobs read configuration through dlt's standard config system under the
`[jobs.<section>.<spec>]` section. The MCP server in this workspace is a good
example -- we switch its transport to SSE in `.dlt/config.toml`:

```toml
[jobs.jaffle_mcp.mcp]
transport = "sse"
```

`jaffle_mcp` is the module name (the section), `mcp` is the launcher's config
spec. Because config files are profile-aware, you can override the transport
per profile by adding the same section to `dev.config.toml` or `prod.config.toml`.
Any job-specific settings your code reads from `dlt.config.value` follow the
same pattern.

### Deploy and run

```sh
cd jaffle_shop_workspace
uv sync

# preview the deployment
uv run dlt runtime deploy --dry-run

# deploy
uv run dlt runtime deploy

# run pipeline using pipeline name
uv dlt runtime run-pipeline jaffle_ingest -f
```

After ingestion completes, watch for the transform to start:

```sh
uv run dlt runtime logs transform_jaffle -f
```

### What's next

All jobs in this workspace use replace mode -- every run loads everything from
scratch. In the next chapter, we add **incremental loading** with cursor-based
pagination, **freshness constraints** that prevent transforms from running on
stale data, and a **backfill** mechanism that cascades a full refresh through
the entire pipeline graph.

---

## Chapter 4: Incremental Pipelines

**Workspace**: `usgs_earthquakes_workspace`

This workspace ingests real-time earthquake data from the USGS, transforms it
into analytics tables, and ships a dashboard. It demonstrates dlt's most
advanced deployment features: incremental loading with cursor pushdown, freshness
constraints between jobs, backfill with refresh cascade, and dependency groups
for per-job package requirements.

### What's inside

```
usgs_earthquakes_workspace/
  usgs/
    __init__.py               # dlt source: earthquakes (incremental) + feeds_summary (replace)
    settings.py               # API URLs and constants
    transformations.py        # Ibis transformations: daily stats + severity classification
  usgs_pipeline.py            # 5 jobs: backfill, daily, 2 transforms, clock
  usgs_dashboard.py           # marimo dashboard with 6 charts
  utils.py                    # restore_incremental helper
  __deployment__.py
  pyproject.toml
  .dlt/
    config.toml
    dev.config.toml
    prod.config.toml
    access.config.toml
```

### The job graph

```
backfill_usgs (manual, refresh="always")
  ══╦══> usgs_daily (cron */3 + followup, freshness gate)
    ║      ┆
    ║      ┆ freshness: usgs_daily.is_fresh
    ║      ┆
    ║      ├╌╌╌> transform_earthquakes (every 5m, incremental)
    ║      └╌╌╌> transform_feeds_summary (every 5m, replace)
    ║
clock (detached 5-minute heartbeat)
```

- **Solid arrow** (`══>`) is a trigger + freshness gate: the daily job fires on
  its own cron **and** immediately after backfill succeeds, but only after
  backfill has completed at least once.
- **Dotted arrows** (`╌╌>`) are freshness gates only: transforms run on their
  own 5-minute cron but wait until the most recent ingest interval is complete
  before processing. This prevents transforms from observing half-loaded data.

### Incremental loading

The `earthquakes` resource uses `dlt.sources.incremental` to track where it
left off between runs:

```python
@dlt.resource(write_disposition="merge", primary_key="id")
def earthquakes(
    time: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "time",
        initial_value=USGS_EPOCH,
        range_end="closed",
    ),
) -> Iterable[TDataItem]:
    params = {
        "starttime": time.start_value.to_iso8601_string(),
        ...
    }
```

Key details:

- **Cursor type is `pendulum.DateTime`** (not `str`). This drives how the
  cursor values are used in downstream Ibis filters -- they remain typed
  timestamps, not strings.
- **Cursor pushdown**: `time.start_value` and `time.end_value` are passed
  directly to the FDSN API as `starttime`/`endtime`, so only the needed
  slice is fetched.
- **`merge` + `primary_key="id"`**: earthquake records can be revised by USGS.
  Merge-by-id updates existing rows in place.
- **`range_end="closed"`**: both endpoints inclusive, matching the FDSN
  service's own `endtime` semantics.

The `feeds_summary` resource is the opposite: `write_disposition="replace"` with
no cursor. The "significant events of the past 30 days" feed is a small snapshot
rebuilt each run.

### Freshness constraints

A freshness constraint is a **gate**: "don't run me until the upstream job's
last completed interval has fully covered my scheduled interval."

```python
@run.pipeline(
    usgs_ing_pipeline,
    trigger=["*/3 * * * *", backfill_usgs.success],
    freshness=backfill_usgs.is_fresh,
    name="usgs_daily_load",
)
def usgs_daily(run_context: TJobRunContext):
    ...
```

Freshness is **not** a trigger. The distinction matters:

| Concept | Semantics |
|---------|-----------|
| **Trigger** (`trigger=upstream.success`) | "Run me immediately when upstream finishes" (event-driven) |
| **Freshness** (`freshness=upstream.is_fresh`) | "Run me on my own schedule, but only when upstream is done" (gate) |

In this workspace:

- `usgs_daily` declares `freshness=backfill_usgs.is_fresh` -- the cron-driven
  load is gated on backfill having completed at least once. After backfill is
  fresh, the cron fires every 3 minutes regardless.
- Both transform jobs declare `freshness=[usgs_daily.is_fresh]` -- even though
  they have their own 5-minute cron, they wait until the most recent ingest
  interval is complete. This prevents `transform_feeds_summary` from observing
  the brief mid-load window where `feeds_summary` (a replace resource) has been
  dropped but not yet rewritten.

### Refresh cascade

The `refresh=` parameter on a job decorator controls how a **refresh signal
propagates downstream**. When a job runs with `run_context["refresh"] == True`,
it's responsible for reacting to that flag in its function body.

| Policy | Behavior |
|--------|----------|
| `refresh="always"` | Every successful run cascades a refresh signal to all downstream jobs (originator) |
| `refresh="auto"` (default) | Passes through if received, otherwise no-op (transparent) |
| `refresh="block"` | Stops the signal -- downstream jobs never receive it |

The backfill job is the cascade originator:

```python
@run.pipeline(
    "usgs_ingest_pipeline",
    expose={"tags": ["backfill"], "display_name": "USGS backfill cascade"},
    refresh="always",
)
def backfill_usgs(run_context: TJobRunContext):
    usgs_ing_pipeline.refresh = "drop_sources"
    _load_ingest(["earthquakes", "feeds_summary"])
```

When backfill succeeds, Runtime clears `prev_completed_run` on all reachable
downstream jobs (BFS walk, stopped by `block` policies). Those jobs then start
with `run_context["refresh"] = True` and react accordingly:

| Job | `refresh=` | Reaction in body |
|-----|------------|------------------|
| `backfill_usgs` | `always` | Sets `pipeline.refresh = "drop_sources"` to wipe all tables and incremental cursor |
| `usgs_daily` | `auto` | No-op -- backfill already dropped sources upstream |
| `transform_earthquakes` | `auto` | Sets `pipeline.refresh = "drop_resources"` and passes `time_window=None` to rebuild from full catalog |
| `transform_feeds_summary` | `auto` | Sets `pipeline.refresh = "drop_resources"` -- the replace transform naturally rebuilds |

**Why `drop_sources` vs `drop_resources`:**

- `drop_sources` (ingest) wipes all tables **and** the source-level state
  including the incremental cursor. Next run starts at `USGS_EPOCH`.
- `drop_resources` (transforms) drops only the transform's output table. The
  upstream pipeline's incremental cursor is untouched, so the transform can
  re-read it on the very next run.

### Incremental transforms

`transform_earthquakes` doesn't have its own incremental cursor. Instead it uses
`restore_incremental()` to read the upstream pipeline's persisted state and pass
the time window to the Ibis transformation:

```python
incremental = restore_incremental(
    usgs_ing_pipeline,
    usgs_source().earthquakes,
    dlt.sources.incremental[pendulum.DateTime](
        "time", initial_value=USGS_EPOCH, range_end="closed",
    ),
)
if incremental is None:
    return  # ingestion has not run yet

if run_context["refresh"]:
    usgs_eq_stats_pipeline.refresh = "drop_resources"
    time_window = None                    # rebuild everything
else:
    time_window = (incremental.start_value, incremental.last_value)

usgs_eq_stats_pipeline.run(
    earthquake_daily_stats(usgs_ing_pipeline.dataset(), time_window)
)
```

The Ibis transformation applies the window as a filter:

```python
@dlt.hub.transformation(
    table_name="earthquake_daily_stats",
    write_disposition="merge",
    primary_key=["day", "region"],
)
def earthquake_daily_stats(
    dataset: dlt.Dataset,
    time_window: Optional[Tuple[pendulum.DateTime, pendulum.DateTime]] = None,
) -> typing.Iterator[ir.Table]:
    eq = dataset.table("earthquakes").to_ibis()
    if time_window is not None:
        start, end = time_window
        eq = eq.filter((eq.time >= start) & (eq.time <= end))
    yield (
        eq.mutate(day=eq.time.cast("date"), region=...)
        .group_by(["day", "region"])
        .aggregate(event_count=ibis._.count(), ...)
    )
```

The `merge` disposition with `primary_key=["day", "region"]` makes overlapping
windows idempotent -- partial-day rows on slice boundaries are updated in place.

### Dependency groups

The transform jobs need `ibis-framework` but the ingest jobs don't. Rather than
adding ibis to the workspace's main dependencies, we declare it as a
**dependency group** in `pyproject.toml`:

```toml
[dependency-groups]
ibis = ["ibis-framework[duckdb]"]
```

Then tell Runtime to install it only for the jobs that need it:

```python
@run.pipeline(
    usgs_eq_stats_pipeline,
    trigger=trigger.every("5m"),
    freshness=[usgs_daily.is_fresh],
    require={"dependency_groups": ["ibis"]},
)
def transform_earthquakes(run_context: TJobRunContext):
    ...
```

Runtime composes the execution environment from the workspace's base dependencies
plus the job's declared `dependency_groups`. Ingest jobs get a leaner environment
without ibis, while transform jobs get the full analytical stack.

### Deploy and run

```sh
cd usgs_earthquakes_workspace
uv sync

# preview the deployment
uv run dlt runtime deploy --dry-run

# deploy the full job graph
uv run dlt runtime deploy

# kick off the initial backfill -- cascades refresh to all downstream jobs
uv run dlt runtime trigger "tag:backfill"

# after backfill completes, cron takes over:
#   usgs_daily fires every 3 minutes
#   transforms fire every 5 minutes (gated on daily freshness)

# monitor
uv run dlt runtime logs backfill_usgs -f
uv run dlt runtime logs transform_earthquakes -f

# force a full refresh at any time
uv run dlt runtime launch backfill_usgs --refresh
```
