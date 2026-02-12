# Tutorial: dltHub Transformations with GitHub Data

This tutorial walks through building **dlt transformations** on top of raw GitHub API data. You'll learn how to ingest data with nested structures, inspect the raw schema, write transformations that leverage parent-child table relationships, and visualize the results in a marimo notebook.

By the end you'll have three clean, analytics-ready tables derived from two GitHub API endpoints.

## What are dlt transformations?

[dlt transformations](https://dlthub.com/docs/hub/features/transformations) let you build new tables from data that has already been ingested with `dlt`. You write them with the `@dlt.hub.transformation` decorator — same signature as `@dlt.resource`, but instead of yielding raw data, you yield an ibis expression or SQL query that reshapes existing tables.

Key benefits:
- **No data movement** — when source and destination are the same database, dlt executes the transformation as pure SQL.
- **Schema evolution** — dlt automatically detects the output schema from your query.
- **Same write dispositions** — `append`, `replace`, `merge` all work just like regular resources.

## Recommended approach: local-first transformations

We recommend a **two-stage pipeline** where you load raw data to a **local DuckDB** first, run transformations locally, and only push the clean, transformed tables to your warehouse:

```
GitHub API  →  local DuckDB (raw data)  →  transform locally  →  warehouse (clean tables only)
```

**Why this approach?**

1. **Lower warehouse costs** — raw tables with 61+ columns and thousands of rows never hit your warehouse. You only load the slim, aggregated results.
2. **Faster iteration** — transformations run against a local DuckDB file, so you get sub-second feedback while developing. No waiting for warehouse round-trips.
3. **Cheaper compute** — aggregations, joins, and filters execute on your local machine (or CI runner), not on warehouse compute credits.
4. **Clean warehouse** — your warehouse only contains the tables your analysts and dashboards actually need, not raw API dumps.

This is exactly the pattern used in this tutorial: `github_ingest` pipeline loads to local DuckDB, `github_transform` pipeline reads from that local dataset and loads transformed results to the warehouse destination.

## Prerequisites

- Python 3.10+
- A GitHub personal access token (stored in `.dlt/secrets.toml`)
- The `dlt`, `ibis-framework`, `duckdb`, `marimo`, and `altair` packages installed

```bash
uv pip install dlt ibis-framework duckdb marimo altair
```

## Step 1: Define the data source

We start with `github_pipeline.py`, which defines a `dlt.source` that pulls from two GitHub REST API endpoints: **commits** and **contributors**.

```python
@dlt.source
def github_rest_api_source(
    owner: str = "dlt-hub",
    repo: str = "dlt",
    access_token: str = dlt.secrets.value,
):
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {"type": "bearer", "token": access_token},
            "paginator": {"type": "header_link"},
        },
        "resources": [
            {
                "name": "commits",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/commits",
                    "params": {"owner": owner, "repo": repo},
                },
            },
            {
                "name": "contributors",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/contributors",
                    "params": {"owner": owner, "repo": repo},
                },
            },
        ],
    }
    yield from rest_api_resources(config)
```

We chose only two endpoints deliberately — even with just two, dlt's automatic normalization produces enough structure to demonstrate meaningful transformations.

## Step 2: Ingest and inspect the raw data

Run the ingestion step from `github_transformations.py`:

```python
destination = dlt.destinations.duckdb("local_github.duckdb")
github_ingest_pipe = dlt.pipeline("github_ingest", destination=destination)
ingest_load_info = github_ingest_pipe.run(github_rest_api_source())
```

After ingestion, dlt has created **three** tables — not two:

| Raw Table | Rows | Columns | Description |
|---|---|---|---|
| `commits` | ~4,097 | 61 | Parent table — one row per commit |
| `commits__parents` | ~4,570 | 6 | **Child table** — parent SHAs per commit |
| `contributors` | ~154 | 22 | Standalone table — one row per contributor |

### Where did `commits__parents` come from?

The GitHub API returns each commit with a nested `parents` array:

```json
{
  "sha": "abc123...",
  "parents": [
    { "sha": "def456...", "url": "..." },
    { "sha": "ghi789...", "url": "..." }
  ]
}
```

dlt automatically **normalizes** this nested array into a separate child table called `commits__parents`. The relationship is:

```
commits._dlt_id  =  commits__parents._dlt_parent_id
```

A regular commit produces 1 row in `commits__parents` (1 parent). A **merge commit** produces 2+ rows (2+ parents). This parent-child relationship is the foundation of our transformations.

> 📸 **[SCREENSHOT: Raw schema mermaid diagram from the notebook — shows `commits`, `commits__parents` with the parent-child link, and `contributors`]**

## Step 3: Plan the transformations

Looking at the raw data, we identify three useful transformations:

### Why transform at all?

| Problem | Solution |
|---|---|
| `commits` has 61 columns — most are URLs and nested metadata nobody queries | **Flatten** to 13 essential fields |
| No way to tell merge commits from regular commits in the raw table | **Join** with `commits__parents` child table to compute `parent_count` and `is_merge_commit` |
| `contributors` only has API-reported contribution counts, no commit details | **Enrich** by joining `contributors → commits → commits__parents` to add commit stats per user |
| No time-series summary exists | **Aggregate** daily commit activity with merge/regular breakdown |

### Target tables

| Transformed Table | Source Tables | What it does |
|---|---|---|
| `users` | `contributors` + `commits` + `commits__parents` | Contributors enriched with per-user commit stats |
| `commits` | `commits` + `commits__parents` | Flattened 61 → 13 columns, with merge commit detection |
| `daily_activity` | `commits` + `commits__parents` | Daily rollup: total, merge, regular commits, unique authors |

## Step 4: Write the transformations

All transformations live in `github_transformations.py`. Each is decorated with `@dlt.hub.transformation` and receives a `dlt.Dataset` pointing to the raw ingested data.

### Transformation 1: `users`

This is the most complex transformation. It joins the full chain — `contributors → commits → commits__parents` — to enrich each contributor with commit statistics.

```python
@dlt.hub.transformation
def users(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    contributors = dataset.table("contributors").to_ibis()
    commits = dataset.table("commits").to_ibis()
    parents = dataset.table("commits__parents").to_ibis()
```

**Step A** — Select clean fields from contributors:

```python
    from_contributors = contributors.select(
        login=contributors.login,
        user_id=contributors.id,
        avatar_url=contributors.avatar_url,
        user_type=contributors.type,
        site_admin=contributors.site_admin,
        api_contributions=contributors.contributions,
    )
```

**Step B** — Use the parent-child join to count parents per commit, then aggregate per author:

```python
    # count parents per commit via the child table
    parent_counts = (
        parents
        .group_by(parents._dlt_parent_id)
        .aggregate(parent_count=parents._dlt_id.count())
    )

    # join commits with parent counts
    commits_enriched = commits.left_join(
        parent_counts, commits._dlt_id == parent_counts._dlt_parent_id
    ).select(
        commits.author__login,
        commits.sha,
        commits.commit__author__date,
        commits.commit__message,
        commits.commit__verification__verified,
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
    )

    # aggregate per author
    commit_stats = (
        commits_enriched
        .filter(commits_enriched.author__login.notnull())
        .group_by(commits_enriched.author__login)
        .aggregate(
            total_commits=commits_enriched.sha.count(),
            merge_commits=(commits_enriched.parent_count > 1).cast("int64").sum(),
            verified_commits=...,
            first_commit=...,
            most_recent_commit=...,
            avg_message_length=...,
        )
    )
```

**Step C** — Join back to contributors and yield:

```python
    yield (
        from_contributors
        .left_join(commit_stats, from_contributors.login == commit_stats.author__login)
        .select(
            login=from_contributors.login,
            # ... contributor fields + commit stats ...
        )
    )
```

**Result:** 154 rows, 12 columns — each contributor now has `total_commits`, `merge_commits`, `verified_commits`, `first_commit`, `most_recent_commit`, and `avg_message_length`.

### Transformation 2: `commits`

Flattens the 61-column raw commits table to 13 essential fields, and joins with the child table to add merge commit detection:

```python
@dlt.hub.transformation
def commits(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    commits = dataset.table("commits").to_ibis()
    parents = dataset.table("commits__parents").to_ibis()

    parent_counts = (
        parents
        .group_by(parents._dlt_parent_id)
        .aggregate(parent_count=parents._dlt_id.count())
    )

    enriched = commits.left_join(
        parent_counts, commits._dlt_id == parent_counts._dlt_parent_id
    )

    yield enriched.select(
        sha=commits.sha,
        author_login=ibis.coalesce(commits.author__login, ibis.literal("unknown")),
        committer_login=ibis.coalesce(commits.committer__login, ibis.literal("unknown")),
        # ... other fields ...
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
        is_merge_commit=ibis.coalesce(parent_counts.parent_count, 0) > 1,
    )
```

**Result:** 4,097 rows, 13 columns — with 474 merge commits detected via the parent-child join.

### Transformation 3: `daily_activity`

Daily rollup using the same parent-child join to break down merge vs. regular commits:

```python
@dlt.hub.transformation
def daily_activity(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    # ... same parent-child join pattern ...

    yield (
        enriched
        .mutate(activity_date=enriched.commit__author__date.date())
        .group_by("activity_date")
        .aggregate(
            total_commits=enriched.sha.count(),
            merge_commits=(enriched.parent_count > 1).cast("int64").sum(),
            regular_commits=(enriched.parent_count <= 1).cast("int64").sum(),
            unique_authors=enriched.author__login.nunique(),
        )
    )
```

**Result:** 871 rows, 5 columns — one row per day with commit breakdown.

## Step 5: Group and run

All transformations are grouped into a `@dlt.source` and executed via a second pipeline that loads into the warehouse:

```python
@dlt.source
def github_analytics(raw_dataset: dlt.Dataset) -> list:
    return [
        users(raw_dataset),
        commits(raw_dataset),
        daily_activity(raw_dataset),
    ]

# run the transformations
github_transform_pipe = dlt.pipeline("github_transform", destination="warehouse")
github_transform_pipe.run(github_analytics(github_ingest_pipe.dataset()))
```

Run the full pipeline:

```bash
python github_transformations.py
```

> 📸 **[SCREENSHOT: Terminal output showing both pipeline loads completing successfully]**

## Step 6: Explore in the marimo notebook

The notebook `github_transformations_notebook.py` visualizes the before/after schemas and creates a report from the transformed data.

```bash
marimo run github_transformations_notebook.py
```

### Schema comparison

The notebook uses `pipeline.default_schema.to_mermaid()` to render ER diagrams for both the raw and transformed schemas:

```python
# Raw schema
mo.mermaid(ingest_pipe.default_schema.to_mermaid())

# Transformed schema
mo.mermaid(transform_pipe.default_schema.to_mermaid())
```

> 📸 **[SCREENSHOT: Raw schema mermaid diagram — `commits` (61 cols) with parent-child arrow to `commits__parents` (6 cols), plus `contributors` (22 cols)]**

> 📸 **[SCREENSHOT: Transformed schema mermaid diagram — `users` (12 cols), `commits` (13 cols), `daily_activity` (5 cols)]**

### Summary stats

The notebook computes key metrics using ibis directly on the transformed tables:

```python
total_commits = t_commits.count().execute()
merge_commits = t_commits.filter(t_commits.is_merge_commit).count().execute()
```

These are rendered as stat cards via `mo.stat()`:

> 📸 **[SCREENSHOT: Stat cards row — Total Commits: 4,097 | Merge Commits: 474 | Contributors: 154 | Active: 134 | Active Days: 871]**

### Top contributors chart

A horizontal bar chart shows the top 15 contributors, colored by merge commit count:

```python
bars = (
    alt.Chart(top_users, title="Top 15 Contributors by Commits")
    .mark_bar()
    .encode(
        x=alt.X("total_commits:Q", title="Commits"),
        y=alt.Y("login:N", sort="-x", title="Contributor"),
        color=alt.Color("merge_commits:Q", scale=alt.Scale(scheme="blues")),
    )
)
```

> 📸 **[SCREENSHOT: Horizontal bar chart — Top 15 Contributors by Commits, bars colored by merge commit intensity]**

### Commit activity over time

A dual-line chart shows monthly regular vs. merge commits:

> 📸 **[SCREENSHOT: Line chart — Monthly Commits: Regular (blue) vs Merge (red) over time]**

### Author activity over time

An area chart shows the average number of daily active authors per month:

> 📸 **[SCREENSHOT: Area chart — Average Daily Active Authors per Month, green filled area]**

### Contributor breakdown table

A table showing each top contributor's merge % and verified %:

> 📸 **[SCREENSHOT: Table with columns: Contributor, Total, Merges, Verified, Merge %, Verified %]**

## Key patterns to reuse

### 1. The parent-child join pattern

Whenever dlt normalizes a nested array, it creates a child table linked by `_dlt_parent_id`. To use it:

```python
parent_counts = (
    child_table
    .group_by(child_table._dlt_parent_id)
    .aggregate(count=child_table._dlt_id.count())
)

enriched = parent_table.left_join(
    parent_counts,
    parent_table._dlt_id == parent_counts._dlt_parent_id
)
```

This pattern works for any nested array that dlt normalizes — not just GitHub commits.

### 2. The two-pipeline pattern

```python
# Pipeline 1: ingest raw data to local duckdb
ingest_pipe = dlt.pipeline("ingest", destination=dlt.destinations.duckdb("local.duckdb"))
ingest_pipe.run(source())

# Pipeline 2: transform and load to warehouse
transform_pipe = dlt.pipeline("transform", destination="warehouse")
transform_pipe.run(transformations(ingest_pipe.dataset()))
```

This keeps raw data local (cheap storage, fast queries) and only pushes clean, aggregated tables to the warehouse.

### 3. Grouping transformations in a source

```python
@dlt.source
def my_analytics(raw_dataset: dlt.Dataset) -> list:
    return [
        transformation_1(raw_dataset),
        transformation_2(raw_dataset),
        transformation_3(raw_dataset),
    ]
```

All transformations execute together in a single `pipeline.run()` call.

## Summary

| Step | What happens |
|---|---|
| **Ingest** | 2 GitHub API endpoints → 3 raw tables (dlt auto-creates `commits__parents` from nested array) |
| **Transform** | 3 transformations using ibis expressions, all leveraging the `_dlt_parent_id` join |
| **Load** | Clean tables land in the warehouse; raw data stays in local DuckDB |
| **Explore** | marimo notebook shows schema before/after + interactive report |

The full code is in two files:
- `github_pipeline.py` — source definition (2 endpoints)
- `github_transformations.py` — 3 transformations + ingestion/transform runner
- `github_transformations_notebook.py` — marimo notebook for schema viz + report
