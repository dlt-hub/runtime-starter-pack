import marimo

__generated_with = "0.19.2"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


# ── 1. Introduction ──────────────────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Transformations on GitHub API Data

    > **Licensed feature** — `dlt` transformations require the `dlthub` package and
    > an active license.  You can self-issue a trial with `dlt license issue dlthub.transformation`.

    Raw API data is deeply nested and spread across multiple tables.
    **`dlt` transformations** let you build new analytics-ready tables directly from
    data that has already been ingested — using familiar Ibis expressions *or* plain SQL.

    This notebook walks through the full cycle:

    1. **Ingest** raw data from the GitHub REST API into local DuckDB
    2. **Explore** the raw schema — discover the parent-child relationship `dlt` created
    3. **Build** a transformation step-by-step using Ibis
    4. **Package** it with `@dlt.hub.transformation` — with options like `write_disposition`
    5. **Run** the transform pipeline — dlt detects that source and target share the same
       engine and executes the query as **pure SQL** (zero data movement)
    6. **Visualize** the result with a short report
    """)
    return


# ── 2. Ingest raw data ──────────────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 1 — Ingest raw data

    Load commits and contributors from the GitHub REST API into a local DuckDB.
    This gives us fast, free local data to iterate on.
    """)
    return


@app.cell
def _():
    from github_pipeline import github_rest_api_source
    return (github_rest_api_source,)


@app.cell
def _(github_rest_api_source):
    ingest_pipe = dlt.pipeline(
        "github_ingest",
        destination=dlt.destinations.duckdb("local_github.duckdb"),
    )
    ingest_info = ingest_pipe.run(github_rest_api_source())
    ingest_info
    return (ingest_pipe,)


# ── 3. Explore raw data ─────────────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 — Explore the raw schema

    `dlt` normalized the nested JSON from the GitHub API into relational tables.
    Notice the **parent-child relationship**: the `parents` array inside each commit
    became a separate `commits__parents` table, linked via `_dlt_id` / `_dlt_parent_id`.
    """)
    return


@app.cell
def _(ingest_pipe):
    mo.vstack([
        mo.md("### Raw Schema"),
        mo.mermaid(ingest_pipe.default_schema.to_mermaid()),
    ])
    return


@app.cell
def _(ingest_pipe):
    ingest_pipe.dataset().tables
    return


# ── 4. Build the transformation interactively ────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 — Build a transformation with Ibis

    The raw `commits` table has 61 columns with deeply nested field names like
    `commit__author__name`. Let's reshape it into a clean, flat table and enrich it
    with data from the child table.

    We'll do this step-by-step so you can see each piece.
    """)
    return


@app.cell
def _():
    import ibis
    return (ibis,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### 4a. Load raw tables into Ibis

    `dlt` datasets expose an `.to_ibis()` method that gives you a lazy Ibis table
    expression — no data is materialized until you call `.execute()` or `.to_pyarrow()`.
    """)
    return


@app.cell
def _(ingest_pipe):
    raw_commits = ingest_pipe.dataset().commits.to_ibis()
    raw_parents = ingest_pipe.dataset().commits__parents.to_ibis()
    return (raw_commits, raw_parents)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### 4b. Explore the parent-child join

    Each commit has a `_dlt_id` and each parent row has a `_dlt_parent_id` pointing back.
    Let's count parents per commit — this tells us if it's a **merge commit** (2+ parents)
    or a **regular commit** (1 parent).
    """)
    return


@app.cell
def _(raw_parents):
    parent_counts = (
        raw_parents
        .group_by(raw_parents._dlt_parent_id)
        .aggregate(parent_count=raw_parents._dlt_id.count())
    )
    parent_counts.to_pyarrow()
    return (parent_counts,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### 4c. Join and select clean columns

    Now we join the parent counts back to commits and select only the fields we need,
    giving them readable names:
    """)
    return


@app.cell
def _(ibis, parent_counts, raw_commits):
    enriched = raw_commits.left_join(
        parent_counts, raw_commits._dlt_id == parent_counts._dlt_parent_id
    )

    clean_commits = enriched.select(
        sha=raw_commits.sha,
        author_login=ibis.coalesce(raw_commits.author__login, ibis.literal("unknown")),
        committer_login=ibis.coalesce(raw_commits.committer__login, ibis.literal("unknown")),
        author_name=raw_commits.commit__author__name,
        author_email=raw_commits.commit__author__email,
        authored_at=raw_commits.commit__author__date,
        committed_at=raw_commits.commit__committer__date,
        message=raw_commits.commit__message,
        message_length=raw_commits.commit__message.length(),
        is_verified=raw_commits.commit__verification__verified,
        comment_count=raw_commits.commit__comment_count,
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
        is_merge_commit=ibis.coalesce(parent_counts.parent_count, 0) > 1,
    )
    return (clean_commits,)


@app.cell
def _(clean_commits):
    mo.vstack([
        mo.md("**Result: 13 clean columns instead of 61 nested ones**"),
        clean_commits.head(10).to_pyarrow(),
    ])
    return


# ── 5. Package as a dlt transformation ───────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 4 — Package as a `dlt` transformation

    The Ibis query above works great interactively. To make it **reusable and loadable**,
    wrap it in a `@dlt.hub.transformation` and group transformations into a `@dlt.source`.

    The decorator accepts the same arguments as `@dlt.resource`, including
    **`write_disposition`** (`"replace"` (default), `"append"`, or `"merge"`),
    `primary_key`, `columns`, and more.

    > **Tip — SQL alternative:** If you prefer SQL over Ibis, you can write
    > `yield dataset("SELECT ... FROM commits JOIN ...")` instead. Both approaches
    > produce the same result; dlt compiles everything to the destination's SQL dialect.
    """)
    return


@app.cell
def _():
    import typing
    from ibis import ir
    return (ir, typing)


@app.cell
def _(ibis, ir, typing):
    @dlt.hub.transformation
    def commits(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
        """Flattened commits joined with the child table.

        Joins commits (parent) with commits__parents (child) via:
            commits._dlt_id = commits__parents._dlt_parent_id
        to compute parent_count and is_merge_commit.
        """
        raw = dataset.table("commits").to_ibis()
        parents = dataset.table("commits__parents").to_ibis()

        parent_counts = (
            parents
            .group_by(parents._dlt_parent_id)
            .aggregate(parent_count=parents._dlt_id.count())
        )

        enriched = raw.left_join(
            parent_counts, raw._dlt_id == parent_counts._dlt_parent_id
        )

        yield enriched.select(
            sha=raw.sha,
            author_login=ibis.coalesce(raw.author__login, ibis.literal("unknown")),
            committer_login=ibis.coalesce(raw.committer__login, ibis.literal("unknown")),
            author_name=raw.commit__author__name,
            author_email=raw.commit__author__email,
            authored_at=raw.commit__author__date,
            committed_at=raw.commit__committer__date,
            message=raw.commit__message,
            message_length=raw.commit__message.length(),
            is_verified=raw.commit__verification__verified,
            comment_count=raw.commit__comment_count,
            parent_count=ibis.coalesce(parent_counts.parent_count, 0),
            is_merge_commit=ibis.coalesce(parent_counts.parent_count, 0) > 1,
        )
    return (commits,)


@app.cell
def _(commits):
    @dlt.source
    def github_analytics(raw_dataset: dlt.Dataset) -> list:
        """Transformed tables derived from raw GitHub data."""
        return [
            commits(raw_dataset),
        ]
    return (github_analytics,)


# ── 6. Run the transform pipeline ────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 5 — Run the transform pipeline

    Feed the raw dataset into the transformation source and load the result
    into the warehouse.

    **Where does the work happen?**

    - **Same engine** (e.g. DuckDB → same DuckDB): dlt detects that both datasets live
      on the same destination and executes the transformation as **pure SQL** —
      no data is copied out and back in.
    - **Different engine** (e.g. local DuckDB → Postgres/MotherDuck): dlt extracts the
      query results as Parquet files and performs a regular `dlt` load into the target.

    This means you can **avoid warehouse compute costs** by running transformations
    on a local DuckDB and only pushing the aggregated/cleaned results to the warehouse.

    **Schema evolution:** Before executing, dlt computes the resulting schema from
    your query. It will automatically create new columns or tables as needed —
    or **fail early** if there are incompatible schema changes, protecting your data.
    """)
    return


@app.cell
def _(github_analytics, ingest_pipe):
    transform_pipe = dlt.pipeline(
        "github_transform",
        destination="warehouse",
    )
    transform_info = transform_pipe.run(
        github_analytics(ingest_pipe.dataset())
    )
    transform_info
    return (transform_pipe,)


# ── 7. Compare schemas ──────────────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Before & After

    Compare the raw schema (3 tables, 61+ columns) with the transformed schema
    (1 clean table, 13 columns).
    """)
    return


@app.cell
def _(ingest_pipe):
    mo.vstack([
        mo.md("### Raw Schema — Before"),
        mo.md(
            "Three tables from the API. "
            "Note `commits` → `commits__parents` parent-child relationship."
        ),
        mo.mermaid(ingest_pipe.default_schema.to_mermaid()),
    ])
    return


@app.cell
def _(transform_pipe):
    mo.vstack([
        mo.md("### Transformed Schema — After"),
        mo.md(
            "One clean `commits` table with 13 readable columns, "
            "enriched with `parent_count` and `is_merge_commit` from the child table."
        ),
        mo.mermaid(transform_pipe.default_schema.to_mermaid()),
    ])
    return


# ── 8. Report ────────────────────────────────────────────────────────


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Report

    Let's explore the transformed data with a few quick analyses.
    """)
    return


@app.cell
def _(transform_pipe):
    t_commits = transform_pipe.dataset().commits.to_ibis()
    return (t_commits,)


@app.cell
def _(t_commits):
    total = t_commits.count().execute()
    merges = t_commits.filter(t_commits.is_merge_commit).count().execute()
    unique_authors = t_commits.author_login.nunique().execute()

    mo.vstack([
        mo.md("### Summary"),
        mo.hstack([
            mo.stat(value=total, label="Total Commits"),
            mo.stat(value=merges, label="Merge Commits"),
            mo.stat(value=total - merges, label="Regular Commits"),
            mo.stat(value=unique_authors, label="Unique Authors"),
        ], justify="center"),
    ])
    return


@app.cell
def _(ibis, t_commits):
    top_authors = (
        t_commits
        .group_by("author_login")
        .aggregate(
            total_commits=t_commits.sha.count(),
            merge_commits=t_commits.is_merge_commit.cast("int64").sum(),
        )
        .order_by(ibis.desc("total_commits"))
        .head(15)
        .execute()
    )
    return (top_authors,)


@app.cell
def _(top_authors):
    import altair as alt

    bars = (
        alt.Chart(top_authors, title="Top 15 Authors by Commits")
        .mark_bar()
        .encode(
            x=alt.X("total_commits:Q", title="Commits"),
            y=alt.Y("author_login:N", sort="-x", title="Author"),
            color=alt.Color(
                "merge_commits:Q",
                scale=alt.Scale(scheme="blues"),
                title="Merge Commits",
            ),
            tooltip=["author_login:N", "total_commits:Q", "merge_commits:Q"],
        )
        .properties(width=650, height=400)
    )

    mo.vstack([
        mo.md("### Top Authors"),
        bars,
    ])
    return (alt,)


@app.cell
def _(ibis, t_commits):
    monthly = (
        t_commits
        .mutate(month=t_commits.authored_at.truncate("M"))
        .group_by("month")
        .aggregate(
            total=t_commits.sha.count(),
            merges=t_commits.is_merge_commit.cast("int64").sum(),
        )
        .mutate(regular=ibis._.total - ibis._.merges)
        .order_by("month")
        .execute()
    )
    return (monthly,)


@app.cell
def _(alt, mo, monthly):
    base = alt.Chart(monthly).encode(x=alt.X("month:T", title="Month"))

    regular_line = base.mark_line(color="#4c78a8", point=True).encode(
        y=alt.Y("regular:Q", title="Commits"),
        tooltip=["month:T", "regular:Q"],
    )
    merge_line = base.mark_line(color="#e45756", point=True).encode(
        y=alt.Y("merges:Q"),
        tooltip=["month:T", "merges:Q"],
    )

    chart = (
        (regular_line + merge_line)
        .properties(width=700, height=350, title="Monthly Commits: Regular vs Merge")
    )

    mo.vstack([
        mo.md("### Commit Activity Over Time"),
        mo.md("🔵 Regular commits &nbsp; 🔴 Merge commits"),
        chart,
    ])
    return


@app.cell
def _(ibis, t_commits):
    daily_authors = (
        t_commits
        .mutate(month=t_commits.authored_at.truncate("M"))
        .group_by("month")
        .aggregate(unique_authors=t_commits.author_login.nunique())
        .order_by("month")
        .execute()
    )
    return (daily_authors,)


@app.cell
def _(alt, daily_authors, mo):
    authors_chart = (
        alt.Chart(daily_authors, title="Unique Authors per Month")
        .mark_area(opacity=0.5, color="#54a24b", line=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("unique_authors:Q", title="Unique Authors"),
            tooltip=["month:T", "unique_authors:Q"],
        )
        .properties(width=700, height=300)
    )

    mo.vstack([
        mo.md("### Author Activity Over Time"),
        authors_chart,
    ])
    return


@app.cell
def _(mo, top_authors):
    df = top_authors.copy()
    df["merge_ratio"] = (df["merge_commits"] / df["total_commits"] * 100).round(1)
    df = df.rename(columns={
        "author_login": "Author",
        "total_commits": "Total",
        "merge_commits": "Merges",
        "merge_ratio": "Merge %",
    })

    mo.vstack([
        mo.md("### Author Breakdown"),
        df,
    ])
    return


if __name__ == "__main__":
    app.run()
