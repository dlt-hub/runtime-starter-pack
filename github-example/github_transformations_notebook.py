import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import dlt

    ingest_pipe = dlt.attach(
        pipeline_name="github_ingest",
        destination=dlt.destinations.duckdb("local_github.duckdb"),
    )

    transform_pipe = dlt.attach(
        pipeline_name="github_transform",
        destination="warehouse",
    )
    return ingest_pipe, transform_pipe


@app.cell
def _(ingest_pipe, mo):
    mo.vstack([
        mo.md("## Raw Schema — Before Transformation"),
        mo.md(
            "Three raw tables ingested from the GitHub REST API. "
            "Note the parent-child relationship: `commits` → `commits__parents`."
        ),
        mo.mermaid(ingest_pipe.default_schema.to_mermaid()),
    ])
    return


@app.cell
def _(mo, transform_pipe):
    mo.vstack([
        mo.md("## Transformed Schema — After Transformation"),
        mo.md(
            "Three transformed tables: `users` (12 cols), `commits` (13 cols), "
            "`daily_activity` (5 cols)."
        ),
        mo.mermaid(transform_pipe.default_schema.to_mermaid()),
    ])
    return


@app.cell
def _(transform_pipe):
    t = transform_pipe.dataset()
    t_users = t.table("users").to_ibis()
    t_commits = t.table("commits").to_ibis()
    t_daily = t.table("daily_activity").to_ibis()
    return t_commits, t_daily, t_users


@app.cell
def _(mo, t_commits, t_daily, t_users):
    total_commits = t_commits.count().execute()
    merge_commits = t_commits.filter(t_commits.is_merge_commit).count().execute()
    total_users = t_users.count().execute()
    active_users = t_users.filter(t_users.total_commits > 0).count().execute()
    total_days = t_daily.count().execute()

    mo.vstack([
        mo.md("## Report: dlt-hub/dlt Repository"),
        mo.hstack([
            mo.stat(value=total_commits, label="Total Commits"),
            mo.stat(value=merge_commits, label="Merge Commits"),
            mo.stat(value=total_users, label="Contributors"),
            mo.stat(value=active_users, label="Active (≥1 commit)"),
            mo.stat(value=total_days, label="Active Days"),
        ], justify="center"),
    ])
    return


@app.cell
def _(t_users):
    import ibis

    top_users = (
        t_users
        .filter(t_users.total_commits > 0)
        .order_by(ibis.desc("total_commits"))
        .head(15)
        .select("login", "total_commits", "merge_commits", "verified_commits")
        .execute()
    )
    return (top_users,)


@app.cell
def _(mo, top_users):
    import altair as alt

    bars = (
        alt.Chart(top_users, title="Top 15 Contributors by Commits")
        .mark_bar()
        .encode(
            x=alt.X("total_commits:Q", title="Commits"),
            y=alt.Y("login:N", sort="-x", title="Contributor"),
            color=alt.Color("merge_commits:Q", scale=alt.Scale(scheme="blues"), title="Merge Commits"),
            tooltip=["login:N", "total_commits:Q", "merge_commits:Q", "verified_commits:Q"],
        )
        .properties(width=650, height=400)
    )

    mo.vstack([
        mo.md("### Top Contributors"),
        bars,
    ])
    return (alt,)


@app.cell
def _(t_daily):
    import ibis as _ibis

    monthly = (
        t_daily
        .mutate(month=t_daily.activity_date.truncate("M"))
        .group_by("month")
        .aggregate(
            total=t_daily.total_commits.sum(),
            merges=t_daily.merge_commits.sum(),
            regular=t_daily.regular_commits.sum(),
        )
        .order_by("month")
        .execute()
    )
    return (monthly,)


@app.cell
def _(alt, mo, monthly):
    base = alt.Chart(monthly).encode(
        x=alt.X("month:T", title="Month"),
    )

    merge_line = base.mark_line(color="#e45756", point=True).encode(
        y=alt.Y("merges:Q", title="Commits"),
        tooltip=["month:T", "merges:Q"],
    )

    regular_line = base.mark_line(color="#4c78a8", point=True).encode(
        y=alt.Y("regular:Q"),
        tooltip=["month:T", "regular:Q"],
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
def _(t_daily):
    monthly_authors = (
        t_daily
        .mutate(month=t_daily.activity_date.truncate("M"))
        .group_by("month")
        .aggregate(avg_daily_authors=t_daily.unique_authors.mean())
        .order_by("month")
        .execute()
    )
    return (monthly_authors,)


@app.cell
def _(alt, mo, monthly_authors):
    authors_chart = (
        alt.Chart(monthly_authors, title="Average Daily Active Authors per Month")
        .mark_area(opacity=0.5, color="#54a24b", line=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("avg_daily_authors:Q", title="Avg Daily Authors"),
            tooltip=["month:T", "avg_daily_authors:Q"],
        )
        .properties(width=700, height=300)
    )

    mo.vstack([
        mo.md("### Author Activity Over Time"),
        authors_chart,
    ])
    return


@app.cell
def _(mo, top_users):
    import pandas as pd

    df = top_users.copy()
    df["merge_ratio"] = (df["merge_commits"] / df["total_commits"] * 100).round(1)
    df["verified_ratio"] = (df["verified_commits"] / df["total_commits"] * 100).round(1)
    df = df.rename(columns={
        "login": "Contributor",
        "total_commits": "Total",
        "merge_commits": "Merges",
        "verified_commits": "Verified",
        "merge_ratio": "Merge %",
        "verified_ratio": "Verified %",
    })

    mo.vstack([
        mo.md("### Contributor Breakdown"),
        df,
    ])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
