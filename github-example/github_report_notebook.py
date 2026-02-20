import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # GitHub Commit Report

    An interactive report built on **transformed** GitHub API data.

    > Run `github_transformations_notebook.py` first to ingest raw data and produce
    > the transformed `commits` table.
    """)
    return


@app.cell
def _():
    transform_pipe = dlt.pipeline(
        "github_transform",
        destination="warehouse",
    )
    return (transform_pipe,)


@app.cell
def _(transform_pipe):
    import ibis

    t = transform_pipe.dataset().commits.to_ibis()
    return ibis, t


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## At a Glance
    """)
    return


@app.cell
def _(t):
    total = t.count().execute()
    merges = t.filter(t.is_merge_commit).count().execute()
    regular = total - merges
    authors = t.author_login.nunique().execute()
    verified = t.filter(t.is_verified).count().execute()
    avg_msg = round(t.message_length.mean().execute(), 1)

    mo.hstack([
        mo.stat(value=total, label="Total Commits"),
        mo.stat(value=merges, label="Merge Commits"),
        mo.stat(value=regular, label="Regular Commits"),
        mo.stat(value=authors, label="Unique Authors"),
        mo.stat(value=verified, label="Verified Commits"),
        mo.stat(value=avg_msg, label="Avg Message Length"),
    ], justify="center")
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Top Contributors
    """)
    return


@app.cell
def _(ibis, t):
    top_authors = (
        t
        .group_by("author_login")
        .aggregate(
            total_commits=t.sha.count(),
            merge_commits=t.is_merge_commit.cast("int64").sum(),
            verified_commits=t.is_verified.cast("int64").sum(),
            avg_message_length=t.message_length.mean().round(1),
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
        alt.Chart(top_authors, title="Top 15 Authors by Commit Count")
        .mark_bar()
        .encode(
            x=alt.X("total_commits:Q", title="Commits"),
            y=alt.Y("author_login:N", sort="-x", title="Author"),
            color=alt.Color(
                "merge_commits:Q",
                scale=alt.Scale(scheme="blues"),
                title="Merge Commits",
            ),
            tooltip=[
                alt.Tooltip("author_login:N", title="Author"),
                alt.Tooltip("total_commits:Q", title="Total"),
                alt.Tooltip("merge_commits:Q", title="Merges"),
                alt.Tooltip("verified_commits:Q", title="Verified"),
            ],
        )
        .properties(width=650, height=400)
    )

    mo.vstack([bars])
    return (alt,)


@app.cell
def _(top_authors):
    df = top_authors.copy()
    df["merge_pct"] = (df["merge_commits"] / df["total_commits"] * 100).round(1)
    df["verified_pct"] = (df["verified_commits"] / df["total_commits"] * 100).round(1)
    df = df.rename(columns={
        "author_login": "Author",
        "total_commits": "Total",
        "merge_commits": "Merges",
        "verified_commits": "Verified",
        "avg_message_length": "Avg Msg Len",
        "merge_pct": "Merge %",
        "verified_pct": "Verified %",
    })

    mo.vstack([
        mo.md("### Breakdown"),
        df,
    ])
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Commit Activity Over Time
    """)
    return


@app.cell
def _(ibis, t):
    monthly = (
        t
        .mutate(month=t.authored_at.truncate("M"))
        .group_by("month")
        .aggregate(
            total=t.sha.count(),
            merges=t.is_merge_commit.cast("int64").sum(),
        )
        .mutate(regular=ibis._.total - ibis._.merges)
        .order_by("month")
        .execute()
    )
    return (monthly,)


@app.cell
def _(alt, monthly):
    def _():
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
        return mo.vstack([
            mo.md("🔵 Regular &nbsp; 🔴 Merge"),
            chart,
        ])


    _()
    return


@app.cell
def _(alt, t):
    monthly_authors = (
        t
        .mutate(month=t.authored_at.truncate("M"))
        .group_by("month")
        .aggregate(unique_authors=t.author_login.nunique())
        .order_by("month")
        .execute()
    )

    authors_area = (
        alt.Chart(monthly_authors, title="Unique Authors per Month")
        .mark_area(opacity=0.5, color="#54a24b", line=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("unique_authors:Q", title="Unique Authors"),
            tooltip=["month:T", "unique_authors:Q"],
        )
        .properties(width=700, height=300)
    )

    mo.vstack([authors_area])
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Merge Ratio Trend
    """)
    return


@app.cell
def _(alt, monthly):
    merge_df = monthly.copy()
    merge_df["merge_ratio"] = (merge_df["merges"] / merge_df["total"] * 100).round(1)

    ratio_chart = (
        alt.Chart(merge_df, title="Merge Commit % per Month")
        .mark_area(opacity=0.4, color="#e45756", line={"color": "#e45756"})
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("merge_ratio:Q", title="Merge %"),
            tooltip=[
                alt.Tooltip("month:T", title="Month"),
                alt.Tooltip("merge_ratio:Q", title="Merge %"),
            ],
        )
        .properties(width=700, height=280)
    )

    mo.vstack([ratio_chart])
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## When Do People Commit?
    """)
    return


@app.cell
def _(alt, t):
    dow_hour = (
        t
        .mutate(
            day_of_week=t.authored_at.day_of_week.index(),
            hour=t.authored_at.hour(),
        )
        .group_by(["day_of_week", "hour"])
        .aggregate(commits=t.sha.count())
        .execute()
    )

    day_labels = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    dow_hour["day_name"] = dow_hour["day_of_week"].map(day_labels)

    heatmap = (
        alt.Chart(dow_hour, title="Commits by Day of Week × Hour")
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("day_name:N", title="Day", sort=list(day_labels.values())),
            color=alt.Color("commits:Q", scale=alt.Scale(scheme="blues"), title="Commits"),
            tooltip=[
                alt.Tooltip("day_name:N", title="Day"),
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("commits:Q", title="Commits"),
            ],
        )
        .properties(width=650, height=250)
    )

    mo.vstack([heatmap])
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Commit Message Length
    """)
    return


@app.cell
def _(alt, t):
    msg_data = (
        t
        .select("message_length", "is_merge_commit")
        .execute()
    )
    msg_data["type"] = msg_data["is_merge_commit"].map({True: "Merge", False: "Regular"})

    histogram = (
        alt.Chart(msg_data, title="Message Length Distribution")
        .mark_bar(opacity=0.7)
        .encode(
            x=alt.X("message_length:Q", bin=alt.Bin(maxbins=50), title="Characters"),
            y=alt.Y("count():Q", title="Commits"),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(domain=["Regular", "Merge"], range=["#4c78a8", "#e45756"]),
                title="Type",
            ),
            tooltip=["type:N", "count():Q"],
        )
        .properties(width=700, height=300)
    )

    mo.vstack([histogram])
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Commit Verification
    """)
    return


@app.cell
def _(alt, ibis, t):
    def _():
        verified_monthly = (
            t
            .mutate(month=t.authored_at.truncate("M"))
            .group_by("month")
            .aggregate(
                total=t.sha.count(),
                verified=t.is_verified.cast("int64").sum(),
            )
            .mutate(unverified=ibis._.total - ibis._.verified)
            .order_by("month")
            .execute()
        )

        base = alt.Chart(verified_monthly).encode(x=alt.X("month:T", title="Month"))

        v_line = base.mark_line(color="#54a24b", point=True).encode(
            y=alt.Y("verified:Q", title="Commits"),
            tooltip=["month:T", "verified:Q"],
        )
        u_line = base.mark_line(color="#bab0ac", point=True, strokeDash=[4, 2]).encode(
            y="unverified:Q",
            tooltip=["month:T", "unverified:Q"],
        )

        chart = (
            (v_line + u_line)
            .properties(width=700, height=300, title="Verified vs Unverified Commits per Month")
        )

        return mo.vstack([
            mo.md("🟢 Verified &nbsp; ⚪ Unverified"),
            chart,
        ])

    _()
    return


if __name__ == "__main__":
    app.run()
