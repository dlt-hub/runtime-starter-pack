import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import dlt
    import pandas as pd
    return alt, dlt, mo, pd


@app.cell
def _(dlt):
    from usgs_pipeline import usgs_ing_pipeline, usgs_eq_stats_pipeline, usgs_feeds_classified_pipeline
    # pipeline_ing = dlt.attach("usgs_ingest_pipeline")
    # pipeline_eq = dlt.attach("usgs_earthquake_stats_pipeline")
    # pipeline_feeds = dlt.attach("usgs_feeds_classified_pipeline")
    dataset_ing = usgs_ing_pipeline.dataset()
    dataset_eq = usgs_eq_stats_pipeline.dataset()
    dataset_feeds = usgs_feeds_classified_pipeline.dataset()
    return dataset_eq, dataset_feeds, dataset_ing


@app.cell
def _(mo):
    mo.md(
        r"""
# USGS Earthquake Dashboard

Breakdowns of seismic events ingested via `usgs_ingest_pipeline` and the
derived tables produced by `usgs_earthquake_stats_pipeline` and
`usgs_feeds_classified_pipeline`. Final section is operational: rows per
`_dlt_load_id` for every table.
"""
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Data breakdowns""")
    return


@app.cell
def _(dataset_eq):
    df_chart1 = dataset_eq("""
        SELECT day, SUM(event_count) AS events
        FROM earthquake_daily_stats
        GROUP BY day
        ORDER BY day
    """).df()
    return (df_chart1,)


@app.cell
def _(alt, df_chart1):
    _chart = (
        alt.Chart(df_chart1, title="Daily earthquake events (M≥2.5)")
        .mark_line(point=True)
        .encode(
            x=alt.X("day:T", title="Day"),
            y=alt.Y("events:Q", title="Events per day"),
            tooltip=["day:T", "events:Q"],
        )
        .properties(width=720, height=320)
    )
    _chart
    return


@app.cell
def _(dataset_eq):
    df_chart2 = dataset_eq("""
        SELECT region, SUM(event_count) AS events
        FROM earthquake_daily_stats
        GROUP BY region
        ORDER BY events DESC
        LIMIT 15
    """).df()
    return (df_chart2,)


@app.cell
def _(alt, df_chart2):
    _chart = (
        alt.Chart(df_chart2, title="Top 15 regions by total earthquake count")
        .mark_bar()
        .encode(
            x=alt.X("events:Q", title="Total events"),
            y=alt.Y("region:N", sort="-x", title="Region"),
            tooltip=["region:N", "events:Q"],
        )
        .properties(width=720, height=420)
    )
    _chart
    return


@app.cell
def _(dataset_ing):
    df_chart3 = dataset_ing("""
        SELECT
            FLOOR(magnitude * 2) / 2 AS magnitude_bin,
            COUNT(*) AS events
        FROM earthquakes
        WHERE magnitude IS NOT NULL
        GROUP BY magnitude_bin
        ORDER BY magnitude_bin
    """).df()
    return (df_chart3,)


@app.cell
def _(alt, df_chart3):
    _chart = (
        alt.Chart(df_chart3, title="Earthquake magnitude distribution (0.5-mag bins)")
        .mark_bar()
        .encode(
            x=alt.X("magnitude_bin:Q", title="Magnitude (binned, 0.5)"),
            y=alt.Y("events:Q", title="Number of events"),
            tooltip=["magnitude_bin:Q", "events:Q"],
        )
        .properties(width=720, height=320)
    )
    _chart
    return


@app.cell
def _(dataset_feeds):
    df_chart4 = dataset_feeds("""
        SELECT severity, COUNT(*) AS events
        FROM feeds_summary_classified
        GROUP BY severity
        ORDER BY events DESC
    """).df()
    return (df_chart4,)


@app.cell
def _(alt, df_chart4):
    severity_order = ["minor", "light", "moderate", "strong", "major", "great"]
    _chart = (
        alt.Chart(df_chart4, title="Significant events by severity (past 30 days)")
        .mark_bar()
        .encode(
            x=alt.X("severity:N", sort=severity_order, title="Severity"),
            y=alt.Y("events:Q", title="Events"),
            color=alt.Color("severity:N", sort=severity_order, legend=None),
            tooltip=["severity:N", "events:Q"],
        )
        .properties(width=560, height=320)
    )
    _chart
    return


@app.cell
def _(dataset_ing):
    df_chart5 = dataset_ing("""
        SELECT
            magnitude,
            COALESCE(depth_km__v_double, CAST(depth_km AS DOUBLE)) AS depth_km,
            type
        FROM earthquakes
        WHERE magnitude IS NOT NULL
          AND COALESCE(depth_km__v_double, CAST(depth_km AS DOUBLE)) IS NOT NULL
    """).df()
    return (df_chart5,)


@app.cell
def _(alt, df_chart5):
    _chart = (
        alt.Chart(df_chart5, title="Magnitude vs depth")
        .mark_circle(opacity=0.45, size=30)
        .encode(
            x=alt.X("depth_km:Q", title="Depth (km)"),
            y=alt.Y("magnitude:Q", title="Magnitude"),
            color=alt.Color("type:N", title="Event type"),
            tooltip=["magnitude:Q", "depth_km:Q", "type:N"],
        )
        .properties(width=720, height=420)
        .interactive()
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md(r"""## Operational: row counts by `_dlt_load_id`""")
    return


@app.cell
def _(dataset_eq, dataset_feeds, dataset_ing, pd):
    _per_table = [
        ("earthquakes", dataset_ing),
        ("feeds_summary", dataset_ing),
        ("earthquake_daily_stats", dataset_eq),
        ("feeds_summary_classified", dataset_feeds),
    ]
    _parts = []
    for _table, _ds in _per_table:
        _part = _ds(
            f"SELECT _dlt_load_id, COUNT(*) AS row_count "
            f"FROM {_table} GROUP BY _dlt_load_id ORDER BY _dlt_load_id"
        ).df()
        _part["table_name"] = _table
        _parts.append(_part)
    df_chart6 = pd.concat(_parts, ignore_index=True)
    return (df_chart6,)


@app.cell
def _(alt, df_chart6):
    _chart = (
        alt.Chart(df_chart6, title="Row counts by _dlt_load_id (per table)")
        .mark_bar()
        .encode(
            x=alt.X(
                "_dlt_load_id:N",
                title="Load id",
                axis=alt.Axis(labels=False, ticks=False),
            ),
            y=alt.Y("row_count:Q", title="Rows"),
            color=alt.Color("_dlt_load_id:N", legend=None),
            tooltip=["table_name:N", "_dlt_load_id:N", "row_count:Q"],
        )
        .properties(width=240, height=200)
        .facet(facet=alt.Facet("table_name:N", title=None), columns=2)
        .resolve_scale(y="independent", x="independent")
    )
    _chart
    return


if __name__ == "__main__":
    app.run()
