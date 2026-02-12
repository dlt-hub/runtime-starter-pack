import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data Quality Metrics with dlthub

    This notebook demonstrates how to compute and store data quality metrics for your data pipelines using dlthub.
    """)
    return


@app.cell
def _():
    import time
    import random

    import marimo as mo
    import dlt
    import duckdb
    import altair as alt

    import dlthub.data_quality as dq
    from dlthub.data_quality.metrics._definitions import get_schema_metric_hints
    from dlthub.data_quality.metrics._reader import read_metric
    return alt, dlt, dq, duckdb, get_schema_metric_hints, mo, random, time


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1. Setup: Create a pipeline and load data multiple times

    First, let's create a simple pipeline with sample customer data. We'll run it 4 times
    with different data to simulate multiple loads over time.
    """)
    return


@app.cell
def _(dlt, dq, duckdb):
    # Create an in-memory DuckDB destination
    destination = dlt.destinations.duckdb(duckdb.connect(":memory:"))
    pipeline = dlt.pipeline(
        "_customer_metrics", destination=destination, pipelines_dir=".pipelines"
    )

    # Define a data source that generates different data based on a run number
    @dq.with_metrics(
        dq.metrics.column.minimum_length("name"),
        dq.metrics.column.mean("age"),
        dq.metrics.column.unique_count("city"),
        dq.metrics.column.null_count("city"),
    )
    @dlt.resource
    def customers(run_number: int):
        """Generate different customer data for each run to simulate data changes over time."""
        # Different data for each run - not cumulative
        if run_number == 1:
            return [
                {
                    "id": 1,
                    "name": "Alice",
                    "age": 25,
                    "balance": 1000.50,
                    "is_active": True,
                    "city": "NYC",
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "age": 30,
                    "balance": 2500.75,
                    "is_active": True,
                    "city": "LA",
                },
                {
                    "id": 3,
                    "name": "Charlie",
                    "age": 35,
                    "balance": 500.25,
                    "is_active": False,
                    "city": "Chicago",
                },
            ]
        elif run_number == 2:
            return [
                {
                    "id": 4,
                    "name": "Diana",
                    "age": 28,
                    "balance": 3200.00,
                    "is_active": True,
                    "city": "Boston",
                },
                {
                    "id": 5,
                    "name": "Eve",
                    "age": 32,
                    "balance": 1800.50,
                    "is_active": True,
                    "city": "Seattle",
                },
                {
                    "id": 6,
                    "name": "Frank",
                    "age": 45,
                    "balance": 4100.25,
                    "is_active": False,
                    "city": "Austin",
                },
                {
                    "id": 7,
                    "name": "Grace",
                    "age": 29,
                    "balance": 2200.00,
                    "is_active": True,
                    "city": "NYC",
                },
            ]
        elif run_number == 3:
            return [
                {
                    "id": 8,
                    "name": "Henry",
                    "age": 38,
                    "balance": 5500.75,
                    "is_active": True,
                    "city": "Denver",
                },
                {
                    "id": 9,
                    "name": "Iris",
                    "age": 27,
                    "balance": 1200.00,
                    "is_active": False,
                    "city": "Portland",
                },
            ]
        else:  # run_number == 4
            return [
                {
                    "id": 10,
                    "name": "Jack",
                    "age": 41,
                    "balance": 6700.50,
                    "is_active": True,
                    "city": "Miami",
                },
                {
                    "id": 11,
                    "name": "Kate",
                    "age": 33,
                    "balance": 3900.25,
                    "is_active": True,
                    "city": "Phoenix",
                },
                {
                    "id": 12,
                    "name": "Leo",
                    "age": 26,
                    "balance": 900.00,
                    "is_active": False,
                    "city": "LA",
                },
                {
                    "id": 13,
                    "name": "Mia",
                    "age": 31,
                    "balance": 2800.75,
                    "is_active": True,
                    "city": "Chicago",
                },
                {
                    "id": 14,
                    "name": "Noah",
                    "age": 39,
                    "balance": 4500.00,
                    "is_active": True,
                    "city": "Seattle",
                },
            ]


    @dq.with_metrics(
        dq.metrics.column.minimum("delivery_time"),
        dq.metrics.column.mean("amount"),
        dq.metrics.column.unique_count("order_id"),
        dq.metrics.column.unique_count("customer_id"),
    )
    @dlt.resource
    def orders(run_number: int):
        """Generate different order data for each run to simulate changes over time. Mock data includes several data types."""
        import datetime

        if run_number == 1:
            return [
                {
                    "order_id": 101,
                    "customer_id": 1,
                    "amount": 150.75,
                    "order_date": datetime.date(2023, 8, 20),
                    "delivered": True,
                    "items": 2,
                    "delivery_time": None,
                },
                {
                    "order_id": 102,
                    "customer_id": 2,
                    "amount": 230.40,
                    "order_date": datetime.date(2023, 8, 22),
                    "delivered": False,
                    "items": 1,
                    "delivery_time": None,
                },
            ]
        elif run_number == 2:
            return [
                {
                    "order_id": 103,
                    "customer_id": 4,
                    "amount": 99.99,
                    "order_date": datetime.date(2023, 8, 25),
                    "delivered": True,
                    "items": 3,
                    "delivery_time": datetime.time(14, 30),
                },
                {
                    "order_id": 104,
                    "customer_id": 5,
                    "amount": 450.00,
                    "order_date": datetime.date(2023, 8, 27),
                    "delivered": True,
                    "items": 2,
                    "delivery_time": datetime.time(16, 15),
                },
                {
                    "order_id": 105,
                    "customer_id": 7,
                    "amount": 75.25,
                    "order_date": datetime.date(2023, 8, 27),
                    "delivered": False,
                    "items": 1,
                    "delivery_time": None,
                },
            ]
        elif run_number == 3:
            return [
                {
                    "order_id": 106,
                    "customer_id": 8,
                    "amount": 310.00,
                    "order_date": datetime.date(2023, 9, 2),
                    "delivered": True,
                    "items": 4,
                    "delivery_time": datetime.time(11, 0),
                },
            ]
        else:  # run_number == 4
            return [
                {
                    "order_id": 107,
                    "customer_id": 10,
                    "amount": 500.45,
                    "order_date": datetime.date(2023, 9, 5),
                    "delivered": False,
                    "items": 2,
                    "delivery_time": None,
                },
                {
                    "order_id": 108,
                    "customer_id": 13,
                    "amount": 120.99,
                    "order_date": datetime.date(2023, 9, 7),
                    "delivered": True,
                    "items": 1,
                    "delivery_time": datetime.time(9, 45),
                },
            ]

    @dq.with_metrics(
        dq.metrics.dataset.total_row_count(),
        dq.metrics.dataset.load_row_count(),
    )
    @dlt.source
    def point_of_sale(i: int):
        return [customers(i), orders(i)]
    return pipeline, point_of_sale


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Run the pipeline followed by the special source `data_quality_metrics()` which will compute metrics on the provided dataset.
    """)
    return


@app.cell
def _(dq, pipeline, point_of_sale, random, time):
    for i in range(4):
        time.sleep(random.random())
        pipeline.run(point_of_sale(i))
        pipeline.run(dq.data_quality_metrics(pipeline.dataset()))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 2. Inspect data quality metrics
    """)
    return


@app.function
def normalize_metric_hints(source_metrics_hints) -> list:
    hints = []
    hints.extend(source_metrics_hints.get("dataset", []))
    for table_name, table_metrics in source_metrics_hints.get("tables", {}).items():
        for m in table_metrics:
            m["table"] = table_name
            hints.append(m)

    for table_name, table_column_metrics in source_metrics_hints.get("columns", {}).items():
        for column_name, column_metrics in table_column_metrics.items():
            for m in column_metrics:
                m["table"] = table_name
                m["column"] = column_name
                hints.append(m)

    return hints


@app.cell
def _(dataset, get_schema_metric_hints):
    existing_metrics = normalize_metric_hints(get_schema_metric_hints(dataset.schema))
    return (existing_metrics,)


@app.cell
def _(existing_metrics, mo):
    _tables = list(sorted(set(m["table"] for m in existing_metrics if m.get("table") is not None)))
    table_select = mo.ui.dropdown(
        options=_tables,
        value=_tables[0] if _tables else None,
        label="Table"
    )
    return (table_select,)


@app.cell
def _(existing_metrics, mo, table_select):
    _columns = list(
        sorted(
            set(
                m["column"] for m in existing_metrics
                if m.get("table") == table_select.value and m["column"] is not None
            )
        )
    )
    column_select = mo.ui.dropdown(
        options=_columns,
        value=_columns[0] if _columns else None,
        label="Column"
    )
    return (column_select,)


app._unparsable_cell(
    r"""
    _columns = list(
        sorted(
            set(
                m[\"name\"] for m in existing_metrics
                if m.get(\"table\"] == table_select.value
                and m[\"column\"] == column_select.value
                and m[\"name\"] is not None
            )
        )
    )
    metric_select = mo.ui.dropdown(
        options=_columns,
        value=_columns[0] if _columns else None,
        label=\"Metric\"
    )
    """,
    name="_"
)


@app.cell
def _(column_select, metric_select, mo, table_select):
    mo.hstack([metric_select, table_select, column_select], justify="start")
    return


@app.cell
def _(mo):
    _options = {"load_id": "_dlt_load_id", "load_time": "loaded_at"}
    x_axis_select = mo.ui.dropdown(
        options=_options,
        value="load_id",
        label="X-axis"
    )
    x_axis_select
    return (x_axis_select,)


@app.cell
def _(column_select, metric_select, table_select):
    metric_name = metric_select.value[0] if isinstance(metric_select.value, list) else metric_select.value
    column_name = column_select.value
    table_name = table_select.value
    return column_name, metric_name, table_name


@app.cell
def _(alt, column_name, metric_name, metric_table, x_axis_select):
    _x_col = x_axis_select.value
    _x_title = "Load id" if _x_col == "_dlt_load_id" else "Load time"
    _y_title = f"{metric_name}({column_name})"

    alt.Chart(metric_table).mark_line(point=True).encode(
        x=alt.X(_x_col, title=_x_title),
        y=alt.Y("metric_value", title=_y_title),
        tooltip=["loaded_at", "_dlt_load_id", "metric_name", "metric_value"]
    ).interactive()
    return


@app.cell
def _(column_name, dataset, dq, metric_name, table_name):
    metric_table = dq.read_metric(dataset, table=table_name, column=column_name, metric=metric_name).arrow()
    metric_table
    return (metric_table,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 3. Manually inspection
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We see that the `dlt.Pipeline` has two schemas: the main one from the pipeline and the separate `_dlt_data_quality` which holds data quality metrics and checks. All the data goes to the same dataset nonetheless
    """)
    return


@app.cell
def _(pipeline):
    pipeline.schema_names
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Main data schema
    """)
    return


@app.cell
def _(mo, pipeline):
    mo.mermaid(pipeline.schemas[pipeline.schema_names[0]].to_mermaid())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    You can see the registered checks on the schema. See `x-dq-metrics` on the table `customers` and on the top-level `settings` key.
    """)
    return


@app.cell
def _(pipeline):
    pipeline.default_schema.to_dict()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    You can see it more clearly with this utility
    """)
    return


@app.cell
def _(dataset, get_schema_metric_hints):
    get_schema_metric_hints(dataset.schema)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Internal data quality schema
    """)
    return


@app.cell
def _(mo, pipeline):
    mo.mermaid(pipeline.schemas[pipeline.schema_names[1]].to_mermaid())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Retrieve the `dlt.Dataset` from the pipeline and create an ibis connection to power the `marimo` ui
    """)
    return


@app.cell
def _(pipeline):
    dataset = pipeline.dataset()
    con = dataset.ibis()
    return con, dataset


@app.cell
def _(mo):
    mo.md(r"""
    We see that the dataset is only aware of the main data schema
    """)
    return


@app.cell
def _(dataset):
    dataset.schema.name
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    But inside via marimo UI or Ibis, we see that both the main data and data quality exists in our destination
    """)
    return


@app.cell
def _(con):
    con.tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The dataset's `_dlt_loads` table will also show the runs for both schema. This allows to separate "data runs" from "data quality runs".
    """)
    return


@app.cell
def _(dataset):
    dataset.loads_table().arrow()
    return


if __name__ == "__main__":
    app.run()
