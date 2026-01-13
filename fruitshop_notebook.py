# NOTE: this is a notebook that assumes a successful fruitshop pipeline run

import marimo

__generated_with = "0.19.2"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell
def _():
    pipeline = dlt.attach(
        pipeline_name="fruitshop",
        destination="fruitshop_destination",
        dataset_name="fruitshop_data",
    )
    return (pipeline,)


@app.cell(hide_code=True)
def _(pipeline):
    # NOTE: This line displays the destination dialect of the pipeline
    # gives a hint as to what kind of database is being used
    mo.vstack(
        [
            mo.md("## Pipeline Info"),
            mo.md(
                f"Pipeline is using destination type: {pipeline.destination.destination_type}"
            ),
        ]
    )
    return


@app.cell
def _(pipeline):
    pipeline.dataset().tables
    return


@app.cell
def _(pipeline):
    # NOTE: This line displays the data of the customers table in a marimo table
    mo.vstack(
        [
            mo.md("## Inventory Table"),
            pipeline.dataset().inventory.arrow(),
        ]
    )
    return


@app.cell
def _(pipeline):
    mo.vstack(
        [
            mo.md("## Pipeline Mermaid Diagram"),
            mo.mermaid(pipeline.default_schema.to_mermaid()),
        ]
    )
    return


@app.cell
def _():
    import dlthub
    import dlthub.data_quality as dq
    return (dq,)


@app.cell
def _(dq):
    inventory_checks = [
        dq.checks.is_in("name", ["apple", "pear", "cherry"]),
        dq.checks.is_not_null("price"),
        dq.checks.case("price < 0")  # arbitrary condition evaluated row-wise
    ]
    return (inventory_checks,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Use `dlthub.data_quality.prepare_checks()` to create a query that will return checks results. It takes as input:
    - the `dlt.Relation` associated with a table found in the dataset
    - a list of `checks`
    - the check granularity `level`: `"row"`, `"table"`, or `"dataset"`
    """)
    return


@app.cell
def _(dq, inventory_checks, pipeline):
    _query_rel = dq.prepare_checks(
        pipeline.dataset().inventory,
        inventory_checks,
        level="table",
    )
    _query_rel.arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    The `CheckSuite` object can run checks the same way and provides convenience methods to explore check results.

    It can be instantiated by passing a dataset and the check definitions.
    """)
    return


@app.cell
def _(dq, inventory_checks, pipeline):
    check_suite = dq.CheckSuite(pipeline.dataset(), checks={"inventory": inventory_checks})
    check_suite.checks
    return (check_suite,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Using `.add_checks` allows you to register checks later.

    The `CheckSuite` provides `.get_successes()` and `.get_failures()` to retrieve the records that failed specific checks.

    Original check was filtering for: Apples, Pears, and Cherrys
    """)
    return


@app.cell
def _(check_suite):
    check_suite.get_successes("inventory", "name__is_in").arrow()
    return


@app.cell
def _(check_suite):
    check_suite.get_failures("inventory", "name__is_in").arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Since `dlthub.data_quality.prepare_checks(...)` returns a `dlt.Relation`, you can pass it directly to `dlt.Pipeline.run()`.

    Depending on your use case, you will need to decide:
    - what check level to save: `row`, `table`, or `dataset`
    - where to checks results: checks are computed where the data lives, but you can move data quality to a different location
    - what pipeline and dataset to use to store checks results

    ```py
    pipeline.run(
        [dq.prepare_checks(some_dataset, some_dataset_checks).arrow()],
        table_name="dlt_data_quality",
    )
    ```
    """)
    return


@app.cell
def _(pipeline):
    customers = pipeline.dataset().customers.to_ibis()
    customers.to_pyarrow()
    return (customers,)


@app.cell
def _():
    import ibis
    return (ibis,)


@app.cell
def _(customers, ibis):
    customer_cities = (
        customers.group_by("city")
        .aggregate(
            number_of_customers=ibis._.id.count(),
        )
    )
    customer_cities  # we see the query plan
    return (customer_cities,)


@app.cell
def _(customer_cities):
    customer_cities.to_pyarrow()
    # customer_cities.to_pandas to_polars, etc.
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Ibis transformations statements can be parameterized i.e. pass in datasets

    ```py
    @dlt.hub.transformation
    def customer_payments(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:


        orders = dataset.table("orders").to_ibis()



        payments = dataset.table("payments").to_ibis()



        yield (
            payments.left_join(orders, payments.order_id == orders.id)
            .group_by(orders.customer_id)
            .aggregate(total_amount=ibis._.amount.sum())
        )
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    We can then gather: pairs of datasets + transformations into a list and materialize at runtime as a data source with `@dlt.source` decorator

    ```py
    @dlt.source
    def customers_metrics(raw_dataset: dlt.Dataset) -> list:
        return [

            customer_payments(raw_dataset),

            another_transformation(raw_dataset),

            ...,
        ]
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Pass "customer metrics" as a source into a dlt.pipeline to load it

    ```py
    new_pipeline = dlt.pipeline(

        "customer_metrics",

        destination="some_data_warehouse", # e.g. Snowflake, Bigquery
    )


    new_pipeline.run(

        customers_metrics( # takes in a dataset as input

            original_pipeline.dataset() # pass in the results of our original pipeline

        )

    )
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Run data quality checks against the transformed data

    ```py

    dq.prepare_checks(

        customer_metrics(original_pipeline.dataset()),

        [
            dq.checks.case("number_of_orders > 20"),
            ...,
        ],

        level="dataset"

    ).df()
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
