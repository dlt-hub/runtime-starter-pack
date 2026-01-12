# NOTE: this is a notebook that assumes a successful fruitshop pipeline run

import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

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


@app.cell
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
    # NOTE: This line displays the data of the customers table in a marimo table
    mo.vstack(
        [
            mo.md("## Customers Table"),
            pipeline.dataset().customers.arrow(),
        ]
    )
    return


@app.cell
def _(pipeline: dlt.Pipeline, mo=mo):
    mo.vstack(
        [
            mo.md("## Pipeline Mermaid Diagram"),
            mo.mermaid(pipeline.default_schema.to_mermaid()),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
