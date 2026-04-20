# NOTE: this is a notebook that assumes a successful fruitshop pipeline run

import marimo

__generated_with = "0.19.10"
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



if __name__ == "__main__":
    app.run()
