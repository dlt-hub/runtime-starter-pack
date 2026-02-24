# NOTE: this is a notebook that assumes a successful lancedb pipeline run

import marimo

__generated_with = "0.19.2"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell
def _():
    pipeline = dlt.attach(
        pipeline_name="octolens_mention",
        destination="lance",
        dataset_name="octolens_mention",
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
    # NOTE: This line displays the data of the mentions table in a marimo table
    mo.vstack(
        [
            mo.md("## Mentions Table"),
            pipeline.dataset().mentions.arrow(),
        ]
    )
    return


@app.cell
def _(pipeline):
    mo.vstack(
        [
            mo.md("## Schema as a Mermaid diagram"),
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
    mentions_checks = [
        dq.checks.is_not_null("URL"),
        dq.checks.is_not_null("Timestamp"),
        dq.checks.is_not_null("Source"),
        dq.checks.is_in("Sentiment", ["Positive", "Negative", "Neutral"]),
        dq.checks.is_not_null("Language"),
        # At least one of Title or Body should have content
        dq.checks.case("Title IS NOT NULL OR Body IS NOT NULL"),
    ]
    return (mentions_checks,)


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Use `dlthub.data_quality.prepare_checks()` to create a query that will return checks results. It takes as input:
    - the `dlt.Relation` associated with a table found in the dataset
    - a list of `checks`
    - the check granularity `level`: `"row"`, `"table"`, or `"dataset"`
    """
    )
    return


@app.cell(hide_code=True)
def _(dq, mentions_checks, pipeline):
    _query_rel = dq.prepare_checks(
        pipeline.dataset().mentions,
        mentions_checks,
        level="table",
    )
    _query_rel.arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    The `CheckSuite` object can run checks the same way and provides convenience methods to explore check results.

    It can be instantiated by passing a dataset and the check definitions.
    """
    )
    return


@app.cell
def _(dq, mentions_checks, pipeline):
    check_suite = dq.CheckSuite(
        pipeline.dataset(), checks={"mentions": mentions_checks}
    )
    check_suite.checks
    return (check_suite,)


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Using `.add_checks` allows you to register checks later.

    The `CheckSuite` provides `.get_successes()` and `.get_failures()` to retrieve the records that failed specific checks.

    Original check was filtering for sentiment: Positive, Negative, and Neutral
    """
    )
    return


@app.cell
def _(check_suite):
    check_suite.get_successes("mentions", "Sentiment__is_in").arrow()
    return


@app.cell
def _(check_suite):
    check_suite.get_failures("mentions", "Sentiment__is_in").arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
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
    """
    )
    return


@app.cell
def _(pipeline):
    mentions = pipeline.dataset().mentions.to_ibis()
    mentions.to_pyarrow()
    return (mentions,)


@app.cell
def _():
    import ibis

    return (ibis,)


@app.cell
def _(ibis, mentions):
    mentions_by_source = mentions.group_by("Source").aggregate(
        mention_count=ibis._.Source_ID.count(),
        sentiment_distribution=ibis._.Sentiment.collect(),
    )
    mentions_by_source  # we see the query plan
    return (mentions_by_source,)


@app.cell
def _(mentions_by_source):
    mentions_by_source.to_pyarrow()
    # mentions_by_source.to_pandas, to_polars, etc.
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ## Vector Search Example

    LanceDB supports vector search on embedded columns. The pipeline can embed the `Title`, `Body`, and `URL` fields.

    You can perform vector searches using the LanceDB client directly:

    ```py
    with pipeline.destination_client() as client:
        db = client.db_client
        mentions_table = db.open_table("octolens_mention___mentions")

        # Search for mentions similar to a query
        results = (
            mentions_table.search(
                "data quality and testing",
                vector_column_name="vector",
            )
            .limit(10)
            .to_arrow()
        )
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Ibis transformations statements can be parameterized i.e. pass in datasets

    ```py
    @dlt.hub.transformation
    def mentions_with_metadata(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:

        mentions = dataset.table("mentions").to_ibis()

        # Add computed fields like sentiment_score, is_recent, etc.
        yield (
            mentions.mutate(
                has_image=mentions.Image_URL.notnull(),
                content_length=mentions.Body.length(),
            )
        )
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    We can then gather: pairs of datasets + transformations into a list and materialize at runtime as a data source with `@dlt.source` decorator

    ```py
    @dlt.source
    def mentions_analytics(raw_dataset: dlt.Dataset) -> list:
        return [

            mentions_with_metadata(raw_dataset),

            sentiment_aggregation(raw_dataset),

            ...,
        ]
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Pass "mentions analytics" as a source into a dlt.pipeline to load it

    ```py
    new_pipeline = dlt.pipeline(

        "mentions_analytics",

        destination="some_data_warehouse", # e.g. Snowflake, Bigquery, or LanceDB
    )


    new_pipeline.run(

        mentions_analytics( # takes in a dataset as input

            original_pipeline.dataset() # pass in the results of our original pipeline

        )

    )
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Run data quality checks against the transformed data

    ```py

    dq.prepare_checks(

        mentions_analytics(original_pipeline.dataset()),

        [
            dq.checks.is_not_null("URL"),
            dq.checks.is_in("Sentiment", ["Positive", "Negative", "Neutral"]),
            ...,
        ],

        level="dataset"

    ).df()
    ```
    """
    )
    return


if __name__ == "__main__":
    app.run()
