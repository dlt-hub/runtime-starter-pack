import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Data Quality for GitHub API Data
    > **Licensed feature** — `dlt` data quality requires the `dlthub` package and
    > an active license.  You can self-issue a trial with `dlt license issue dlthub.transformation`.

    This notebook demonstrates two complementary features of `dlt` data quality:

    - **Metrics** — scalar values that *describe* your data (row counts, null rates, averages).
      They profile the dataset so you can monitor trends over time.
    - **Checks** — boolean assertions that *validate* your data (is this column non-null?
      is this value in the allowed set?). They surface specific records that violate expectations.

    Together, metrics and checks give you a complete picture:
    metrics tell you *what the data looks like*, checks tell you *whether it meets your standards*.
    """)
    return


@app.cell
def _():
    from github_pipeline import github_rest_api_source
    import dlthub
    import dlthub.data_quality as dq
    return dq, github_rest_api_source


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 1 — Set metrics on resources

    A **data quality metric** is a function applied to data that returns a **scalar value**
    describing a property of the data. Metrics can operate at the **column**, **table**, or
    **dataset** level.

    We grab individual resources from the source and wrap them with `dq.with_metrics()`:
    """)
    return


@app.cell
def _(dq, github_rest_api_source):
    source = github_rest_api_source()

    resource_commits = dq.with_metrics(
        source.commits,
        dq.metrics.column.null_count("sha"),
        dq.metrics.column.null_rate("commit__author__name"),
        dq.metrics.column.unique_count("sha"),
        dq.metrics.column.mean("commit__comment_count"),
        dq.metrics.column.maximum("commit__comment_count"),
        dq.metrics.table.row_count(),
    )
    return resource_commits, source


@app.cell
def _(dq, source):
    resource_contributors = dq.with_metrics(
        source.contributors,
        dq.metrics.column.null_count("login"),
        dq.metrics.column.unique_count("login"),
        dq.metrics.column.mean("contributions"),
        dq.metrics.column.maximum("contributions"),
        dq.metrics.column.minimum("contributions"),
        dq.metrics.table.row_count(),
    )
    return (resource_contributors,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 — Run pipeline

    Load data using the metric-instrumented resources.
    The pipeline now knows which metrics to compute after the load.
    """)
    return


@app.cell
def _(resource_commits, resource_contributors):
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )
    load_info = pipeline.run([resource_commits, resource_contributors])
    load_info
    return (pipeline,)


@app.cell(hide_code=True)
def _(pipeline):
    mo.vstack(
        [
            mo.md("### Pipeline Info"),
            mo.md(
                f"Pipeline **`{pipeline.pipeline_name}`** is using "
                f"destination type: **`{pipeline.destination.destination_type}`**"
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
    mo.vstack(
        [
            mo.md("### Schema"),
            mo.mermaid(pipeline.default_schema.to_mermaid()),
        ]
    )
    return


@app.cell
def _(pipeline):
    mo.vstack(
        [
            mo.md("### Commits (first 5 rows)"),
            pipeline.dataset().commits.head(5).arrow(),
        ]
    )
    return


@app.cell
def _(pipeline):
    mo.vstack(
        [
            mo.md("### Contributors (first 5 rows)"),
            pipeline.dataset().contributors.head(5).arrow(),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 — Run metrics

    Compute all metrics defined on the resources and store results in `_dlt_dq_metrics`.
    Each pipeline run appends new rows, building a **historical audit trail**.
    """)
    return


@app.cell
def _(dq, pipeline):
    dq.run_metrics(pipeline)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Commits metrics

    | Metric | What it tells us |
    |---|---|
    | `null_count("sha")` | Number of commits without a SHA — should be 0 |
    | `null_rate("commit__author__name")` | Fraction of commits with missing author — monitors completeness |
    | `unique_count("sha")` | Distinct SHAs — detects duplicates if lower than row count |
    | `mean("commit__comment_count")` | Average comments per commit — baseline for monitoring |
    | `maximum("commit__comment_count")` | Most-commented commit — detects outliers |
    | `row_count()` | Total number of commits loaded |
    """)
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `null_count(sha)`** (should be 0)"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            column="sha",
            metric="null_count",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `null_rate(commit__author__name)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            column="commit__author__name",
            metric="null_rate",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `unique_count(sha)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            column="sha",
            metric="unique_count",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `mean(commit__comment_count)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            column="commit__comment_count",
            metric="mean",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `maximum(commit__comment_count)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            column="commit__comment_count",
            metric="maximum",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**commits — `row_count()`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="commits",
            metric="row_count",
        ).arrow(),
    ])
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Contributors metrics

    | Metric | What it tells us |
    |---|---|
    | `null_count("login")` | Contributors without a username — should be 0 |
    | `unique_count("login")` | Distinct logins — detects duplicates |
    | `mean("contributions")` | Average contributions per contributor |
    | `maximum("contributions")` | Top contributor's count |
    | `minimum("contributions")` | Least active contributor's count |
    | `row_count()` | Total number of contributors |

    Metrics are descriptive — they don't pass or fail. That's what **checks** are for.
    """)
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `null_count(login)`** (should be 0)"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            column="login",
            metric="null_count",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `unique_count(login)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            column="login",
            metric="unique_count",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `mean(contributions)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            column="contributions",
            metric="mean",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `maximum(contributions)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            column="contributions",
            metric="maximum",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `minimum(contributions)`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            column="contributions",
            metric="minimum",
        ).arrow(),
    ])
    return


@app.cell
def _(dq, pipeline):
    mo.vstack([
        mo.md("**contributors — `row_count()`**"),
        dq.read_metric(
            pipeline.dataset(),
            table="contributors",
            metric="row_count",
        ).arrow(),
    ])
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 4 — Define quality checks

    While metrics *describe* the data, **checks** *assert expectations* about it.
    A check is a function that returns a result (e.g., success rate) which is converted to a
    pass/fail **outcome** based on a **decision rule**.

    The relationship between metrics and checks:

    | Metric | Related check | Connection |
    |---|---|---|
    | `null_count("sha")` | `is_not_null("sha")` | Metric counts nulls; check flags the rows |
    | `null_rate("commit__author__name")` | `is_not_null("commit__author__name")` | Metric gives the rate; check gives pass/fail |
    | `unique_count("login")` | `is_not_null("login")` | Both monitor identity completeness |
    | `minimum("contributions")` | `case("contributions > 0")` | If min > 0, the check passes |
    """)
    return


@app.cell
def _(dq):
    commits_checks = [
        dq.checks.is_not_null("sha"),
        dq.checks.is_not_null("commit__author__name"),
        dq.checks.is_not_null("commit__author__date"),
        dq.checks.case("commit__comment_count >= 0"),
    ]
    return (commits_checks,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Commits checks

    | Check | Category | Why |
    |---|---|---|
    | `is_not_null("sha")` | Identity | Every commit needs a unique hash — without it, no join or dedup works |
    | `is_not_null("commit__author__name")` | Completeness | Missing authors break aggregations (e.g., "commits per author") |
    | `is_not_null("commit__author__date")` | Completeness | Missing dates break time-series analytics |
    | `case("commit__comment_count >= 0")` | Validity | Comment count must not be negative — boundary sanity check |
    """)
    return


@app.cell
def _(dq):
    contributors_checks = [
        dq.checks.is_not_null("login"),
        dq.checks.is_not_null("id"),
        dq.checks.is_in("type", ["User", "Bot"]),
        dq.checks.case("contributions > 0"),
    ]
    return (contributors_checks,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Contributors checks

    | Check | Category | Why |
    |---|---|---|
    | `is_not_null("login")` | Identity | Every contributor needs a username for joins and display |
    | `is_not_null("id")` | Identity | Stable numeric key — logins can be renamed, IDs cannot |
    | `is_in("type", ["User", "Bot"])` | Domain | Only known GitHub account types — catches data drift |
    | `case("contributions > 0")` | Contract | The API only returns contributors with ≥ 1 contribution |
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 5 — Run checks

    Execute all checks against the loaded data using `dq.prepare_checks()` at the **table** level.
    """)
    return


@app.cell
def _(commits_checks, dq, pipeline):
    mo.vstack([
        mo.md("### Commits — check results"),
        dq.prepare_checks(
            pipeline.dataset().commits,
            commits_checks,
            level="table",
        ).arrow(),
    ])
    return


@app.cell
def _(contributors_checks, dq, pipeline):
    mo.vstack([
        mo.md("### Contributors — check results"),
        dq.prepare_checks(
            pipeline.dataset().contributors,
            contributors_checks,
            level="table",
        ).arrow(),
    ])
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Exploring results with CheckSuite

    The `CheckSuite` bundles checks for multiple tables and provides convenience methods
    to drill into individual results.
    """)
    return


@app.cell
def _(commits_checks, contributors_checks, dq, pipeline):
    check_suite = dq.CheckSuite(
        pipeline.dataset(),
        checks={
            "commits": commits_checks,
            "contributors": contributors_checks,
        },
    )
    check_suite.checks
    return (check_suite,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Use `.get_successes()` and `.get_failures()` to retrieve the actual records
    that passed or failed a specific check.

    For example, let's look at which contributors are **not** of type `User` or `Bot`:
    """)
    return


@app.cell
def _(check_suite):
    check_suite.get_successes("contributors", "type__is_in").arrow()
    return


@app.cell
def _(check_suite):
    check_suite.get_failures("contributors", "type__is_in").arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    And let's check which commits have a null `commit__author__name`:
    """)
    return


@app.cell
def _(check_suite):
    check_suite.get_failures("commits", "commit__author__name__is_not_null").arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Persisting results

    **Metrics** are automatically stored in the `_dlt_dq_metrics` table by `dq.run_metrics()`.
    Each pipeline run appends new rows, building a historical audit trail that you can
    query to detect trends and regressions.

    **Check results** can also be persisted. Since `dq.prepare_checks(...)` returns a
    `dlt.Relation`, you can pass it directly to `pipeline.run()`:

    ```python
    pipeline.run(
        [dq.prepare_checks(dataset.commits, commits_checks, level="table").arrow()],
        table_name="dlt_data_quality",
    )
    ```

    Depending on your use case, decide:
    - **Check level to save**: `row` (per-record), `table` (summary), or `dataset` (cross-table)
    - **Where to store**: same destination, a separate monitoring database, or both
    - **Which pipeline/dataset**: keep quality data alongside business data or in a dedicated dataset
    """)
    return


if __name__ == "__main__":
    app.run()
