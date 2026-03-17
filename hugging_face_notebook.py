# Full walkthrough of the HuggingFace DuckDB pipeline
# This notebook walks through loading OpenVid video metadata from HuggingFace
# into LanceDB using DuckDB as the ingestion layer.

import marimo

__generated_with = "0.19.10"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # HuggingFace DuckDB Pipeline Walkthrough

    This notebook complements our blog post on dltHub's recent integration with Hugging Face:   https://dlthub.com/blog/hugging-face-dlt-ml

    We'll walthrough loading the OpenVid Dataset into LanceDB using dltHub and then writing the data back to Hugging Face after data exploration, quality checks, and
    filtering.

    The pipeline:
    1. Uses DuckDB's `hf://` adapter to read parquet files directly from HuggingFace Hub
    2. Filters out heavy columns (video blobs, embeddings) to keep only metadata
    3. Streams rows in batches into LanceDB via `dlt`
    4. Embeds the `caption` column for vector search
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Define the Pipeline and Data Source

    First we set up the `dlt` pipeline targeting LanceDB and define our
    `dlt.resource` that reads from HuggingFace using DuckDB's `hf://` protocol.

    DuckDB can query parquet files hosted on HuggingFace Hub directly — no
    download step needed. We exclude heavy columns (`video_blob`, `embedding`)
    and stream rows in batches of 1,000.
    """)
    return


@app.cell
def _():
    import os

    import duckdb

    HF_PARQUET_URL = "hf://datasets/lance-format/openvid-lance@~parquet/**/*.parquet"
    EXCLUDED_COLUMNS = {"video_blob", "embedding"}
    BATCH_SIZE = 1000
    DATASET_NAME = "openvid"

    # Authenticate with HuggingFace to avoid HTTP 429 rate limits.
    # Set HF_TOKEN in your environment before running this notebook.
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        mo.output.append(
            mo.callout(
                mo.md(
                    "**Warning:** `HF_TOKEN` not set. You may hit HuggingFace rate limits (HTTP 429). "
                    "Get a token at https://huggingface.co/settings/tokens and set it with "
                    "`export HF_TOKEN=hf_...`"
                ),
                kind="warn",
            )
        )

    pipeline = dlt.pipeline(
        pipeline_name="openvid",
        destination="lance",
        dataset_name=DATASET_NAME,
    )
    return BATCH_SIZE, EXCLUDED_COLUMNS, HF_PARQUET_URL, duckdb, pipeline


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The dlt Resource

    A `dlt.resource` is a generator that yields batches of data. The
    `write_disposition="replace"` means each run overwrites the previous data.

    Column discovery and data streaming happen in a single DuckDB connection —
    we `DESCRIBE` the remote parquet schema, filter out heavy columns, then
    stream rows in batches.
    """)
    return


@app.cell
def _(BATCH_SIZE, EXCLUDED_COLUMNS, HF_PARQUET_URL, duckdb):
    @dlt.resource(write_disposition="replace")
    def openvid_videos(limit: int = 100):
        with duckdb.connect() as conn:
            schema = conn.execute(
                f"DESCRIBE SELECT * FROM '{HF_PARQUET_URL}' LIMIT 0"
            ).fetchall()
            columns_sql = ", ".join(
                col[0] for col in schema if col[0] not in EXCLUDED_COLUMNS
            )
            result = conn.execute(
                f"SELECT {columns_sql} FROM '{HF_PARQUET_URL}' LIMIT {limit}"
            )
            columns = [desc[0] for desc in result.description]

            while rows := result.fetchmany(BATCH_SIZE):
                yield [dict(zip(columns, row)) for row in rows]

    return (openvid_videos,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Load Data into LanceDB

    Now we run the pipeline and load the data into LanceDB.

    Use the slider to control how many rows to load from HuggingFace.

    > **Embedding support:** dlt + LanceDB can automatically generate OpenAI
    > vector embeddings at load time. To enable this, set `OPENAI_API_KEY` in
    > your environment and wrap the resource with `lancedb_adapter`:
    >
    > ```python
    > from dlt.destinations.adapters import lancedb_adapter
    >
    > # Automatically embed the caption column using OpenAI text-embedding-3-small
    > # This makes your data searchable by meaning, not just keywords
    > load_info = pipeline.run(
    >     lancedb_adapter(openvid_videos(limit=100), embed=["caption"]),
    >     table_name="videos",
    > )
    > ```
    >
    > The embedding model is configured in `.dlt/config.toml`:
    > ```toml
    > [destination.lance]
    > embedding_model_provider="openai"
    > embedding_model="text-embedding-3-small"
    > ```
    >
    > This is one of the most powerful features of the dlt + LanceDB integration —
    > your data is searchable by meaning, not just keywords, right out of the box.
    """)
    return


@app.cell(hide_code=True)
def _():
    limit_slider = mo.ui.slider(
        start=10,
        stop=500,
        step=10,
        value=100,
        label="Rows to load",
    )
    limit_slider if mo.app_meta().mode != "run" else mo.md("**Loading 100 rows** (app mode)")
    return (limit_slider,)


@app.cell
def _(limit_slider, openvid_videos, pipeline):
    load_info = pipeline.run(
        openvid_videos(limit=limit_slider.value),
        table_name="videos",
    )
    mo.md(f"```\n{load_info}\n```")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Discover Available Tables

    The pipeline's dataset object exposes a `.tables` property listing all tables
    that were loaded. In our case, we expect a `videos` table containing the
    OpenVid metadata.
    """)
    return


@app.cell
def _(pipeline):
    pipeline.dataset().tables
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Inspect the Schema

    `dlt` tracks schema information for every pipeline run. We can render the
    schema as a Mermaid diagram to visualize the table structure, column types,
    and relationships.
    """)
    return


@app.cell
def _(pipeline):
    mo.mermaid(pipeline.default_schema.to_mermaid())
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Preview the Videos Table

    Let's look at the raw data that was loaded. The pipeline selected only
    metadata columns from HuggingFace, excluding heavy binary data like
    `video_blob` and `embedding`.

    The columns we have include:
    - `video_path` - path to the video file on HuggingFace
    - `caption` - text description of the video content
    - `aesthetic_score` - visual quality rating (0-10)
    - `motion_score` - amount of motion in the video
    - `temporal_consistency_score` - frame-to-frame consistency (0-1)
    - `camera_motion` - type of camera movement (static, pan, tilt, etc.)
    - `fps`, `seconds`, `frame` - video duration metadata
    """)
    return


@app.cell
def _(pipeline):
    pipeline.dataset().videos.arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Data Quality Checks

    We use `dlthub.data_quality` to validate the loaded data. These checks
    verify structural integrity (uniqueness, nullability) and domain constraints
    (score ranges, valid categories).
    """)
    return


@app.cell
def _():
    import dlthub.data_quality as dq

    return (dq,)


@app.cell
def _(dq):
    videos_checks = [
        # uniqueness & key constraints
        dq.checks.is_unique("video_path"),
        dq.checks.is_primary_key("video_path"),

        # required fields must be present
        dq.checks.is_not_null("caption"),
        dq.checks.is_not_null("aesthetic_score"),
        dq.checks.is_not_null("motion_score"),

        # scores within expected bounds
        dq.checks.case("aesthetic_score BETWEEN 0 AND 10"),
        dq.checks.case("motion_score >= 0"),
        dq.checks.case("temporal_consistency_score BETWEEN 0 AND 1"),

        # video metadata sanity
        dq.checks.case("fps > 0 AND fps <= 120"),
        dq.checks.case("seconds > 0"),
        dq.checks.case("frame > 0"),

        # camera_motion should be a known category
        # Using case() instead of is_in() to avoid a sqlglot lineage resolution bug
        dq.checks.case(
            "camera_motion IN ('static', 'pan', 'tilt', 'zoom', 'rotate', 'follow', 'handheld')"
        ),
    ]
    return (videos_checks,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Table-Level Results

    Running checks at `level="table"` returns aggregate pass/fail counts for
    each check across the entire table.
    """)
    return


@app.cell
def _(dq, pipeline, videos_checks):
    dq.prepare_checks(
        pipeline.dataset().videos,
        videos_checks,
        level="table",
    ).arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Understanding `prepare_checks`

    `dlthub.data_quality.prepare_checks()` returns a `dlt.Relation` with check
    results. It takes as input:
    - the `dlt.Relation` associated with a table found in the dataset
    - a list of `checks`
    - the check granularity `level`: `"row"`, `"table"`, or `"dataset"`

    Since it returns a `dlt.Relation`, you can pass it directly to
    `dlt.Pipeline.run()` to persist check results alongside your data.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### CheckSuite

    The `CheckSuite` object can run checks the same way and provides convenience
    methods to explore check results. It can be instantiated by passing a dataset
    and the check definitions.
    """)
    return


@app.cell
def _(dq, pipeline, videos_checks):
    check_suite = dq.CheckSuite(
        pipeline.dataset(), checks={"videos": videos_checks}
    )
    check_suite.checks
    return (check_suite,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Drilling into Successes and Failures

    Using `.get_successes()` and `.get_failures()` you can retrieve the actual
    rows that passed or failed specific checks. This is useful for understanding
    *which* records have data quality issues.

    For example, let's see which rows pass and fail the `camera_motion` category
    check:
    """)
    return


@app.cell
def _(check_suite):
    check_suite.get_successes("videos", "camera_motion__is_in").arrow()
    return


@app.cell
def _(check_suite):
    check_suite.get_failures("videos", "camera_motion__is_in").arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Persisting Check Results

    Since `dlthub.data_quality.prepare_checks(...)` returns a `dlt.Relation`, you
    can pipe check results into any destination. Depending on your use case, decide:
    - what check level to save: `row`, `table`, or `dataset`
    - where to store results: checks are computed where the data lives, but you
      can move data quality to a different location
    - what pipeline and dataset to use for storage

    ```python
    pipeline.run(
        [dq.prepare_checks(some_dataset, some_dataset_checks).arrow()],
        table_name="dlt_data_quality",
    )
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Explore with Ibis

    `dlt` datasets can be converted to [Ibis](https://ibis-project.org/) tables
    for expressive, lazy analytics. Ibis builds a query plan that only executes
    when you materialize results (e.g., `.to_pyarrow()`).
    """)
    return


@app.cell
def _():
    import ibis

    return (ibis,)


@app.cell
def _(pipeline):
    videos_ibis = pipeline.dataset().videos.to_ibis()
    return (videos_ibis,)


@app.cell
def _(ibis, videos_ibis):
    stats = videos_ibis.aggregate(
        total=ibis._.caption.count(),
        avg_aesthetic=ibis._.aesthetic_score.mean(),
        min_aesthetic=ibis._.aesthetic_score.min(),
        max_aesthetic=ibis._.aesthetic_score.max(),
        avg_motion=ibis._.motion_score.mean(),
        min_motion=ibis._.motion_score.min(),
        max_motion=ibis._.motion_score.max(),
        avg_temporal=ibis._.temporal_consistency_score.mean(),
        avg_fps=ibis._.fps.mean(),
        avg_seconds=ibis._.seconds.mean(),
    )
    stats.to_pyarrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Score Distributions

    Understanding the distribution of scores helps identify outliers and set
    sensible filtering thresholds for downstream training.
    """)
    return


@app.cell
def _(ibis, videos_ibis):
    camera_motion_counts = (
        videos_ibis
        .group_by("camera_motion")
        .aggregate(
            count=ibis._.caption.count(),
            avg_aesthetic=ibis._.aesthetic_score.mean(),
            avg_motion=ibis._.motion_score.mean(),
        )
        .order_by(ibis.desc("count"))
    )
    camera_motion_counts.to_pyarrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Filter Videos for Training

    Use the sliders below to set minimum thresholds for video quality metrics.
    The filtered dataset updates reactively as you adjust the sliders.
    """)
    return


@app.cell
def _():
    filters = mo.ui.dictionary({
        "aesthetic_score": mo.ui.slider(0.0, 10.0, 0.1, value=4.0, label="Min aesthetic"),
        "motion_score": mo.ui.slider(0.0, 100.0, 1.0, value=0.0, label="Min motion"),
        "temporal_consistency_score": mo.ui.slider(0.0, 1.0, 0.01, value=0.0, label="Min temporal"),
        "fps": mo.ui.slider(0, 120, 1, value=0, label="Min FPS"),
        "seconds": mo.ui.slider(0.0, 300.0, 1.0, value=0.0, label="Min seconds"),
    })
    filters
    return (filters,)


@app.cell
def _(filters, videos_ibis):
    filtered = videos_ibis
    for col, slider in filters.value.items():
        filtered = filtered.filter(filtered[col] >= slider)
    filtered_arrow = filtered.to_pyarrow()

    thresholds = ", ".join(f"{col} >= {v}" for col, v in filters.value.items())
    mo.vstack(
        [
            mo.md(f"### Filtered: {filtered_arrow.num_rows} videos ({thresholds})"),
            filtered_arrow,
        ]
    )
    return (filtered_arrow,)


@app.cell
def _(filtered_arrow):
    mo.md(
        f"**Ready for export:** {filtered_arrow.num_rows} rows x {filtered_arrow.num_columns} columns"
    )
    return


if __name__ == "__main__":
    app.run()
