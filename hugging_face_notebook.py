# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "dlt[lancedb]",
#     "dlthub",
#     "duckdb",
#     "ibis-framework",
#     "marimo",
#     "pyarrow-hotfix",
# ]
# ///

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
    # Curating data with Hugging Face and dltHub

    This notebook complements our [blog post on dltHub's recent integration with Hugging Face](https://dlthub.com/blog/hugging-face-dlt-ml).

    We'll walkthrough loading the OpenVid Dataset into LanceDB using dltHub and then writing the data back to Hugging Face after data exploration, quality checks, and
    filtering.

    The pipeline:
    1. Uses DuckDB's `hf://` adapter to read parquet files directly from HuggingFace Hub
    2. Filters out heavy columns (video blobs, embeddings) to keep only metadata
    3. Streams rows in batches into LanceDB via `dlt`
    4. Embeds the `caption` column for vector search
    5. Runs data quality checks to validate scores, nullability, and categories
    6. Filters videos by quality thresholds to curate a training-ready subset
    7. Writes the curated dataset back to HuggingFace via `dlt`
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Define the pipeline and data source

    A `dlt.pipeline` is the central object that connects a data source to a
    destination. It manages schema evolution, state tracking, and load metadata.

    A `dlt.pipeline` connects a data source to a destination and manages schema
    evolution, state, and load metadata. Here we point it at LanceDB so the
    OpenVid metadata we pull from HuggingFace lands in a queryable vector store.

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="openvid",
        destination="lance",
        dataset_name="openvid",
    )
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    import os

    hf_token = os.environ.get("HF_TOKEN")
    mo.stop(
        hf_token is not None,
    )
    mo.callout(
        mo.md(
            "**Warning:** `HF_TOKEN` not set. You may hit HuggingFace rate limits (HTTP 429). "
            "Get a token at https://huggingface.co/settings/tokens and set it with "
            "`export HF_TOKEN=hf_...`"
        ),
        kind="warn",
    )
    return (hf_token,)


@app.cell
def _():
    import duckdb

    HF_PARQUET_URL = "hf://datasets/lance-format/openvid-lance@~parquet/**/*.parquet"
    EXCLUDED_COLUMNS = {"video_blob", "embedding"}
    BATCH_SIZE = 1000
    DATASET_NAME = "openvid"

    pipeline = dlt.pipeline(
        pipeline_name="openvid",
        destination="lance",
        dataset_name=DATASET_NAME,
    )
    return BATCH_SIZE, EXCLUDED_COLUMNS, HF_PARQUET_URL, duckdb, pipeline


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The dlt resource

    A `dlt.resource` is the core building block for getting data into a pipeline.
    It's a Python generator that yields batches of data. The
    `write_disposition="replace"` means each run overwrites the previous data.

    Column discovery and data streaming happen in a single DuckDB connection —
    we `DESCRIBE` the remote parquet schema, filter out heavy columns, then
    stream rows in batches.

    ```python
    @dlt.resource(write_disposition="replace")
    def openvid_videos(limit: int = 100):
        with duckdb.connect() as conn:
            # Discover columns, exclude heavy ones, stream in batches
            ...
            while rows := result.fetchmany(BATCH_SIZE):
                yield [dict(zip(columns, row)) for row in rows]
    ```

    `dlt` automatically infers and evolves the destination schema as data
    flows through the resource. Use
    [schema contracts](https://dlthub.com/docs/general-usage/schema-contracts)
    to control how unexpected changes are handled.
    """)
    return


@app.cell
def _(BATCH_SIZE, EXCLUDED_COLUMNS, HF_PARQUET_URL, duckdb, hf_token):
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
    ## Load data into LanceDB

    Now we run the pipeline and load the data into LanceDB.
    """)
    return


@app.cell(hide_code=True)
def _():
    RUN_MODE_LIMIT = 10
    is_run_mode = mo.app_meta().mode == "run"

    match mo.app_meta().mode:
        case "run":
            limit_slider = None

        case _:
            limit_slider = mo.ui.slider(
                start=0,
                stop=500,
                step=10,
                value=0,
                label="Rows to load",
            )

    limit_slider if limit_slider is not None else mo.md(f"**Only loading {RUN_MODE_LIMIT} rows.** Feel free to use interactive mode to load more.\n\nIn marimo edit mode, use the slider to control how many rows to load from HuggingFace.")
    return RUN_MODE_LIMIT, is_run_mode, limit_slider


@app.cell
def _(RUN_MODE_LIMIT, limit_slider, openvid_videos, pipeline):
    _limit = limit_slider.value if limit_slider is not None else RUN_MODE_LIMIT
    mo.stop(_limit == 0, mo.md("**Set rows to load > 0 to run the pipeline**"))
    load_info = pipeline.run(
        openvid_videos(limit=_limit),
        table_name="videos",
    )
    return (load_info,)


@app.cell(hide_code=True)
def _(load_info):
    match load_info:
        case None:
            mo.callout(
                mo.md("**Skipped loading** — using existing data"),
                kind="info",
            )

        case _:
            mo.md(f"```\n{load_info}\n```")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Embedding columns at load time

    With the provider configured, wrap your resource with `lancedb_adapter`
    to specify which columns to embed. Your data arrives in LanceDB ready
    for vector search.

    ```python
    from dlt.destinations.adapters import lancedb_adapter

    load_info = pipeline.run(
        lancedb_adapter(openvid_videos(limit=100), embed=["caption"]),
        table_name="videos",
    )
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Configuring embeddings

    The dlt adapter for LanceDB can automatically generate vector embeddings
    at load time. Configure your embedding provider in `.dlt/config.toml` —
    you can use OpenAI, Cohere, HuggingFace sentence-transformers, or any
    other provider supported by `dlt`.

    ```toml
    # .dlt/config.toml
    [destination.lance]
    destination_type="lancedb"
    embedding_model_provider="openai"  # or "cohere", "sentence-transformers", etc.
    embedding_model="text-embedding-3-small"
    ```
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Discover available tables

    The pipeline's dataset object exposes a `.tables` property listing all tables
    that were loaded. In our case, we expect a `videos` table containing the
    OpenVid metadata.

    ```python
    pipeline.dataset().tables
    ```
    """)
    return


@app.cell
def _(load_info, pipeline):
    pipeline.dataset().tables
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Inspect the schema

    `dlt` tracks schema information for every pipeline run. We can render the
    schema as a Mermaid diagram to visualize the table structure, column types,
    and relationships.

    ```python
    pipeline.default_schema.to_mermaid()
    ```
    """)
    return


@app.cell
def _(load_info, pipeline):
    mo.mermaid(pipeline.default_schema.to_mermaid())
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Preview the videos table

    Let's look at the raw data that was loaded. The pipeline selected only
    metadata columns from HuggingFace, excluding heavy binary data like
    `video_blob` and `embedding`. In a production deployment, you might
    include these columns or even generate additional embeddings.

    `dlt` datasets support multiple materialization formats — call
    `.arrow()` for a PyArrow table, `.df()` for a Pandas DataFrame, or
    iterate the relation directly for row-by-row access:

    ```python
    pipeline.dataset().videos.df()        # Pandas DataFrame
    pipeline.dataset().videos.arrow()     # PyArrow Table
    pipeline.dataset().videos.fetchall()  # list of tuples
    ```

    The columns we have include:
    - `video_path` - path to the video file on HuggingFace
    - `caption` - text description of the video content
    - `aesthetic_score` - visual quality rating (0-10)
    - `motion_score` - amount of motion in the video
    - `temporal_consistency_score` - frame-to-frame consistency (0-1)
    - `camera_motion` - type of camera movement (static, pan, tilt, etc.)
    - `fps`, `seconds`, `frame` - video duration metadata

    ```python
    pipeline.dataset().videos.arrow()
    ```
    """)
    return


@app.cell
def _(load_info, pipeline):
    pipeline.dataset().videos.arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Data quality checks

    We use `dlthub.data_quality` to validate the loaded data. These checks
    verify structural integrity (uniqueness, nullability) and domain constraints
    (score ranges, valid categories).

    ```python
    import dlthub.data_quality as dq

    videos_checks = [
        dq.checks.is_unique("video_path"),
        dq.checks.is_not_null("caption"),
        dq.checks.case("aesthetic_score BETWEEN 0 AND 10"),
        ...
    ]
    ```
    """)
    return


@app.cell
def _():
    import dlthub.data_quality as dq

    return (dq,)


@app.cell
def _(dq):
    videos_checks = [
        # uniqueness & key constraints (primary key = unique + not null)
        dq.checks.is_unique("video_path"),
        dq.checks.is_not_null("video_path"),

        # required fields must be present
        dq.checks.is_not_null("caption"),
        dq.checks.is_not_null("aesthetic_score"),
        dq.checks.is_not_null("motion_score"),

        # scores within expected bounds
        dq.checks.case("aesthetic_score BETWEEN 0 AND 10"),
        dq.checks.case("motion_score >= 0"),
        dq.checks.case("temporal_consistency_score BETWEEN 0 AND 1.001"),

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
    ### Table-level results

    Running checks at `level="table"` returns aggregate pass/fail counts for
    each check across the entire table.
    """)
    return


@app.cell
def _(dq, load_info, pipeline, videos_checks):
    dq.prepare_checks(
        pipeline.dataset().videos,
        videos_checks,
        level="table",
    ).arrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Row-level results

    Running checks at `level="row"` returns per-row pass/fail results, so you
    can see exactly which rows have issues.
    """)
    return


@app.cell
def _(dq, load_info, pipeline, videos_checks):
    dq.prepare_checks(
        pipeline.dataset().videos,
        videos_checks,
        level="row",
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

    Since it returns a `dlt.Relation`, you can materialize it as Arrow or
    pass it directly to `dlt.Pipeline.run()` to persist check results
    alongside your data.

    See the [data quality docs](https://dlthub.com/docs/hub/features/transformations) for more details.
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
def _(dq, load_info, pipeline, videos_checks):
    check_suite = dq.CheckSuite(
        pipeline.dataset(), checks={"videos": videos_checks}
    )
    check_suite.checks
    return (check_suite,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Drilling into successes and failures

    Using `.get_successes()` and `.get_failures()` you can retrieve the actual
    rows that passed or failed specific checks. This is useful for understanding
    *which* records have data quality issues.

    For example, let's look at the `camera_motion` check. This check validates
    that every row's `camera_motion` value is one of the expected categories:
    **static**, **pan**, **tilt**, **zoom**, **rotate**, **follow**, or
    **handheld**. Rows with unexpected values (typos, nulls, or new categories
    not in our allow-list) will fail.
    """)
    return


@app.cell
def _(check_suite, mo):
    try:
        _successes_table = check_suite.get_successes("videos", "camera_motion__case__In").arrow()
        _successes_result = mo.vstack([
            mo.md("**Rows with valid `camera_motion` values**"),
            _successes_table,
        ])
    except Exception as e:
        _successes_result = mo.callout(mo.md(f"**Could not resolve successes:** `{e}`"), kind="warn")
    _successes_result
    return


@app.cell
def _(check_suite, mo):
    try:
        _failures_table = check_suite.get_failures("videos", "camera_motion__case__In").arrow()
        _failures_result = mo.vstack([
            mo.md("**Rows with unexpected `camera_motion` values**"),
            _failures_table,
        ])
    except Exception as e:
        _failures_result = mo.callout(mo.md(f"**Could not resolve failures:** `{e}`"), kind="warn")
    _failures_result
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Persisting check results

    Since `dlthub.data_quality.prepare_checks(...)` returns a `dlt.Relation`, you
    can pipe check results into any destination. Depending on your use case, decide:
    - what check level to save: `row`, `table`, or `dataset`
    - where to store results: checks are computed where the data lives, but you
      can move data quality to a different location
    - what pipeline and dataset to use for storage

    ```python
    dq_pipeline = dlt.pipeline(
        pipeline_name="data_quality",
        destination="motherduck",  # easily load into a different destination
        dataset_name="quality_results",
    )
    dq_pipeline.run(
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

    ```python
    videos_ibis = pipeline.dataset().videos.to_ibis()
    stats = videos_ibis.aggregate(
        total=ibis._.caption.count(),
        avg_aesthetic=ibis._.aesthetic_score.mean(),
    )
    stats.to_pyarrow()
    ```
    """)
    return


@app.cell
def _():
    import ibis

    return (ibis,)


@app.cell
def _(load_info, pipeline):
    videos_ibis = pipeline.dataset().videos.to_ibis()
    return (videos_ibis,)


@app.cell
def _(ibis, videos_ibis):
    stats = videos_ibis.aggregate(
        total=ibis._.caption.count(),
        avg_aesthetic=ibis._.aesthetic_score.mean(),
        avg_motion=ibis._.motion_score.mean(),
        avg_temporal=ibis._.temporal_consistency_score.mean(),
        avg_fps=ibis._.fps.mean(),
        avg_seconds=ibis._.seconds.mean(),
    )
    stats.to_pyarrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Analyzing score distributions by camera motion

    Understanding the distribution of scores across different camera motion
    types helps identify outliers and set sensible filtering thresholds for
    downstream training.
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
    ## Filter videos for training

    Set minimum thresholds for video quality metrics to curate a training-ready
    subset. In edit mode, use the sliders to experiment with thresholds. In run
    mode, the defaults below are applied automatically.
    """)
    return


@app.cell
def _(is_run_mode):
    _defaults = {
        "aesthetic_score": 5.0,
        "motion_score": 2.0,
        "temporal_consistency_score": 0.99,
        "fps": 24,
        "seconds": 2.0,
    }

    filters = mo.ui.dictionary({
        "aesthetic_score": mo.ui.slider(0.0, 10.0, 0.5, value=_defaults["aesthetic_score"], label="Min aesthetic"),
        "motion_score": mo.ui.slider(0.0, 100.0, 1.0, value=_defaults["motion_score"], label="Min motion"),
        "temporal_consistency_score": mo.ui.slider(0.0, 1.0, 0.01, value=_defaults["temporal_consistency_score"], label="Min temporal"),
        "fps": mo.ui.slider(0, 120, 1, value=_defaults["fps"], label="Min FPS"),
        "seconds": mo.ui.slider(0.0, 300.0, 1.0, value=_defaults["seconds"], label="Min seconds"),
    })
    filters
    return (filters,)


@app.cell
def _(filters, videos_ibis):
    filtered = videos_ibis
    for col, slider in filters.value.items():
        filtered = filtered.filter(filtered[col] >= slider)
    filtered_arrow = filtered.to_pyarrow()
    return (filtered_arrow,)


@app.cell
def _(ibis, filtered_arrow, videos_ibis):
    import pyarrow as pa

    _before = videos_ibis.aggregate(
        count=ibis._.caption.count(),
        avg_aesthetic=ibis._.aesthetic_score.mean(),
        avg_motion=ibis._.motion_score.mean(),
        avg_temporal=ibis._.temporal_consistency_score.mean(),
        avg_fps=ibis._.fps.mean(),
        avg_seconds=ibis._.seconds.mean(),
    ).to_pyarrow()

    _filtered_ibis = ibis.memtable(filtered_arrow)
    _after = _filtered_ibis.aggregate(
        count=ibis._.caption.count(),
        avg_aesthetic=ibis._.aesthetic_score.mean(),
        avg_motion=ibis._.motion_score.mean(),
        avg_temporal=ibis._.temporal_consistency_score.mean(),
        avg_fps=ibis._.fps.mean(),
        avg_seconds=ibis._.seconds.mean(),
    ).to_pyarrow()

    _before_row = {col: _before.column(col)[0].as_py() for col in _before.column_names}
    _after_row = {col: _after.column(col)[0].as_py() for col in _after.column_names}

    _comparison = pa.table({
        "metric": list(_before_row.keys()),
        "before (full)": [float(v) if v is not None else 0.0 for v in _before_row.values()],
        "after (filtered)": [float(v) if v is not None else 0.0 for v in _after_row.values()],
    })

    _kept = _after_row["count"]
    _total = _before_row["count"]
    _pct = (_kept / _total * 100) if _total > 0 else 0

    mo.vstack([
        mo.md(f"**Kept {int(_kept)} / {int(_total)} videos ({_pct:.1f}%)**"),
        _comparison,
    ])
    return


@app.cell
def _(filtered_arrow):
    mo.vstack(
        [
            filtered_arrow.drop("video_path"),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Write back to HuggingFace

    Once you're happy with your filtered dataset, you can write it back to
    HuggingFace Hub using `dlt`. This closes the loop — load from HuggingFace,
    validate, filter, and publish the curated dataset back.
    """)
    return


@app.cell
def _(filtered_arrow):
    mo.md(
        f"Your curated dataset contains **{filtered_arrow.num_rows} videos** "
        f"across **{filtered_arrow.num_columns} columns**, ready to export back to HuggingFace."
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ```python
    hf_pipeline = dlt.pipeline(
        pipeline_name="openvid_curated",
        destination="filesystem",
        dataset_name="openvid_curated",
    )
    hf_pipeline.run(
        [filtered_arrow],
        table_name="videos",
        write_disposition="replace",
    )
    ```

    The `filesystem` destination uses the `hf://` protocol to push directly to
    HuggingFace Hub. Configure the bucket URL and authentication token in
    `.dlt/secrets.toml`:

    ```toml
    # .dlt/secrets.toml
    [destination.filesystem]
    bucket_url = "hf://datasets/my-org"

    [destination.filesystem.credentials]
    hf_token = "hf_..."  # Your HuggingFace User Access Token
    ```

    **`bucket_url`** — the `hf://datasets/` prefix tells `dlt` to treat the
    destination as a HuggingFace dataset repo. Replace `my-org` with your
    HuggingFace username or organization. The `dataset_name` from the pipeline
    becomes the repo name (e.g. `my-org/openvid_curated`).

    **`hf_token`** — a [User Access Token](https://huggingface.co/settings/tokens)
    with write permissions. You can also set this via the `HF_TOKEN` environment
    variable.

    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## What's next?

    All data files for a table are committed in a single git commit, and `dlt`
    automatically creates and maintains the repo's `README.md` with proper
    metadata so the dataset appears in the HuggingFace Dataset Viewer.

    Everything you've done in this notebook — loading from HuggingFace,
    validating, filtering, and exporting — runs locally with open-source `dlt`.
    When you're ready to move to production, [dltHub Pro](https://dlthub.com/solutions/for-frontier-labs)
    runs your working pipeline as-is on managed, enterprise-grade infrastructure
    — no rewriting, no stitching together scheduling and monitoring. One
    developer can deliver what previously required an entire platform team.

    See the [blog post](https://dlthub.com/blog/hugging-face-dlt-ml) for a
    walkthrough of how and why we built this integration.

    ---

    Built with love in SF by [dltHub](https://dlthub.com).
    """)
    return


if __name__ == "__main__":
    app.run()
