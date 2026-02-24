# NOTE: this is a notebook that assumes a successful hugging_face_pipeline run

import marimo

__generated_with = "0.19.10"
app = marimo.App(width="full")

with app.setup:
    import dlt
    import marimo as mo


@app.cell
def _():
    pipeline = dlt.attach(
        pipeline_name="openvid",
        destination="lance",
        dataset_name="openvid",
    )
    return (pipeline,)


@app.cell(hide_code=True)
def _(pipeline):
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
    mo.vstack(
        [
            mo.md("## Videos Table"),
            pipeline.dataset().videos.arrow(),
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
    videos_checks = [
        dq.checks.is_not_null("video_path"),
        dq.checks.is_not_null("caption"),
        dq.checks.is_not_null("aesthetic_score"),
        dq.checks.is_not_null("motion_score"),
        dq.checks.is_not_null("temporal_consistency_score"),
        dq.checks.is_not_null("camera_motion"),
        dq.checks.is_not_null("frame"),
        dq.checks.is_not_null("fps"),
        dq.checks.is_not_null("seconds"),
    ]
    return (videos_checks,)


@app.cell(hide_code=True)
def _(dq, pipeline, videos_checks):
    mo.vstack(
        [
            mo.md("## Table-level Data Quality"),
            dq.prepare_checks(
                pipeline.dataset().videos,
                videos_checks,
                level="table",
            ).arrow(),
        ]
    )
    return


@app.cell
def _(dq, pipeline, videos_checks):
    check_suite = dq.CheckSuite(
        pipeline.dataset(), checks={"videos": videos_checks}
    )
    check_suite.checks
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Filter Videos for Training

    Use the sliders below to set minimum thresholds for **aesthetic score**,
    **motion score**, **temporal consistency**, **FPS**, and **seconds**.
    The filtered dataset will be displayed and can then be exported as a
    training-ready Arrow / Parquet file.
    """)
    return


@app.cell
def _():
    aesthetic_slider = mo.ui.slider(
        start=0.0,
        stop=10.0,
        step=0.1,
        value=4.0,
        label="Min aesthetic score",
    )
    motion_slider = mo.ui.slider(
        start=0.0,
        stop=100.0,
        step=1.0,
        value=0.0,
        label="Min motion score",
    )
    temporal_slider = mo.ui.slider(
        start=0.0,
        stop=1.0,
        step=0.01,
        value=0.0,
        label="Min temporal consistency",
    )
    fps_slider = mo.ui.slider(
        start=0,
        stop=120,
        step=1,
        value=0,
        label="Min FPS",
    )
    seconds_slider = mo.ui.slider(
        start=0.0,
        stop=300.0,
        step=1.0,
        value=0.0,
        label="Min seconds",
    )
    mo.vstack(
        [
            mo.hstack([aesthetic_slider, motion_slider, temporal_slider]),
            mo.hstack([fps_slider, seconds_slider]),
        ]
    )
    return aesthetic_slider, fps_slider, motion_slider, seconds_slider, temporal_slider


@app.cell
def _(pipeline):
    videos_ibis = pipeline.dataset().videos.to_ibis()
    return (videos_ibis,)


@app.cell
def _(aesthetic_slider, fps_slider, motion_slider, seconds_slider, temporal_slider, videos_ibis):
    filtered = videos_ibis.filter(
        (videos_ibis.aesthetic_score >= aesthetic_slider.value)
        & (videos_ibis.motion_score >= motion_slider.value)
        & (videos_ibis.temporal_consistency_score >= temporal_slider.value)
        & (videos_ibis.fps >= fps_slider.value)
        & (videos_ibis.seconds >= seconds_slider.value)
    )
    filtered_arrow = filtered.to_pyarrow()

    mo.vstack(
        [
            mo.md(
                f"### Filtered: {filtered_arrow.num_rows} videos "
                f"(aesthetic ≥ {aesthetic_slider.value}, "
                f"motion ≥ {motion_slider.value}, "
                f"temporal ≥ {temporal_slider.value}, "
                f"fps ≥ {fps_slider.value}, "
                f"seconds ≥ {seconds_slider.value})"
            ),
            filtered_arrow,
        ]
    )
    return (filtered_arrow,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Score Distributions
    """)
    return


@app.cell
def _():
    import ibis

    return (ibis,)


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
        min_temporal=ibis._.temporal_consistency_score.min(),
        max_temporal=ibis._.temporal_consistency_score.max(),
        avg_fps=ibis._.fps.mean(),
        min_fps=ibis._.fps.min(),
        max_fps=ibis._.fps.max(),
        avg_seconds=ibis._.seconds.mean(),
        min_seconds=ibis._.seconds.min(),
        max_seconds=ibis._.seconds.max(),
    )
    mo.vstack(
        [
            mo.md("### Summary Statistics"),
            stats.to_pyarrow(),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Export Training Dataset

    The filtered Arrow table above can be saved directly to Parquet for model
    training, or loaded back into a new dlt pipeline targeting a different
    destination.

    ```python
    import pyarrow.parquet as pq

    pq.write_table(filtered_arrow, "training_data.parquet")
    ```

    Or pipe through a new dlt pipeline:

    ```python
    training_pipeline = dlt.pipeline(
        "openvid_training",
        destination="lance",  # or any warehouse
        dataset_name="training",
    )
    training_pipeline.run(
        [filtered_arrow],
        table_name="curated_videos",
    )
    ```
    """)
    return


@app.cell
def _(filtered_arrow):
    mo.vstack(
        [
            mo.md("### Training Dataset Preview"),
            mo.md(
                f"Ready for export: **{filtered_arrow.num_rows} rows** × "
                f"**{filtered_arrow.num_columns} columns** "
                f"({filtered_arrow.column_names})"
            ),
            filtered_arrow,
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Vector Search Example

    LanceDB supports vector search on embedded columns. The pipeline embeds the
    `caption` field.

    ```python
    with pipeline.destination_client() as client:
        db = client.db_client
        videos_table = db.open_table("openvid___videos")

        results = (
            videos_table.search(
                "a cinematic sunset over the ocean",
                vector_column_name="vector",
            )
            .limit(10)
            .to_arrow()
        )
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
