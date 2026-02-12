"""Pipeline that ingests GitHub data and builds transformed tables.

Raw tables ingested from the GitHub REST API:
    commits           – parent table  (61 columns)
    commits__parents  – child table   (6 columns, parent SHAs per commit)
    contributors      – standalone    (22 columns)

Parent-child relationship:
    commits._dlt_id  =  commits__parents._dlt_parent_id

Transformed tables:
    commits             – flattened commits joined with the child table to add
                          parent_count and is_merge_commit
"""

import dlt
import typing

import ibis
from ibis import ir

from github_pipeline import github_rest_api_source



# ── commits ────────────────────────────────────────────────────────────
@dlt.hub.transformation
def commits(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Flattened commits joined with the child table.

    Joins commits (parent) with commits__parents (child) via:
        commits._dlt_id = commits__parents._dlt_parent_id
    to compute parent_count and is_merge_commit.
    """
    commits = dataset.table("commits").to_ibis()
    parents = dataset.table("commits__parents").to_ibis()

    parent_counts = (
        parents
        .group_by(parents._dlt_parent_id)
        .aggregate(parent_count=parents._dlt_id.count())
    )

    enriched = commits.left_join(
        parent_counts, commits._dlt_id == parent_counts._dlt_parent_id
    )

    yield enriched.select(
        sha=commits.sha,
        author_login=ibis.coalesce(commits.author__login, ibis.literal("unknown")),
        committer_login=ibis.coalesce(commits.committer__login, ibis.literal("unknown")),
        author_name=commits.commit__author__name,
        author_email=commits.commit__author__email,
        authored_at=commits.commit__author__date,
        committed_at=commits.commit__committer__date,
        message=commits.commit__message,
        message_length=commits.commit__message.length(),
        is_verified=commits.commit__verification__verified,
        comment_count=commits.commit__comment_count,
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
        is_merge_commit=ibis.coalesce(parent_counts.parent_count, 0) > 1,
    )



# ── source: all analytics tables ──────────────────────────────────────
@dlt.source
def github_analytics(raw_dataset: dlt.Dataset) -> list:
    """Transformed tables derived from raw GitHub data."""
    return [
        commits(raw_dataset),
    ]


if __name__ == "__main__":
    # ── 1. Ingest: load raw GitHub data into local duckdb ──────────────
    destination = dlt.destinations.duckdb("local_github.duckdb")
    github_ingest_pipe = dlt.pipeline("github_ingest", destination=destination)
    ingest_load_info = github_ingest_pipe.run(github_rest_api_source())
    print(ingest_load_info)

    # ── 2. Transform and load into warehouse ───────────────────────────
    github_transform_pipe = dlt.pipeline(
        "github_transform", destination="warehouse"
    )
    transform_load_info = github_transform_pipe.run(
        github_analytics(github_ingest_pipe.dataset())
    )
    print(transform_load_info)
