"""Pipeline that ingests GitHub data and builds transformed analytics tables.

Raw tables ingested from the GitHub REST API:
    commits           – parent table  (61 columns)
    commits__parents  – child table   (6 columns, parent SHAs per commit)
    contributors      – standalone    (22 columns)

Parent-child relationship:
    commits._dlt_id  =  commits__parents._dlt_parent_id

Transformed tables:
    users               – contributors enriched with per-user commit stats
                          computed via the parent-child join
    commits             – flattened commits joined with the child table to add
                          parent_count and is_merge_commit
    daily_activity      – daily rollup of commits, merge commits, unique authors
"""

import dlt
import typing

import ibis
from ibis import ir

from github_pipeline import github_rest_api_source


# ── users ──────────────────────────────────────────────────────────────
@dlt.hub.transformation
def users(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Contributors enriched with commit stats via the parent-child join.

    Joins contributors → commits → commits__parents to add per-user metrics:
    total commits, merge commits, verified commits, first/last commit, avg message length.
    """
    contributors = dataset.table("contributors").to_ibis()
    commits = dataset.table("commits").to_ibis()
    parents = dataset.table("commits__parents").to_ibis()

    # ── select contributor fields ─────────────────────────────────────
    from_contributors = contributors.select(
        login=contributors.login,
        user_id=contributors.id,
        avatar_url=contributors.avatar_url,
        user_type=contributors.type,
        site_admin=contributors.site_admin,
        api_contributions=contributors.contributions,
    )

    # ── compute per-author commit stats via parent-child join ───────
    parent_counts = (
        parents
        .group_by(parents._dlt_parent_id)
        .aggregate(parent_count=parents._dlt_id.count())
    )

    commits_enriched = commits.left_join(
        parent_counts, commits._dlt_id == parent_counts._dlt_parent_id
    ).select(
        commits.author__login,
        commits.sha,
        commits.commit__author__date,
        commits.commit__message,
        commits.commit__verification__verified,
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
    )

    commit_stats = (
        commits_enriched
        .filter(commits_enriched.author__login.notnull())
        .group_by(commits_enriched.author__login)
        .aggregate(
            total_commits=commits_enriched.sha.count(),
            merge_commits=(commits_enriched.parent_count > 1).cast("int64").sum(),
            verified_commits=commits_enriched.commit__verification__verified.cast("int64").sum(),
            first_commit=commits_enriched.commit__author__date.min(),
            most_recent_commit=commits_enriched.commit__author__date.max(),
            avg_message_length=commits_enriched.commit__message.length().mean(),
        )
    )

    # ── enrich contributors with commit stats ─────────────────────────
    yield (
        from_contributors
        .left_join(commit_stats, from_contributors.login == commit_stats.author__login)
        .select(
            login=from_contributors.login,
            user_id=from_contributors.user_id,
            avatar_url=from_contributors.avatar_url,
            user_type=from_contributors.user_type,
            site_admin=from_contributors.site_admin,
            api_contributions=from_contributors.api_contributions,
            total_commits=ibis.coalesce(commit_stats.total_commits, 0),
            merge_commits=ibis.coalesce(commit_stats.merge_commits, 0),
            verified_commits=ibis.coalesce(commit_stats.verified_commits, 0),
            first_commit=commit_stats.first_commit,
            most_recent_commit=commit_stats.most_recent_commit,
            avg_message_length=commit_stats.avg_message_length,
        )
    )


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


# ── daily activity ─────────────────────────────────────────────────────
@dlt.hub.transformation
def daily_activity(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Daily rollup of commits using the parent-child join for merge detection."""
    commits = dataset.table("commits").to_ibis()
    parents = dataset.table("commits__parents").to_ibis()

    parent_counts = (
        parents
        .group_by(parents._dlt_parent_id)
        .aggregate(parent_count=parents._dlt_id.count())
    )

    enriched = commits.left_join(
        parent_counts, commits._dlt_id == parent_counts._dlt_parent_id
    ).select(
        commits.sha,
        commits.author__login,
        commits.commit__author__date,
        parent_count=ibis.coalesce(parent_counts.parent_count, 0),
    )

    yield (
        enriched
        .mutate(activity_date=enriched.commit__author__date.date())
        .group_by("activity_date")
        .aggregate(
            total_commits=enriched.sha.count(),
            merge_commits=(enriched.parent_count > 1).cast("int64").sum(),
            regular_commits=(enriched.parent_count <= 1).cast("int64").sum(),
            unique_authors=enriched.author__login.nunique(),
        )
    )


# ── source: all analytics tables ──────────────────────────────────────
@dlt.source
def github_analytics(raw_dataset: dlt.Dataset) -> list:
    """Transformed analytics tables derived from raw GitHub data."""
    return [
        users(raw_dataset),
        commits(raw_dataset),
        daily_activity(raw_dataset),
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
