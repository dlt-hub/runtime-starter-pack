"""
GitHub Data Transformations
Transforms raw GitHub data into a normalized schema matching standard GitHub ERD patterns.

This creates the following normalized tables:
- repository: Repository metadata
- user: Deduplicated user records from multiple sources
- issue: Issues (excluding pull_request info)
- issue_comment: Comments on issues
- collaborator: Repository collaborators with permissions
"""

import dlt
import duckdb
from typing import Iterator, Dict, Any
from pathlib import Path


# Path to source database
SOURCE_DB = "_local/dev/github_to_duckdb.duckdb"
SOURCE_DATASET = "github_data_20260209083051"


@dlt.resource(write_disposition="replace", table_name="repository")
def normalized_repository() -> Iterator[Dict[str, Any]]:
    """Extract repository metadata from issues data."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT DISTINCT
            'dlt-hub/dlt' as full_name,
            'dlt' as name,
            'dlt-hub' as owner_login,
            MIN(created_at) as created_at
        FROM {SOURCE_DATASET}.issues
        LIMIT 1
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="user")
def normalized_users() -> Iterator[Dict[str, Any]]:
    """Deduplicate users from issues and contributors."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT DISTINCT
            author__login as login,
            author__url as url,
            author__avatar_url as avatar_url,
            NULL as name,
            NULL as bio,
            NULL as company,
            NULL as location,
            NULL as created_at
        FROM {SOURCE_DATASET}.issues
        WHERE author__login IS NOT NULL

        UNION

        SELECT DISTINCT
            author__login as login,
            author__url as url,
            author__avatar_url as avatar_url,
            NULL as name,
            NULL as bio,
            NULL as company,
            NULL as location,
            NULL as created_at
        FROM {SOURCE_DATASET}.issues__comments
        WHERE author__login IS NOT NULL

        UNION

        SELECT DISTINCT
            user__login as login,
            user__url as url,
            user__avatar_url as avatar_url,
            user__name as name,
            user__bio as bio,
            user__company as company,
            user__location as location,
            user__created_at as created_at
        FROM {SOURCE_DATASET}.contributors
        WHERE user__login IS NOT NULL
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="issue")
def normalized_issues() -> Iterator[Dict[str, Any]]:
    """Extract issues in normalized format."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            _dlt_id as id,
            'dlt-hub/dlt' as repository_full_name,
            author__login as user_login,
            number,
            state,
            title,
            body,
            closed,
            created_at,
            updated_at,
            closed_at,
            reactions_total_count,
            comments_total_count,
            author_association
        FROM {SOURCE_DATASET}.issues
        ORDER BY number
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="issue_comment")
def normalized_issue_comments() -> Iterator[Dict[str, Any]]:
    """Extract and denormalize issue comments."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            c.id as id,
            c._dlt_parent_id as issue_id,
            c.author__login as user_login,
            c.body as body,
            c.created_at as created_at,
            c.author_association as author_association,
            c.reactions_total_count as reactions_total_count
        FROM {SOURCE_DATASET}.issues__comments c
        ORDER BY c.created_at
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="collaborator")
def normalized_collaborators() -> Iterator[Dict[str, Any]]:
    """Extract repository collaborators with permissions."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            user__login as user_login,
            'dlt-hub/dlt' as repository_full_name,
            permission
        FROM {SOURCE_DATASET}.contributors
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="issue_reaction")
def normalized_issue_reactions() -> Iterator[Dict[str, Any]]:
    """Extract reactions on issues."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            r._dlt_id as id,
            r._dlt_parent_id as issue_id,
            r.user__login as user_login,
            r.content as reaction_type,
            r.created_at as created_at
        FROM {SOURCE_DATASET}.issues__reactions r
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


@dlt.resource(write_disposition="replace", table_name="comment_reaction")
def normalized_comment_reactions() -> Iterator[Dict[str, Any]]:
    """Extract reactions on comments."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            r._dlt_id as id,
            r._dlt_parent_id as comment_id,
            r.user__login as user_login,
            r.content as reaction_type,
            r.created_at as created_at
        FROM {SOURCE_DATASET}.issues__comments__reactions r
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()


if __name__ == "__main__":
    # Create transformation pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_transformations",
        destination="duckdb",
        dataset_name="github_normalized",
    )

    print("Running GitHub data transformations...")
    print("Creating normalized tables:")
    print("  - repository")
    print("  - user")
    print("  - issue")
    print("  - issue_comment")
    print("  - collaborator")
    print("  - issue_reaction")
    print("  - comment_reaction")
    print()

    # Run all transformations
    load_info = pipeline.run([
        normalized_repository,
        normalized_users,
        normalized_issues,
        normalized_issue_comments,
        normalized_collaborators,
        normalized_issue_reactions,
        normalized_comment_reactions,
    ])

    print("Transformation complete!")
    print(f"Load info: {load_info}")
    print()

    # Print summary statistics
    with pipeline.sql_client() as client:
        print("=== TRANSFORMATION SUMMARY ===")
        tables = [
            'repository',
            'user',
            'issue',
            'issue_comment',
            'collaborator',
            'issue_reaction',
            'comment_reaction'
        ]

        for table in tables:
            with client.execute_query(
                f"SELECT COUNT(*) FROM {pipeline.dataset_name}.{table}"
            ) as cursor:
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} rows")
