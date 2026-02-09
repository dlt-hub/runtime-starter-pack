# GitHub Data Transformations

This project transforms raw GitHub data into a normalized schema suitable for analytics and reporting.

## Overview

The transformation pipeline converts raw GitHub data from the `github_pipeline.py` into a normalized schema with these tables:

- **repository**: Repository metadata
- **user**: Deduplicated user records (516 users from 39 collaborators + issue authors + commenters)
- **issue**: GitHub issues (1000 issues)
- **issue_comment**: Comments on issues (1817 comments)
- **collaborator**: Repository collaborators with permissions (39 collaborators)
- **issue_reaction**: Reactions on issues (327 reactions)
- **comment_reaction**: Reactions on comments (350 reactions)

## Files

- `github_pipeline.py`: Loads raw GitHub data from the dlt-hub/dlt repository
- `github_transformations.py`: dlt transformation pipeline (recommended)
- `github_transformations.sql`: SQL views for DuckDB (alternative approach)
- `github_dashboard.py`: Interactive marimo dashboard for data visualization
- `example_queries.py`: Example SQL queries for analytics

## Quick Start

### Step 1: Load Raw Data

```bash
uv run python github_pipeline.py
```

This loads:
- Issues from dlt-hub/dlt (max 1000)
- Contributors/collaborators
- Comments and reactions on issues

Raw data is stored in: `_local/dev/github_to_duckdb.duckdb`

### Step 2: Run Transformations

```bash
uv run python github_transformations.py
```

This creates normalized tables in: `_local/dev/github_transformations.duckdb`

### Step 3: Launch the Dashboard

```bash
marimo edit github_dashboard.py
```

This opens an interactive dashboard in your browser with:
- User engagement metrics
- Issue statistics and trends
- Reaction analysis
- Collaborator overview

The dashboard uses SQL JOINs across multiple tables to create visualizations.

## Transformation Details

### User Deduplication

Users appear in multiple places (issue authors, comment authors, collaborators). The transformation:
1. Extracts users from all sources
2. Prioritizes detailed user info from collaborators table (includes name, bio, company, location)
3. Deduplicates by `login`

### Schema Mapping

| Raw Data | Normalized Table | Transformation |
|----------|------------------|----------------|
| `issues` | `issue` | Flattened issue records |
| `issues__comments` | `issue_comment` | Unnested comments with parent issue link |
| `issues__reactions` | `issue_reaction` | Unnested reactions on issues |
| `issues__comments__reactions` | `comment_reaction` | Unnested reactions on comments |
| `contributors` | `collaborator` + `user` | Split into permissions and user details |

## Sample Queries

### Most Active Commenters

```sql
SELECT
    user_login,
    COUNT(*) as comment_count
FROM github_normalized_YYYYMMDDHHMMSS.issue_comment
GROUP BY user_login
ORDER BY comment_count DESC
LIMIT 10;
```

Result:
- rudolfix: 435 comments
- sh-rp: 213 comments
- burnash: 111 comments
- zilto: 104 comments
- anuunchin: 41 comments

### Issue Activity by State

```sql
SELECT
    state,
    COUNT(*) as issue_count,
    AVG(comments_total_count) as avg_comments
FROM github_normalized_YYYYMMDDHHMMSS.issue
GROUP BY state;
```

### Most Popular Reaction Types

```sql
SELECT
    reaction_type,
    COUNT(*) as count
FROM github_normalized_YYYYMMDDHHMMSS.issue_reaction
GROUP BY reaction_type
ORDER BY count DESC;
```

Result:
- THUMBS_UP: 269 reactions
- EYES: 27 reactions
- ROCKET: 22 reactions
- HEART: 9 reactions

### Issues with Most Engagement

```sql
SELECT
    i.number,
    i.title,
    i.state,
    i.comments_total_count,
    i.reactions_total_count,
    COUNT(DISTINCT c.id) as actual_comments,
    COUNT(DISTINCT r.id) as actual_reactions
FROM github_normalized_YYYYMMDDHHMMSS.issue i
LEFT JOIN github_normalized_YYYYMMDDHHMMSS.issue_comment c ON i.id = c.issue_id
LEFT JOIN github_normalized_YYYYMMDDHHMMSS.issue_reaction r ON i.id = r.issue_id
GROUP BY i.number, i.title, i.state, i.comments_total_count, i.reactions_total_count
ORDER BY (i.comments_total_count + i.reactions_total_count) DESC
LIMIT 10;
```

## Customization

### Change Repository

Edit `github_pipeline.py`:

```python
OWNER = "your-org"
REPO = "your-repo"
MAX_ISSUES = 1000  # Adjust as needed
```

### Modify Transformations

Edit `github_transformations.py` to:
- Add new calculated fields
- Create additional aggregate tables
- Implement incremental transformations

Example - Add user statistics:

```python
@dlt.resource(write_disposition="replace", table_name="user_stats")
def user_statistics():
    """Calculate user engagement statistics."""
    conn = duckdb.connect(SOURCE_DB, read_only=True)

    query = f"""
        SELECT
            u.login,
            u.name,
            COUNT(DISTINCT i.id) as issues_created,
            COUNT(DISTINCT c.id) as comments_made,
            SUM(i.comments_total_count) as total_engagement
        FROM {SOURCE_DATASET}.issues i
        LEFT JOIN {SOURCE_DATASET}.issues__comments c ON c.author__login = i.author__login
        CROSS JOIN {SOURCE_DATASET}.contributors u ON u.user__login = i.author__login
        GROUP BY u.login, u.name
    """

    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    for row in result:
        yield dict(zip(columns, row))
    conn.close()
```

### Load to Different Destinations

The transformations use dlt, so you can load to any supported destination:

```python
pipeline = dlt.pipeline(
    pipeline_name="github_transformations",
    destination="bigquery",  # or "snowflake", "postgres", etc.
    dataset_name="github_normalized",
)
```

## Schema Version

Note: dlt appends timestamps to dataset names for versioning. Your actual schema name will be:
- `github_data_YYYYMMDDHHMMSS` (raw data)
- `github_normalized_YYYYMMDDHHMMSS` (transformed data)

Use `dev_mode=False` in production to avoid timestamp suffixes.

## Alternative: SQL Views

If you prefer SQL-based transformations, use `github_transformations.sql`:

1. Update the dataset name in the SQL file
2. Execute in DuckDB:

```bash
duckdb _local/dev/github_to_duckdb.duckdb < github_transformations.sql
```

This creates views instead of materialized tables, which:
- Take no additional storage
- Always reflect current raw data
- May be slower for complex queries

## Troubleshooting

### Database not found

Ensure you've run `github_pipeline.py` first to load raw data.

### Schema name mismatch

Check the actual schema name in DuckDB:

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name LIKE 'github_%';
```

Update `SOURCE_DATASET` in `github_transformations.py` to match.

### Memory issues

If processing large datasets:
- Reduce `MAX_ISSUES` in `github_pipeline.py`
- Process data in batches
- Use SQL views instead of materialized tables

## Dashboard Features

The marimo dashboard (`github_dashboard.py`) provides:

1. **User Engagement** - Bar chart of top commenters with JOIN across user and issue_comment tables
2. **Issue Metrics** - Pie chart showing open vs closed issues with summary statistics
3. **Activity Timeline** - Combo chart showing monthly issues and cumulative growth
4. **Most Discussed Issues** - Interactive table of issues with highest engagement
5. **Reaction Analysis** - Bar chart of reaction types (👍, ❤️, 🚀, etc.) across issues and comments
6. **Collaborator Overview** - Permission breakdown (ADMIN, WRITE, etc.)

All visualizations use SQL queries that JOIN and aggregate data from the normalized tables.

## Next Steps

1. **Add Pull Requests**: Extend the pipeline to include PR data
2. **Add Labels**: Extract and normalize issue labels
3. **Add Milestones**: Track milestone data
4. **Add Time Series**: Create daily/weekly aggregates for trending
5. **Add Text Analytics**: Implement sentiment analysis on issue titles/descriptions
6. **Schedule Updates**: Set up incremental loads with dlt merge write disposition
7. **Extend Dashboard**: Add more visualizations or filters to the marimo dashboard

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [GitHub API Documentation](https://docs.github.com/en/rest)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Fivetran GitHub Data Model](https://fivetran.com/docs/transformations/data-models/github-data-model)

## Summary

You now have a normalized GitHub dataset ready for:
- Analytics dashboards
- Reporting and metrics
- Machine learning features
- Integration with BI tools (Metabase, Tableau, Looker, etc.)

The transformed data follows standard data warehouse patterns with proper:
- Primary keys (user login, issue id)
- Foreign keys (issue_id in comments, user_login in issues/comments)
- Denormalized structures (unnested arrays)
- Deduplication (users from multiple sources)
