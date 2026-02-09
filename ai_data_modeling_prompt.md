# GitHub Data Transformations with AI

Generate SQL transformations to convert raw GitHub data into a normalized schema matching the [Fivetran GitHub ERD](https://fivetran.com/connector-erd/github).

## Overview

This workflow assumes you already have raw GitHub data loaded via dlt. Your AI assistant will:

1. Inspect your raw data schema
2. Reference the Fivetran GitHub ERD as the target model
3. Generate transformation SQL to normalize your data
4. Create transformed tables matching the Fivetran schema
5. Build a simple marimo dashboard to visualize the transformed data

## Prerequisites

- Raw GitHub data already loaded in DuckDB (from `github_example.py` or similar)
- An AI coding assistant (Claude, GitHub Copilot, Cursor, etc.)

## Using AI to Generate Transformations

### Step 1: Provide Context

Tell your AI assistant about your data and goal:

```
I have raw GitHub data loaded in DuckDB with these tables:
- issues (with nested comments, labels, reactions)
- contributors (with user details and permissions)
- pull_requests (with nested reviews)

I want to transform this into a normalized schema matching the Fivetran GitHub connector:
https://fivetran.com/connector-erd/github

Please:
1. Inspect my raw data schema in DuckDB
2. Reference the Fivetran ERD to understand the target schema
3. Generate dlt transformation resources to create these normalized tables:
   - repository
   - user
   - issue
   - pull_request
   - issue_comment
   - issue_label
   - label
   - collaborator
   - review (if PR data is available)

The transformations should:
- Denormalize nested structures (unnest arrays)
- Deduplicate users from multiple sources
- Create proper foreign key relationships
- Handle nulls appropriately

After transformations are complete, create a marimo dashboard with:
- 3-5 simple visualizations
- SQL queries using JOINs across tables
- Interactive UI elements
- Keep it lean and simple
```

### Step 2: AI Generates Transformations

Your AI assistant will:

1. **Connect to your DuckDB** to inspect the raw schema:
   ```sql
   SHOW TABLES;
   DESCRIBE issues;
   SELECT * FROM issues LIMIT 1;
   ```

2. **Fetch the Fivetran ERD** from the URL to understand the target schema

3. **Generate transformation SQL or dlt resources** like:

   **Option A: SQL Views (for querying in DuckDB)**
   ```sql
   -- Extract and deduplicate users
   CREATE OR REPLACE VIEW github_normalized.user AS
   SELECT DISTINCT
       author__id as id,
       author__login as login,
       author__name as name,
       author__email as email,
       author__type as type,
       author__company as company,
       author__location as location,
       author__bio as bio,
       author__created_at as created_at
   FROM issues
   UNION
   SELECT DISTINCT
       user__id as id,
       user__login as login,
       user__name as name,
       user__email as email,
       user__type as type,
       user__company as company,
       user__location as location,
       user__bio as bio,
       user__created_at as created_at
   FROM contributors;

   -- Extract repository
   CREATE OR REPLACE VIEW github_normalized.repository AS
   SELECT DISTINCT
       repository__id as id,
       repository__name as name,
       repository__full_name as full_name,
       repository__owner__id as owner_id,
       repository__created_at as created_at,
       repository__default_branch as default_branch,
       repository__description as description,
       repository__language as language,
       repository__stargazers_count as stargazers_count
   FROM issues
   LIMIT 1;

   -- Extract issues (excluding pull requests)
   CREATE OR REPLACE VIEW github_normalized.issue AS
   SELECT
       id,
       repository__id as repository_id,
       author__id as user_id,
       number,
       state,
       title,
       body,
       created_at,
       updated_at,
       closed_at,
       CASE WHEN pull_request IS NOT NULL THEN true ELSE false END as is_pull_request
   FROM issues;

   -- Extract pull requests
   CREATE OR REPLACE VIEW github_normalized.pull_request AS
   SELECT
       id,
       id as issue_id,  -- In GitHub API, PRs are also issues
       pull_request__merged_at as merged_at,
       pull_request__base__ref as base_ref,
       pull_request__head__ref as head_ref,
       pull_request__base__sha as base_sha,
       pull_request__head__sha as head_sha
   FROM issues
   WHERE pull_request IS NOT NULL;

   -- Unnest issue comments
   CREATE OR REPLACE VIEW github_normalized.issue_comment AS
   SELECT
       comment.id as id,
       issues.id as issue_id,
       comment.user__id as user_id,
       comment.body as body,
       comment.created_at as created_at,
       comment.updated_at as updated_at
   FROM issues,
   UNNEST(comments) as comment;

   -- Extract labels
   CREATE OR REPLACE VIEW github_normalized.label AS
   SELECT DISTINCT
       label.id as id,
       issues.repository__id as repository_id,
       label.name as name,
       label.color as color,
       label.description as description
   FROM issues,
   UNNEST(labels) as label;

   -- Create issue_label junction table
   CREATE OR REPLACE VIEW github_normalized.issue_label AS
   SELECT
       issues.id as issue_id,
       label.id as label_id
   FROM issues,
   UNNEST(labels) as label;

   -- Extract collaborators
   CREATE OR REPLACE VIEW github_normalized.collaborator AS
   SELECT
       user__id as user_id,
       -- Assuming repository_id from context
       (SELECT repository__id FROM issues LIMIT 1) as repository_id,
       permission
   FROM contributors;
   ```

   **Option B: dlt Transformations (for reusable pipelines)**
   ```python
   import dlt
   import duckdb
   from typing import Iterator, Dict, Any

   @dlt.resource(write_disposition="replace")
   def normalized_users() -> Iterator[Dict[str, Any]]:
       """Deduplicated user records from multiple sources"""
       conn = duckdb.connect('.dlt/github_data/dlthub_data.duckdb')

       result = conn.execute("""
           SELECT DISTINCT
               author__id as id,
               author__login as login,
               author__name as name,
               author__email as email
           FROM issues
           UNION
           SELECT DISTINCT
               user__id as id,
               user__login as login,
               user__name as name,
               user__email as email
           FROM contributors
       """).df()

       yield from result.to_dict('records')

   @dlt.resource(write_disposition="replace")
   def normalized_issues() -> Iterator[Dict[str, Any]]:
       """Issues in Fivetran schema format"""
       conn = duckdb.connect('.dlt/github_data/dlthub_data.duckdb')

       result = conn.execute("""
           SELECT
               id,
               repository__id as repository_id,
               author__id as user_id,
               number,
               state,
               title,
               body,
               created_at
           FROM issues
       """).df()

       yield from result.to_dict('records')

   # Run the transformation pipeline
   if __name__ == "__main__":
       pipeline = dlt.pipeline(
           pipeline_name="github_transformations",
           destination="duckdb",
           dataset_name="github_normalized"
       )

       load_info = pipeline.run([
           normalized_users,
           normalized_issues,
           # ... other transformation resources
       ])

       print(load_info)
   ```

4. **Execute the transformations** to create the views/tables or run the dlt pipeline

5. **Validate the results** with sample queries

### Step 3: Review and Refine

Your AI assistant will show you:
- The generated SQL
- Sample data from each transformed table
- Row counts and basic validation

You can then refine by asking:
```
The issue_comment table looks good, but can you also extract reactions for each comment?
```

Or:
```
Can you materialize these views as tables instead for better performance?
```

### Step 4: Create a Marimo Dashboard

After transformations are complete, ask your AI assistant to create an interactive dashboard:

```
Create a marimo dashboard to visualize the transformed GitHub data.

The dashboard should:
1. Connect to the transformed DuckDB database
2. Include 3-5 simple visualizations showing:
   - User activity metrics (top commenters, engagement)
   - Issue statistics (state distribution, average time to close)
   - Trend analysis (issues over time)
3. Use SQL queries with JOINs to combine data from multiple tables
4. Keep it simple and lean - use marimo's built-in UI elements
5. Save as `github_dashboard.py`

Example queries to include:
- Most active users (JOIN user with issue_comment)
- Issue resolution metrics (aggregate on issue table)
- Activity timeline (GROUP BY date)
```

Your AI assistant will create a marimo app like:

```python
import marimo as mo
import duckdb
import plotly.express as px

app = mo.App()

@app.cell
def connect_db():
    conn = duckdb.connect('_local/dev/github_transformations.duckdb')
    schema = "github_normalized_YYYYMMDDHHMMSS"
    return conn, schema

@app.cell
def top_commenters(conn, schema):
    df = conn.execute(f"""
        SELECT
            u.login,
            u.name,
            COUNT(c.id) as comment_count
        FROM {schema}.user u
        JOIN {schema}.issue_comment c ON u.login = c.user_login
        GROUP BY u.login, u.name
        ORDER BY comment_count DESC
        LIMIT 10
    """).df()

    fig = px.bar(df, x='login', y='comment_count', title='Top Commenters')
    return mo.ui.plotly(fig)

@app.cell
def issue_states(conn, schema):
    df = conn.execute(f"""
        SELECT state, COUNT(*) as count
        FROM {schema}.issue
        GROUP BY state
    """).df()

    fig = px.pie(df, values='count', names='state', title='Issue States')
    return mo.ui.plotly(fig)
```

Run the dashboard with:
```bash
marimo edit github_dashboard.py
```

## Example Session

**You:**
```
I have raw GitHub data in DuckDB. Generate transformations to match the Fivetran schema at https://fivetran.com/connector-erd/github
```

**Your AI assistant will:**
1. Ask for your DuckDB database path (or find it automatically)
2. Inspect your raw tables
3. Fetch and parse the Fivetran ERD
4. Generate transformation SQL or dlt resources (your choice)
5. Create a new schema `github_normalized` with transformed tables
6. Provide summary statistics
7. Create a marimo dashboard for data visualization

**Result:**
```
Created 10 normalized tables in github_normalized schema:
   - repository (1 row)
   - user (247 rows, deduplicated from 523 raw references)
   - issue (156 rows)
   - pull_request (42 rows)
   - issue_comment (892 rows, unnested from issues.comments)
   - label (28 rows, deduplicated)
   - issue_label (312 rows)
   - collaborator (15 rows)
   - milestone (8 rows)
   - review (67 rows, unnested from pull_requests)
```

## Advanced Usage

### Handle Multiple Repositories

If you have data from multiple repositories:

```
I have GitHub data from 3 different repositories in the same database.
Update the transformations to preserve repository_id context in all tables.
```

### Add Custom Analytics Tables

After generating the base transformations:

```
Now create additional analytics tables:
1. contributor_metrics - aggregate contribution stats per user
2. issue_velocity - time-to-close metrics by label
3. pr_review_time - PR approval velocity
```

### Generate dlt Transformations

Instead of raw SQL views:

```
Generate these transformations as dlt @dlt.resource functions.
Create Python transformation pipeline using dlt that I can run repeatedly.
```

Your AI assistant will create a Python file like:
```python
import dlt
import duckdb

@dlt.resource
def github_normalized_users():
    """Extract and deduplicate users from raw tables"""
    conn = duckdb.connect('path/to/github.duckdb')
    query = """
        SELECT DISTINCT
            author__id as id,
            author__login as login,
            author__name as name,
            ...
        FROM issues
        UNION
        SELECT DISTINCT
            user__id as id,
            user__login as login,
            ...
        FROM contributors
    """
    return conn.execute(query).df().to_dict('records')

@dlt.resource
def github_normalized_issues():
    """Extract issues matching Fivetran schema"""
    # transformation logic
    pass

# Run transformations
pipeline = dlt.pipeline(
    pipeline_name="github_normalized",
    destination="duckdb",
    dataset_name="github_normalized"
)
pipeline.run([github_normalized_users(), github_normalized_issues(), ...])
```

### Load Transformations to Different Destinations

With dlt transformations, you can load to any destination:

```
Generate dlt transformations that load the normalized data to BigQuery.
Update the pipeline destination and handle any BigQuery-specific data types.
```

Your AI assistant will update the pipeline:
```python
pipeline = dlt.pipeline(
    pipeline_name="github_transformations",
    destination="bigquery",  # or "snowflake", "postgres", "motherduck"
    dataset_name="github_normalized"
)
```

## Tips for Best Results

### Be Specific About Your Raw Schema

If your raw data has unusual structures:
```
My issues table has nested comments in a JSON column called `issue_comments`
(not `comments`). Please adjust the transformations accordingly.
```

### Request Validation

```
After generating transformations, write validation queries to check:
- No duplicate IDs in primary key columns
- All foreign keys have matching records
- No unexpected nulls in required fields
```

### Ask for Explanations

```
Explain why you chose to UNION users from issues and contributors
rather than JOIN them.
```

### Incremental Transformations

```
Make the dlt transformations incremental using merge write disposition.
Set up primary keys and use upsert logic so I can run them repeatedly
as new data is loaded without duplicates.
```

Your AI assistant will add incremental configuration:
```python
@dlt.resource(
    write_disposition="merge",
    primary_key="id"
)
def normalized_issues():
    # transformation logic with upsert behavior
    pass
```

## Troubleshooting

If your AI assistant generates incorrect transformations:

1. **Provide a sample row**:
   ```
   Here's what the raw issues table actually looks like:
   SELECT * FROM issues LIMIT 1;
   [paste output]
   ```

2. **Clarify the target schema**:
   ```
   The Fivetran ERD shows `user_id` but you generated `author_id`.
   Please use the exact column names from the ERD.
   ```

3. **Point out errors**:
   ```
   The issue_comment transformation failed because comments is already
   an array, not nested JSON. Fix the UNNEST syntax.
   ```

## Next Steps

1. Start a session with your AI coding assistant
2. Paste the prompt from Step 1 above
3. Review the generated transformations
4. Run queries on the normalized data
