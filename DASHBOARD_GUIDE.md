# GitHub Dashboard Guide

## Overview

The GitHub Analytics Dashboard is an interactive marimo app that visualizes the transformed GitHub data using charts and tables.

## Quick Start

### Launch the Dashboard

```bash
marimo edit github_dashboard.py
```

This opens the dashboard in your browser at `http://localhost:2718`

## Dashboard Components

### 1. 📊 User Engagement
**Query:** JOINs `user` and `issue_comment` tables
```sql
SELECT u.login, COUNT(c.id) as comment_count
FROM user u
JOIN issue_comment c ON u.login = c.user_login
GROUP BY u.login
ORDER BY comment_count DESC
```

**Visualization:** Bar chart showing top 10 most active commenters

### 2. 🎯 Issue Metrics
**Query:** Aggregates `issue` table by state
```sql
SELECT state, COUNT(*) as count,
       AVG(comments_total_count) as avg_comments
FROM issue
GROUP BY state
```

**Visualizations:**
- Pie chart: Open vs Closed distribution
- Summary stats: Total issues, close rate

### 3. 📈 Activity Timeline
**Query:** Time series aggregation with window function
```sql
SELECT DATE_TRUNC('month', created_at) as month,
       COUNT(*) as issues_created,
       SUM(COUNT(*)) OVER (ORDER BY month) as cumulative
FROM issue
GROUP BY month
```

**Visualization:** Combo chart with bars (monthly) and line (cumulative)

### 4. 💬 Most Discussed Issues
**Query:** Sorts issues by total engagement
```sql
SELECT number, title, state,
       comments_total_count, reactions_total_count
FROM issue
ORDER BY (comments_total_count + reactions_total_count) DESC
```

**Visualization:** Interactive data table

### 5. 😊 Reaction Analysis
**Query:** UNIONs reactions from issues and comments
```sql
SELECT reaction_type, COUNT(*) as count
FROM (
    SELECT reaction_type FROM issue_reaction
    UNION ALL
    SELECT reaction_type FROM comment_reaction
)
GROUP BY reaction_type
```

**Visualization:** Bar chart with emoji labels (👍, ❤️, 🚀, 👀)

### 6. 🤝 Collaborator Overview
**Query:** Aggregates collaborator permissions
```sql
SELECT permission, COUNT(*) as count
FROM collaborator
GROUP BY permission
```

**Visualization:** Donut chart showing permission breakdown

## Key Features

### Simple and Lean Design
- No complex state management
- Direct SQL queries in each cell
- Built-in marimo UI components (plotly, table)
- Clean, readable code structure

### SQL Query Patterns

**JOINs across tables:**
```python
conn.execute(f"""
    SELECT u.login, COUNT(c.id)
    FROM {schema}.user u
    JOIN {schema}.issue_comment c ON u.login = c.user_login
    GROUP BY u.login
""").df()
```

**Aggregations:**
```python
conn.execute(f"""
    SELECT state, COUNT(*) as count
    FROM {schema}.issue
    GROUP BY state
""").df()
```

**Window functions:**
```python
conn.execute(f"""
    SELECT month,
           SUM(COUNT(*)) OVER (ORDER BY month) as cumulative
    FROM issue
    GROUP BY month
""").df()
```

## How It Works

### 1. Database Connection
```python
conn = duckdb.connect('_local/dev/github_transformations.duckdb', read_only=True)
```

### 2. Schema Discovery
```python
schema = conn.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name LIKE 'github_normalized%'
    ORDER BY schema_name DESC
    LIMIT 1
""").fetchone()[0]
```

### 3. Query Execution
```python
df = conn.execute(f"SELECT ... FROM {schema}.table").df()
```

### 4. Visualization
```python
fig = px.bar(df, x='column1', y='column2')
mo.ui.plotly(fig)
```

## Customization

### Add a New Visualization

1. Create a new cell in the dashboard:
```python
@app.cell
def my_metric(conn, schema, mo, px):
    # Your SQL query
    df = conn.execute(f"""
        SELECT column1, COUNT(*) as count
        FROM {schema}.my_table
        GROUP BY column1
    """).df()

    # Create visualization
    fig = px.line(df, x='column1', y='count', title='My Metric')

    return mo.ui.plotly(fig)
```

2. Add a markdown header:
```python
@app.cell
def __(mo):
    mo.md("## 📊 My Custom Metric")
    return
```

### Change Visualization Type

Marimo supports multiple chart libraries:

**Plotly (recommended):**
```python
import plotly.express as px
fig = px.bar(df, ...)
mo.ui.plotly(fig)
```

**Altair:**
```python
import altair as alt
chart = alt.Chart(df).mark_bar().encode(x='col1', y='col2')
mo.ui.altair(chart)
```

**Matplotlib:**
```python
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.bar(df['x'], df['y'])
mo.ui.pyplot(fig)
```

### Add Interactivity

**Dropdown filter:**
```python
state_filter = mo.ui.dropdown(
    options=['OPEN', 'CLOSED', 'ALL'],
    value='ALL',
    label='Filter by state'
)
```

**Date range picker:**
```python
date_range = mo.ui.date_range(
    start='2025-01-01',
    stop='2026-01-01',
    label='Select date range'
)
```

## Performance Tips

1. **Use indexes:** DuckDB automatically indexes, but you can add explicit indexes for complex queries
2. **Limit large result sets:** Add `LIMIT` clauses to queries returning many rows
3. **Cache expensive queries:** Store query results in variables if used multiple times
4. **Use aggregations:** Pre-aggregate data in SQL rather than in Python

## Troubleshooting

### Schema not found
```python
# Check available schemas
conn.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
""").fetchall()
```

### Empty visualizations
- Ensure transformations have been run (`github_transformations.py`)
- Check that the database path is correct
- Verify data exists: `SELECT COUNT(*) FROM {schema}.issue`

### Dashboard not loading
- Check marimo is installed: `uv pip list | grep marimo`
- Install plotly if needed: `uv pip install plotly pandas`
- Run with debug mode: `marimo edit github_dashboard.py --debug`

## Export Options

### Static HTML
```bash
marimo export html github_dashboard.py > dashboard.html
```

### Python Script
```bash
marimo export script github_dashboard.py > dashboard_script.py
```

### PDF (requires additional setup)
Use browser's print-to-PDF feature from the running dashboard

## Deployment

### Run as Web App
```bash
marimo run github_dashboard.py --host 0.0.0.0 --port 8080
```

### Deploy to Cloud
Marimo apps can be deployed to:
- **Marimo Cloud** (recommended): `marimo cloud deploy github_dashboard.py`
- **Hugging Face Spaces**: Add as marimo app
- **Docker**: Use official marimo Docker image

## Next Steps

1. **Add filters**: Let users filter by date range, user, or state
2. **Add drill-downs**: Click on a bar to see detailed data
3. **Add exports**: Download data as CSV from the dashboard
4. **Add real-time updates**: Refresh data automatically
5. **Add more metrics**: Time to close, response time, etc.

## Resources

- [Marimo Documentation](https://docs.marimo.io)
- [Plotly Express Gallery](https://plotly.com/python/plotly-express/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [GitHub Transformations README](GITHUB_TRANSFORMATIONS_README.md)
