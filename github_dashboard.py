import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import plotly.express as px
    import plotly.graph_objects as go
    return duckdb, go, mo, px


@app.cell
def _(mo):
    mo.md("""
    # GitHub Analytics Dashboard

    Explore insights from the dlt-hub/dlt repository data.
    """)
    return


@app.cell
def _(duckdb):
    # Connect to transformed database
    conn = duckdb.connect('_local/dev/github_transformations.duckdb', read_only=True)

    # Find the latest schema
    schema = conn.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'github_normalized%'
        ORDER BY schema_name DESC
        LIMIT 1
    """).fetchone()[0]

    print(f"Connected to schema: {schema}")
    return conn, schema


@app.cell
def _(mo):
    mo.md("""
    ## 📊 User Engagement
    """)
    return


@app.cell
def _(conn, mo, px, schema):
    # Top 10 most active commenters
    top_commenters_df = conn.execute(f"""
        SELECT
            u.login,
            COALESCE(u.name, u.login) as display_name,
            COUNT(c.id) as comment_count
        FROM {schema}.user u
        JOIN {schema}.issue_comment c ON u.login = c.user_login
        GROUP BY u.login, u.name
        ORDER BY comment_count DESC
        LIMIT 10
    """).df()

    fig_commenters = px.bar(
        top_commenters_df,
        x='display_name',
        y='comment_count',
        title='Top 10 Most Active Commenters',
        labels={'display_name': 'User', 'comment_count': 'Comments'},
        color='comment_count',
        color_continuous_scale='Blues'
    )
    fig_commenters.update_layout(showlegend=False)

    mo.ui.plotly(fig_commenters)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 🎯 Issue Metrics
    """)
    return


@app.cell
def _(conn, go, mo, schema):
    # Issue state distribution and metrics
    issue_metrics = conn.execute(f"""
        SELECT
            state,
            COUNT(*) as count,
            AVG(comments_total_count) as avg_comments,
            AVG(reactions_total_count) as avg_reactions
        FROM {schema}.issue
        GROUP BY state
    """).fetchall()

    # Pie chart for state distribution
    states = [row[0] for row in issue_metrics]
    counts = [row[1] for row in issue_metrics]

    fig_states = go.Figure(data=[go.Pie(
        labels=states,
        values=counts,
        hole=.3,
        marker=dict(colors=['#2ecc71', '#e74c3c'])
    )])
    fig_states.update_layout(title='Issue State Distribution')

    # Summary metrics
    total_issues = sum(counts)
    open_issues = counts[states.index('OPEN')] if 'OPEN' in states else 0
    closed_issues = counts[states.index('CLOSED')] if 'CLOSED' in states else 0
    close_rate = (closed_issues / total_issues * 100) if total_issues > 0 else 0

    mo.vstack([
        mo.ui.plotly(fig_states),
        mo.md(f"""
        **Summary:**
        - Total Issues: {total_issues}
        - Open: {open_issues} ({open_issues/total_issues*100:.1f}%)
        - Closed: {closed_issues} ({close_rate:.1f}%)
        """)
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ## 📈 Activity Timeline
    """)
    return


@app.cell
def _(conn, go, mo, schema):
    # Issues created over time
    timeline_df = conn.execute(f"""
        SELECT
            DATE_TRUNC('month', created_at) as month,
            COUNT(*) as issues_created,
            SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', created_at)) as cumulative_issues
        FROM {schema}.issue
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
    """).df()

    fig_timeline = go.Figure()

    # Bar chart for monthly issues
    fig_timeline.add_trace(go.Bar(
        x=timeline_df['month'],
        y=timeline_df['issues_created'],
        name='Monthly Issues',
        marker_color='lightblue'
    ))

    # Line chart for cumulative
    fig_timeline.add_trace(go.Scatter(
        x=timeline_df['month'],
        y=timeline_df['cumulative_issues'],
        name='Cumulative',
        yaxis='y2',
        line=dict(color='darkblue', width=2)
    ))

    fig_timeline.update_layout(
        title='Issue Creation Timeline',
        xaxis_title='Month',
        yaxis_title='Monthly Issues',
        yaxis2=dict(
            title='Cumulative Issues',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified'
    )

    mo.ui.plotly(fig_timeline)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 💬 Most Discussed Issues
    """)
    return


@app.cell
def _(conn, mo, schema):
    # Top 10 most discussed issues
    discussed_df = conn.execute(f"""
        SELECT
            number,
            title,
            state,
            comments_total_count,
            reactions_total_count,
            user_login as author
        FROM {schema}.issue
        ORDER BY (comments_total_count + reactions_total_count) DESC
        LIMIT 10
    """).df()

    # Create a simple table
    mo.ui.table(
        discussed_df,
        selection=None,
        pagination=True,
        page_size=10
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## 😊 Reaction Analysis
    """)
    return


@app.cell
def _(conn, go, mo, schema):
    # Reaction type distribution across issues and comments
    reactions_df = conn.execute(f"""
        WITH all_reactions AS (
            SELECT reaction_type FROM {schema}.issue_reaction
            UNION ALL
            SELECT reaction_type FROM {schema}.comment_reaction
        )
        SELECT
            reaction_type,
            COUNT(*) as count
        FROM all_reactions
        GROUP BY reaction_type
        ORDER BY count DESC
    """).df()

    # Emoji mapping for better display
    emoji_map = {
        'THUMBS_UP': '👍',
        'THUMBS_DOWN': '👎',
        'LAUGH': '😄',
        'HOORAY': '🎉',
        'CONFUSED': '😕',
        'HEART': '❤️',
        'ROCKET': '🚀',
        'EYES': '👀'
    }

    reactions_df['emoji'] = reactions_df['reaction_type'].map(
        lambda x: f"{emoji_map.get(x, '')} {x}"
    )

    fig_reactions = go.Figure(data=[go.Bar(
        x=reactions_df['emoji'],
        y=reactions_df['count'],
        marker=dict(
            color=reactions_df['count'],
            colorscale='Viridis',
            showscale=True
        )
    )])

    fig_reactions.update_layout(
        title='Reaction Types Distribution',
        xaxis_title='Reaction',
        yaxis_title='Count',
        showlegend=False
    )

    mo.ui.plotly(fig_reactions)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 🤝 Collaborator Overview
    """)
    return


@app.cell
def _(conn, mo, px, schema):
    # Collaborator permissions breakdown
    collab_df = conn.execute(f"""
        SELECT
            c.permission,
            COUNT(*) as count
        FROM {schema}.collaborator c
        GROUP BY c.permission
        ORDER BY count DESC
    """).df()

    fig_collab = px.pie(
        collab_df,
        values='count',
        names='permission',
        title='Collaborator Permissions',
        hole=.3,
        color_discrete_sequence=px.colors.sequential.RdBu
    )

    mo.ui.plotly(fig_collab)
    return


@app.cell
def _(mo, schema):
    mo.md(f"""
    ---

    **Data Source:** `{schema}` in `_local/dev/github_transformations.duckdb`

    **Repository:** dlt-hub/dlt
    """)
    return


@app.cell
def _(conn):
    # Clean up
    conn.close()
    return


if __name__ == "__main__":
    app.run()
