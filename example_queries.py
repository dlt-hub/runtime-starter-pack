"""
Example Queries for GitHub Normalized Data

Run these queries to explore your transformed GitHub data.
"""

import duckdb

# Connect to the transformed database
conn = duckdb.connect('_local/dev/github_transformations.duckdb', read_only=True)

# Find the latest schema
schema = conn.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name LIKE 'github_normalized%'
    ORDER BY schema_name DESC
    LIMIT 1
""").fetchone()[0]

print(f"Using schema: {schema}\n")
print("=" * 80)


# Query 1: User Engagement Metrics
print("\n1. TOP 10 MOST ENGAGED USERS")
print("-" * 80)
result = conn.execute(f"""
    WITH user_metrics AS (
        SELECT
            u.login,
            u.name,
            u.company,
            COUNT(DISTINCT i.id) as issues_authored,
            COUNT(DISTINCT c.id) as comments_made,
            COUNT(DISTINCT ir.id) as issue_reactions,
            COUNT(DISTINCT cr.id) as comment_reactions
        FROM {schema}.user u
        LEFT JOIN {schema}.issue i ON u.login = i.user_login
        LEFT JOIN {schema}.issue_comment c ON u.login = c.user_login
        LEFT JOIN {schema}.issue_reaction ir ON u.login = ir.user_login
        LEFT JOIN {schema}.comment_reaction cr ON u.login = cr.user_login
        GROUP BY u.login, u.name, u.company
    )
    SELECT
        login,
        name,
        company,
        issues_authored,
        comments_made,
        issue_reactions,
        comment_reactions,
        (comments_made + issue_reactions + comment_reactions) as total_activity
    FROM user_metrics
    WHERE total_activity > 0
    ORDER BY total_activity DESC
    LIMIT 10
""").fetchall()

for row in result:
    print(f"{row[0]:20} | {row[1] or '(no name)':25} | Comments: {row[4]:4} | Reactions: {row[5]+row[6]:3} | Total: {row[7]:4}")


# Query 2: Issue Resolution Time
print("\n\n2. AVERAGE TIME TO CLOSE ISSUES (for closed issues)")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        CAST(AVG(EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400) AS INT) as avg_days_to_close,
        COUNT(*) as closed_issues,
        MIN(CAST(EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400 AS INT)) as fastest_close_days,
        MAX(CAST(EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400 AS INT)) as slowest_close_days
    FROM {schema}.issue
    WHERE state = 'CLOSED'
    AND closed_at IS NOT NULL
""").fetchone()

print(f"Average days to close: {result[0]} days")
print(f"Total closed issues: {result[1]}")
print(f"Fastest close: {result[2]} days")
print(f"Slowest close: {result[3]} days")


# Query 3: Most Discussed Issues
print("\n\n3. TOP 10 MOST DISCUSSED ISSUES")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        i.number,
        i.state,
        i.title,
        i.comments_total_count,
        i.reactions_total_count,
        i.user_login as author,
        i.created_at::DATE as created_date
    FROM {schema}.issue i
    ORDER BY (i.comments_total_count + i.reactions_total_count) DESC
    LIMIT 10
""").fetchall()

for row in result:
    title = row[2][:50] + '...' if len(row[2]) > 50 else row[2]
    print(f"#{row[0]:4} [{row[1]:6}] | {title:53} | Comments: {row[3]:3} | Reactions: {row[4]:2}")


# Query 4: Activity Timeline
print("\n\n4. ACTIVITY BY MONTH")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        DATE_TRUNC('month', created_at) as month,
        COUNT(*) as issues_created
    FROM {schema}.issue
    GROUP BY DATE_TRUNC('month', created_at)
    ORDER BY month DESC
    LIMIT 12
""").fetchall()

for row in result:
    print(f"{row[0].strftime('%Y-%m'):10} | {'█' * (row[1] // 5)}{row[1]:3} issues")


# Query 5: Collaborator Permissions
print("\n\n5. COLLABORATOR PERMISSIONS BREAKDOWN")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        c.permission,
        COUNT(*) as count,
        GROUP_CONCAT(u.name || ' (' || u.login || ')', ', ') as users
    FROM {schema}.collaborator c
    JOIN {schema}.user u ON c.user_login = u.login
    WHERE u.name IS NOT NULL
    GROUP BY c.permission
    ORDER BY count DESC
""").fetchall()

for row in result:
    users_list = row[2].split(', ')[:5]  # Show first 5
    more = f" and {len(row[2].split(', ')) - 5} more" if len(row[2].split(', ')) > 5 else ""
    print(f"{row[0]:10} ({row[1]:2} users): {', '.join(users_list)}{more}")


# Query 6: Reaction Sentiment Analysis
print("\n\n6. REACTION SENTIMENT ANALYSIS")
print("-" * 80)
result = conn.execute(f"""
    WITH all_reactions AS (
        SELECT reaction_type FROM {schema}.issue_reaction
        UNION ALL
        SELECT reaction_type FROM {schema}.comment_reaction
    )
    SELECT
        reaction_type,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
    FROM all_reactions
    GROUP BY reaction_type
    ORDER BY count DESC
""").fetchall()

print("Reaction Type  | Count | Percentage | Bar Chart")
print("-" * 80)
for row in result:
    bar = '█' * int(row[2] // 5)
    print(f"{row[0]:14} | {row[1]:5} | {row[2]:5.1f}%     | {bar}")


# Query 7: Comment Thread Analysis
print("\n\n7. ISSUES WITH LONGEST COMMENT THREADS")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        i.number,
        i.title,
        i.user_login as author,
        COUNT(c.id) as comment_count,
        COUNT(DISTINCT c.user_login) as unique_commenters,
        i.created_at::DATE as created
    FROM {schema}.issue i
    LEFT JOIN {schema}.issue_comment c ON i.id = c.issue_id
    GROUP BY i.number, i.title, i.user_login, i.created_at
    HAVING COUNT(c.id) > 0
    ORDER BY comment_count DESC
    LIMIT 10
""").fetchall()

for row in result:
    title = row[1][:45] + '...' if len(row[1]) > 45 else row[1]
    print(f"#{row[0]:4} | {title:48} | {row[3]:3} comments from {row[4]:2} users")


# Query 8: User Interaction Matrix
print("\n\n8. TOP USER INTERACTIONS (Who comments on whose issues?)")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        i.user_login as issue_author,
        c.user_login as commenter,
        COUNT(*) as interaction_count
    FROM {schema}.issue i
    JOIN {schema}.issue_comment c ON i.id = c.issue_id
    WHERE i.user_login != c.user_login  -- Exclude self-comments
    GROUP BY i.user_login, c.user_login
    ORDER BY interaction_count DESC
    LIMIT 10
""").fetchall()

for row in result:
    print(f"{row[1]:20} commented on {row[0]:20}'s issues {row[2]:3} times")


# Query 9: Open Issues Summary
print("\n\n9. OPEN ISSUES SUMMARY")
print("-" * 80)
result = conn.execute(f"""
    SELECT
        COUNT(*) as total_open,
        AVG(comments_total_count) as avg_comments,
        AVG(reactions_total_count) as avg_reactions,
        AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at)) / 86400) as avg_age_days
    FROM {schema}.issue
    WHERE state = 'OPEN'
""").fetchone()

print(f"Total open issues: {result[0]}")
print(f"Average comments per issue: {result[1]:.1f}")
print(f"Average reactions per issue: {result[2]:.1f}")
print(f"Average age of open issues: {result[3]:.0f} days")


# Query 10: Busiest Day of Week
print("\n\n10. ACTIVITY BY DAY OF WEEK")
print("-" * 80)
result = conn.execute(f"""
    WITH all_activity AS (
        SELECT created_at FROM {schema}.issue
        UNION ALL
        SELECT created_at FROM {schema}.issue_comment
    )
    SELECT
        CASE EXTRACT(DOW FROM created_at)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END as day_of_week,
        COUNT(*) as activity_count
    FROM all_activity
    GROUP BY EXTRACT(DOW FROM created_at)
    ORDER BY EXTRACT(DOW FROM created_at)
""").fetchall()

max_activity = max(row[1] for row in result)
for row in result:
    bar = '█' * int((row[1] / max_activity) * 40)
    print(f"{row[0]:10} | {bar} {row[1]:4}")


conn.close()

print("\n" + "=" * 80)
print("Done! All queries executed successfully.")
print("=" * 80)
