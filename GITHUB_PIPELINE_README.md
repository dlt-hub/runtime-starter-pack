# GitHub Data Pipeline

A complete data pipeline to ingest GitHub issues and contributors data into DuckDB using [dlt (data load tool)](https://dlthub.com/).

## Features

- ✅ **Issues Data**: Load issues with comments, reactions, and author information
- ✅ **Contributors Data**: Load repository contributors/collaborators with permissions and profiles
- ✅ **Pull Requests**: Load pull requests with reactions and comments
- ✅ **Repository Events**: Load repository events incrementally
- ✅ **Stargazers**: Load users who starred the repository
- ✅ **GraphQL Optimized**: Uses GitHub's GraphQL API for efficient data retrieval
- ✅ **Incremental Loading**: Support for incremental data loads (events)
- ✅ **DuckDB Storage**: Fast local analytics database

## Project Structure

```
runtime-starter-pack/
├── github/
│   ├── __init__.py         # Source definitions (github_reactions, github_contributors, etc.)
│   ├── queries.py          # GraphQL queries for GitHub API
│   ├── helpers.py          # Helper functions for API calls
│   └── settings.py         # Configuration constants
├── github_pipeline.py      # Example pipeline implementations
├── github_example.py       # Comprehensive usage example
└── .dlt/
    ├── config.toml         # dlt configuration
    └── secrets.toml        # GitHub access token (create this)
```

## Setup

### 1. Install Dependencies

Dependencies are already configured in `pyproject.toml`:
```bash
uv sync  # or pip install -e .
```

### 2. Create GitHub Access Token

1. Go to [GitHub Settings > Developer Settings > Personal Access Tokens](https://github.com/settings/tokens)
2. Click "Generate new token (classic)"
3. Select the following scopes:
   - `repo` (Full control of private repositories)
   - `read:org` (Read org and team membership)
4. Generate and copy the token

### 3. Configure dlt Secrets

Create or edit `.dlt/secrets.toml`:

```toml
[sources.github]
access_token = "ghp_your_github_personal_access_token_here"
```

**Important**: Never commit `secrets.toml` to version control!

## Usage

### Quick Start

Run the example script:

```bash
python github_example.py
```

This will:
1. Load 50 issues from the `dlt-hub/dlt` repository
2. Load all contributors from the repository
3. Display summary statistics and sample data

### Custom Repository

Edit `github_example.py` to load your own repository:

```python
pipeline = load_repository_data(
    owner="your-username",
    repo="your-repository",
    max_issues=100,
    dataset_name="my_github_data"
)
```

### Available Pipeline Functions

#### 1. Load Issues and Contributors (Recommended)

```python
import dlt
from github import github_reactions, github_contributors

pipeline = dlt.pipeline(
    "github_data",
    destination='duckdb',
    dataset_name="my_data"
)

# Load issues
issues_data = github_reactions("owner", "repo", max_items=100).with_resources("issues")
pipeline.run(issues_data)

# Load contributors
contributors_data = github_contributors("owner", "repo")
pipeline.run(contributors_data)
```

#### 2. Load Pull Requests

```python
pr_data = github_reactions("owner", "repo").with_resources("pull_requests")
pipeline.run(pr_data)
```

#### 3. Load Repository Events (Incremental)

```python
from github import github_repo_events

events_data = github_repo_events("owner", "repo")
pipeline.run(events_data)
```

#### 4. Load Stargazers

```python
from github import github_stargazers

stargazers_data = github_stargazers("owner", "repo")
pipeline.run(stargazers_data)
```

## Data Schema

### Issues Table

| Column | Type | Description |
|--------|------|-------------|
| `number` | INTEGER | Issue number |
| `title` | TEXT | Issue title |
| `body` | TEXT | Issue description |
| `state` | TEXT | Issue state (open/closed) |
| `author__login` | TEXT | Issue author username |
| `created_at` | TIMESTAMP | Creation timestamp |
| `closed_at` | TIMESTAMP | Closed timestamp |
| `reactions_totalCount` | INTEGER | Number of reactions |
| `reactions` | JSON | Array of reactions |
| `comments` | JSON | Array of comments |

### Contributors Table

| Column | Type | Description |
|--------|------|-------------|
| `user__login` | TEXT | GitHub username |
| `user__name` | TEXT | Full name |
| `user__email` | TEXT | Email address |
| `user__bio` | TEXT | User bio |
| `user__company` | TEXT | Company |
| `user__location` | TEXT | Location |
| `permission` | TEXT | Permission level (READ, WRITE, ADMIN) |
| `user__created_at` | TIMESTAMP | Account creation date |
| `user__avatar_url` | TEXT | Avatar URL |

## Querying Data

### Using DuckDB CLI

```bash
duckdb .dlt/github_data/github_data.duckdb

-- List all tables
SHOW TABLES;

-- Query issues
SELECT number, title, state, author__login
FROM issues
ORDER BY created_at DESC
LIMIT 10;

-- Query contributors
SELECT user__login, permission, user__company
FROM contributors;

-- Issues by author
SELECT
    author__login,
    COUNT(*) as issue_count,
    AVG(reactions_totalCount) as avg_reactions
FROM issues
GROUP BY author__login
ORDER BY issue_count DESC;
```

### Using Python

```python
import duckdb

conn = duckdb.connect('.dlt/github_data/github_data.duckdb')

# Get top contributors
result = conn.execute("""
    SELECT user__login, permission
    FROM contributors
    ORDER BY user__login
""").df()

print(result)
```

## Advanced Configuration

### Change Destination

Instead of DuckDB, you can load to other destinations:

```python
# Load to Motherduck (cloud DuckDB)
pipeline = dlt.pipeline(
    "github_data",
    destination='motherduck',
    dataset_name="github_data"
)

# Load to PostgreSQL
pipeline = dlt.pipeline(
    "github_data",
    destination='postgres',
    dataset_name="github_data"
)

# Load to BigQuery
pipeline = dlt.pipeline(
    "github_data",
    destination='bigquery',
    dataset_name="github_data"
)
```

See [dlt destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) for all options.

### Incremental Loading

The `github_repo_events` source supports incremental loading:

```python
# First run - loads all events
events_data = github_repo_events("apache", "airflow")
pipeline.run(events_data)

# Subsequent runs - only loads new events
events_data = github_repo_events("apache", "airflow")
pipeline.run(events_data)
```

## API Rate Limits

- **GraphQL API**: 5,000 points per hour
- **REST API**: 5,000 requests per hour (authenticated)

The pipeline automatically monitors rate limits and displays remaining credits.

## Troubleshooting

### Authentication Errors

```
Error: Bad credentials
```

**Solution**: Check that your GitHub token is correctly set in `.dlt/secrets.toml`

### Rate Limit Errors

```
Error: API rate limit exceeded
```

**Solution**: Wait for the rate limit to reset, or use a different token

### No Data Returned

**Solution**: Ensure the repository exists and your token has access to it

## Examples

See these files for complete examples:
- `github_example.py` - Comprehensive example with queries
- `github_pipeline.py` - Multiple pipeline configurations

## Resources

- [dlt Documentation](https://dlthub.com/docs/)
- [GitHub GraphQL API](https://docs.github.com/en/graphql)
- [DuckDB Documentation](https://duckdb.org/docs/)

## License

MIT License - see LICENSE file for details
