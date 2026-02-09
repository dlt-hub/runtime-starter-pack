# GitHub to DuckDB Pipeline - Quick Start

## What Was Created

A complete data pipeline to load GitHub issues and contributors data into DuckDB using dlt.

### New Files:
- **`load_github_to_duckdb.py`** - Simple script to load issues + contributors (START HERE!)
- **`github_example.py`** - Comprehensive example with data queries
- **`setup_github_pipeline.py`** - Setup verification script
- **`GITHUB_PIPELINE_README.md`** - Full documentation

### Modified Files:
- **`github/queries.py`** - Added contributors GraphQL query
- **`github/helpers.py`** - Added `get_contributors()` function
- **`github/__init__.py`** - Added `github_contributors` source
- **`github_pipeline.py`** - Added example pipeline functions

## Quick Start (3 Steps)

### 1. Configure GitHub Token

Edit `.dlt/secrets.toml` and replace `<configure me>` with your GitHub token:

```toml
[sources.github]
access_token = "ghp_your_actual_github_token_here"
```

**Get a token here:** https://github.com/settings/tokens (select `repo` and `read:org` scopes)

### 2. Verify Setup

```bash
python setup_github_pipeline.py
```

This will check your configuration and run a test load.

### 3. Run the Pipeline

```bash
python load_github_to_duckdb.py
```

That's it! Your data is now in DuckDB.

## What Gets Loaded

### Issues Table
- Issue number, title, body
- Author information
- State (open/closed)
- Reactions and comments
- Timestamps

### Contributors Table
- Username and full name
- Permission level (READ, WRITE, ADMIN)
- Profile info (bio, company, location)
- Avatar URL
- Account creation date

## Query Your Data

```bash
# Open DuckDB CLI
duckdb .dlt/github_data/github_data.duckdb

# View tables
SHOW TABLES;

# Query issues
SELECT number, title, state, author__login FROM issues LIMIT 10;

# Query contributors
SELECT user__login, permission, user__company FROM contributors;
```

## Customize

Edit `load_github_to_duckdb.py`:

```python
OWNER = "your-username"  # Your repository owner
REPO = "your-repo"       # Your repository name
MAX_ISSUES = 500         # How many issues to load
```

## Next Steps

- **Load more data**: Edit the configuration variables
- **Load pull requests**: Use `github_reactions().with_resources("pull_requests")`
- **Incremental loads**: Use `github_repo_events()` for events
- **Different destination**: Change `destination="duckdb"` to `"motherduck"`, `"postgres"`, etc.

## Need Help?

- See `GITHUB_PIPELINE_README.md` for detailed documentation
- Check `github_example.py` for advanced examples
- Visit [dlt documentation](https://dlthub.com/docs/)
