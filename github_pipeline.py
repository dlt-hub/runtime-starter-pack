"""
Load GitHub issues and contributors data into DuckDB.
"""

import dlt
from github import github_contributors, github_reactions

# Configuration
OWNER = "dlt-hub"
REPO = "dlt"
MAX_ISSUES = 1000

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="github_to_duckdb",
    destination="duckdb",
    dataset_name="github_data",
    dev_mode=True,
)

# Load issues
print(f"Loading issues from {OWNER}/{REPO}...")
issues = github_reactions(
    owner=OWNER,
    name=REPO,
    items_per_page=100,
    max_items=MAX_ISSUES
).with_resources("issues")
pipeline.run(issues)

# Load contributors
print(f"Loading contributors from {OWNER}/{REPO}...")
contributors = github_contributors(owner=OWNER, name=REPO)
pipeline.run(contributors)

print(f"Done. Data saved to: {pipeline.working_dir}/{pipeline.dataset_name}.duckdb")
