"""GitHub commits ingestion pipeline.

Loads commits and contributors from the GitHub REST API into a warehouse
destination. Uses a job decorator with a recurring trigger so Runtime
runs it automatically every 5 minutes.
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.hub import run
from dlt.hub.run import trigger


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
def github_rest_api_source(
    owner: str = "dlt-hub",
    repo: str = "dlt",
    access_token: str = dlt.secrets.value,
):
    """Define dlt resources from GitHub REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {"type": "bearer", "token": access_token},
            "headers": {
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "dlt-github-rest",
            },
            "paginator": {"type": "header_link"},
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "per_page": 100,
                }
            },
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "commits",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/commits",
                    "params": {"owner": owner, "repo": repo},
                },
            },
            {
                "name": "contributors",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/contributors",
                    "params": {"owner": owner, "repo": repo},
                },
            },
        ],
    }

    yield from rest_api_resources(config)


@run.pipeline(
    "github_pipeline",
    trigger=trigger.every("5m"),
    expose={"tags": ["ingest"], "display_name": "GitHub commits ingest"},
)
def load_commits():
    """Load commits and contributors from the GitHub REST API."""
    github_pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
        progress="log",
    )
    load_info = github_pipeline.run(github_rest_api_source().add_limit(10))
    print(load_info)


if __name__ == "__main__":
    load_commits()
