"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


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
            }
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


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )

    load_info = pipeline.run(github_rest_api_source())
    print(load_info)  # noqa: T201
