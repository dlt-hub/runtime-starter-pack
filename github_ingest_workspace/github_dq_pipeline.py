"""GitHub data quality checks.

Batch job that validates commits and contributors data after ingestion.
Runs on an hourly schedule. The job fails if any check has failures,
succeeds otherwise.
"""

import dlt
from dlt.hub import run
from dlt.hub.run import trigger

import dlthub.data_quality as dq


commits_checks = [
    dq.checks.is_not_null("sha"),
    dq.checks.is_not_null("commit__author__name"),
    dq.checks.is_not_null("commit__author__date"),
    dq.checks.case("commit__comment_count >= 0"),
]

contributors_checks = [
    dq.checks.is_not_null("login"),
    dq.checks.is_not_null("id"),
    dq.checks.is_in("type", ["User", "Bot"]),
    dq.checks.case("contributions > 0"),
]


@run.job(
    trigger=trigger.schedule("0 * * * *"),
    expose={"display_name": "GitHub data quality"},
)
def run_dq_checks():
    """Run data quality checks on GitHub data. Fails if any check has failures."""
    github_pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )

    dataset = github_pipeline.dataset()
    suite = dq.CheckSuite(
        dataset,
        checks={
            "commits": commits_checks,
            "contributors": contributors_checks,
        },
    )

    all_passed = True
    for table_name, check_list in [("commits", commits_checks), ("contributors", contributors_checks)]:
        for check in check_list:
            check_name = check.name
            failures = suite.get_failures(table_name, check_name).arrow()
            if len(failures) > 0:
                print(f"FAIL: {table_name}.{check_name} -- {len(failures)} failures")
                all_passed = False
            else:
                print(f"PASS: {table_name}.{check_name}")

    if not all_passed:
        raise RuntimeError("Data quality checks failed -- see output above for details")
    print("All data quality checks passed")


if __name__ == "__main__":
    run_dq_checks()
