# Workspace Zero
This workspace shows that an OSS workspace produced by `dlt init` can be deployed without any changes and will work just fine on dltHub.
1. `.dlt/.workspace` enables the extended `dlthub` workspace commands
2. There's just one set of secrets/config pointing to motherduck (or any other "production" destination)
3. `requirements.txt` should contain just `duckdb` dependency.
4. `uv` is not required. works with any package manager or build system.
