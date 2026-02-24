---
name: dlt-credentials
description: Explains how dlt (data load tool) credentials and configuration work, including setup, resolution order, TOML files, environment variables, credential types, and advanced usage. Use when the user asks about dlt credentials, config, secrets, or how to configure a dlt pipeline.
---

Give the user a clear, practical overview of how dlt credentials and configuration work. Use the reference material below to answer accurately. Tailor your response to the user's specific question if they provided one via `$ARGUMENTS`, otherwise give a general overview.

If the user asked a specific question: **$ARGUMENTS**

---

## Reference: dlt Credentials & Configuration

### Resolution Order

dlt resolves configuration values by querying these providers in order (highest priority first):

1. **Environment variables**
2. **`secrets.toml` and `config.toml`** (in the `.dlt/` folder)
3. **Vaults** (Google Secrets Manager, Azure Key Vault, AWS Secrets Manager)
4. **Custom providers** (registered via `dlt.config.register_provider()`)
5. **Default argument values** in function signatures (lowest priority)

A value found in a higher-priority provider wins. Explicit arguments passed directly to functions always bypass injection entirely.

### secrets.toml vs config.toml

Both files live in the `.dlt/` directory relative to your working directory.

| File | Purpose | Version control |
|------|---------|-----------------|
| `config.toml` | Non-sensitive settings: file paths, hosts, URLs, performance tuning | Safe to commit |
| `secrets.toml` | Sensitive data: passwords, API keys, private keys, tokens | **Never commit** (add to `.gitignore`) |

Example `secrets.toml`:
```toml
[sources.my_source]
api_key = "sk-..."

[destination.postgres.credentials]
database = "mydb"
password = "secret"
username = "loader"
host = "localhost"
```

Example `config.toml`:
```toml
[sources.my_source]
page_size = 100
base_url = "https://api.example.com"

[runtime]
log_level = "WARNING"
```

### Environment Variables

Environment variable names follow this convention: **uppercase, double underscores (`__`) as section separators**.

```bash
# Pattern: SECTION__SUBSECTION__KEY
export SOURCES__MY_SOURCE__API_KEY="sk-..."
export DESTINATION__POSTGRES__CREDENTIALS__PASSWORD="secret"
```

For complex values (lists/dicts), use Python literal syntax (not JSON):
```bash
export DESTINATION__DUCKDB__CREDENTIALS__PRAGMAS="[\"enable_logging\"]"
```

### Key Resolution Path (Lookup Fallback)

When dlt searches for a config value, it progressively eliminates rightmost path sections. For a function `notion_databases` in module `notion.py` looking for `api_key`:

1. `sources.notion.notion_databases.api_key`
2. `sources.notion.api_key`
3. `sources.api_key`
4. `api_key`

This means you can place config at the most specific or most general level depending on your needs. **Exception:** the `credentials` section in destinations is mandatory and won't be eliminated.

### Injection Rules for Decorated Functions

Functions decorated with `@dlt.source`, `@dlt.resource`, or `@dlt.destination` get automatic config injection:

- **Explicit arguments** passed by the caller are never overridden
- **Required arguments** (no defaults) must be passed explicitly — they cannot be injected
- **Optional arguments** with defaults: dlt searches config providers first, falls back to default
- **`dlt.secrets.value` marker**: argument *must* be provided via injection or explicitly — no default fallback
- **`dlt.config.value` marker**: same as above but for non-sensitive config

```python
@dlt.source
def my_source(
    api_key: str = dlt.secrets.value,  # MUST be in secrets.toml or env
    page_size: int = dlt.config.value, # MUST be in config.toml or env
    timeout: int = 30,                 # Optional, injected if found, else 30
):
    ...
```

### Programmatic Access

```python
import dlt

# Read values
api_key = dlt.secrets["sources.my_source.api_key"]
page_size = dlt.config["sources.my_source.page_size"]

# Read with type coercion
from dlt.sources.credentials import GcpServiceAccountCredentials
creds = dlt.secrets.get("my_section.gcp_credentials", GcpServiceAccountCredentials)

# Write values at runtime
dlt.config["sheet_id"] = "23029402349032049"
dlt.secrets["destination.postgres.credentials.password"] = "new_password"
```

### Built-in Credential Types

dlt provides typed credential classes for common services:

| Type | Use case |
|------|----------|
| `ConnectionStringCredentials` | Any database via connection string |
| `GcpServiceAccountCredentials` | Google Cloud service accounts |
| `GcpOAuthCredentials` | Google Cloud OAuth |
| `AwsCredentials` | AWS (access key, secret, region) |
| `AzureCredentials` | Azure (storage account, key) |
| `AzureServicePrincipalCredentials` | Azure service principal auth |

Example with GCP:
```python
from dlt.sources.credentials import GcpServiceAccountCredentials

creds = GcpServiceAccountCredentials()
creds.parse_native_representation(service_account_dict)

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=bigquery(credentials=creds),
)
```

### Connection Strings

Many credentials accept connection strings as a single value:

```toml
# In secrets.toml
[destination.postgres.credentials]
credentials = "postgresql://user:password@host:5432/dbname"
```

Or via environment variable:
```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://user:password@host:5432/dbname"
```

### Custom Configuration Specs

For advanced use, create custom config specs using dataclasses:

```python
from dataclasses import dataclass
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration

@configspec
class MyCustomConfig(BaseConfiguration):
    api_url: str = None
    retry_count: int = 3
    timeout: float = 30.0
```

### Error Handling

When configuration is missing, dlt raises `ConfigFieldMissingException` with:
- The missing field name
- All attempted lookup keys/locations in priority order
- Whether pipeline-prefixed paths were checked

This makes debugging configuration issues straightforward — read the error message carefully.

### Quick Setup Checklist

1. Create `.dlt/config.toml` for non-sensitive settings
2. Create `.dlt/secrets.toml` for sensitive credentials
3. Add `.dlt/secrets.toml` to `.gitignore`
4. Use the appropriate section structure (`[sources.x]`, `[destination.x.credentials]`)
5. For production, prefer environment variables or vault providers over TOML files

### References

- [Credentials Setup](https://dlthub.com/docs/general-usage/credentials/setup)
- [Advanced Credentials](https://dlthub.com/docs/general-usage/credentials/advanced)
