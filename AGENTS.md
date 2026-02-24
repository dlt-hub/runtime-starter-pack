# Agent Instructions for Runtime Starter Pack (LanceDB)

This is a runtime starter pack for working with LanceDB and dlt (data load tool).

## CRITICAL: Package Management

**ALWAYS use `uv` instead of `pip` for ALL package management operations.**

- ✅ Use: `uv pip install <package>`
- ❌ Never use: `pip install <package>`
- ✅ Use: `uv pip install -r requirements.txt`
- ❌ Never use: `pip install -r requirements.txt`
- ✅ Use: `uv pip uninstall <package>`
- ❌ Never use: `pip uninstall <package>`

This is a strict requirement for this project.

## Project Context

This project demonstrates how to use dlt runtime with LanceDB as a vector database destination.

## Key Files

- `lancedb_pipeline.py` - Main pipeline file for LanceDB integration
- `.dlt/config.toml` - DLT configuration for development
- `.dlt/prod.config.toml` - DLT configuration for production
- `README.md` - Project documentation

## Development Guidelines

### When Working with dlt Pipelines

- Always check configuration files in `.dlt/` before running pipelines
- Use the virtual environment at `.venv/` for all Python operations
- The project uses Python 3.13

### When Making Changes

- Follow existing code patterns in the pipeline files
- Test configuration changes locally before committing
- Keep production and development configs synchronized where appropriate

### Python Coding Style

**CRITICAL: Use `match/case` for conditional flows with multiple branches.**

- ✅ Use `match/case` when you have `elif` or `else` statements
- ✅ Use `match/case` for any conditional with more than just a simple `if` statement
- ❌ Never use `if/elif/else` chains - always prefer `match/case`

**Formatting Requirements:**
- Always add a blank line after each case statement's logic (before the next case)
- This improves readability and visual separation between cases

**Example - Bad:**
```python
if status == "pending":
    handle_pending()
elif status == "complete":
    handle_complete()
else:
    handle_error()
```

**Example - Good:**
```python
match status:
    case "pending":
        handle_pending()

    case "complete":
        handle_complete()

    case _:
        handle_error()
```

This is required for Python 3.10+ and improves code readability and maintainability.

### Git Workflow

- Main branch: `main`
- Always check git status before committing
- Follow conventional commit message style based on repository history

### Test Scripts and Temporary Files

**IMPORTANT: Store all test scripts, experiments, and temporary files in the `workspace/` directory.**

- ✅ Use: `workspace/test_*.py` for test scripts
- ✅ Use: `workspace/experiment_*.py` for experimental code
- ✅ Use: `workspace/scratch_*.py` for scratch work
- ❌ Never create test files in the project root

The `workspace/` directory is in `.gitignore` and will not be committed to the repository. This keeps the main codebase clean and prevents experimental code from being accidentally committed.

## Common Tasks

### Running Pipelines

```bash
python lancedb_pipeline.py
```

### Installing Dependencies

```bash
uv pip install -r requirements.txt
```

Always use `uv` for package management (see CRITICAL section above).

## Testing

- Data quality examples are included in the starter pack
- Test both development and production configurations

## Notes

- This is a starter pack template - customize as needed for specific use cases
- LanceDB is used as the vector database destination
- dlt-runtime is the core dependency for data loading
