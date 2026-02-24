"""OpenVid Dataset Pipeline - Load OpenVid video metadata from HuggingFace into LanceDB using DuckDB"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import argparse

import dlt
import duckdb
from dlt.destinations.adapters import lancedb_adapter
from tabulate import tabulate


# HuggingFace parquet URL for the OpenVid dataset (auto-converted parquet files)
HF_PARQUET_URL = "hf://datasets/lance-format/openvid-lance@~parquet/**/*.parquet"

# Columns to exclude from queries (heavy binary/embedding data)
EXCLUDED_COLUMNS = {"video_blob", "embedding"}

BATCH_SIZE = 1000

# Global pipeline configuration
DATASET_NAME = "openvid"
pipeline = dlt.pipeline(
    pipeline_name="openvid",
    destination="lance",
    dataset_name=DATASET_NAME,
)


def get_metadata_columns(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Discover all columns from the dataset and return those not in the exclusion list."""
    schema = conn.execute(
        f"DESCRIBE SELECT * FROM '{HF_PARQUET_URL}' LIMIT 0"
    ).fetchall()
    all_columns = [row[0] for row in schema]

    print(f"All available columns: {all_columns}")  # noqa: T201
    print(f"Excluded columns: {EXCLUDED_COLUMNS}")  # noqa: T201

    metadata_columns = [col for col in all_columns if col not in EXCLUDED_COLUMNS]
    print(f"Selected columns: {metadata_columns}")  # noqa: T201

    return metadata_columns


@dlt.resource(write_disposition="replace")
def openvid_videos(limit: int = 100):
    """Load OpenVid video metadata from HuggingFace using DuckDB's hf:// adapter.

    Queries the auto-converted parquet files on HuggingFace Hub,
    selecting metadata columns only (skips heavy columns via EXCLUDED_COLUMNS).
    """
    conn = duckdb.connect()

    metadata_columns = get_metadata_columns(conn)
    columns_sql = ", ".join(metadata_columns)
    query = f"""
        SELECT {columns_sql}
        FROM '{HF_PARQUET_URL}'
        LIMIT {limit}
    """

    result = conn.execute(query)
    columns = [desc[0] for desc in result.description]

    while True:
        rows = result.fetchmany(BATCH_SIZE)
        if not rows:
            break

        yield [dict(zip(columns, row)) for row in rows]

    conn.close()


def print_loaded_data() -> None:
    """Display loaded data from LanceDB."""
    print("\n" + "=" * 50)  # noqa: T201
    print("Loaded data from LanceDB:")  # noqa: T201
    print("=" * 50)  # noqa: T201

    with pipeline.destination_client() as client:
        db = client.db_client
        full_table_name = DATASET_NAME

        try:
            table = db.open_table(full_table_name)
            row_count = table.count_rows()
            print(f"\n{full_table_name} has {row_count} rows")  # noqa: T201
            print("\nFirst 10 rows:")  # noqa: T201

            try:
                arrow_table = table.head(10)

            except (NotImplementedError, AttributeError):
                arrow_table = table.search().limit(10).to_arrow()

            existing_columns = [
                col for col in arrow_table.column_names if col not in EXCLUDED_COLUMNS
            ]
            selected_table = arrow_table.select(existing_columns)
            data = selected_table.to_pylist()
            print(  # noqa: T201
                tabulate(data, headers="keys", tablefmt="github")
            )

        except Exception as e:
            print(f"\nError opening table '{full_table_name}': {e}")  # noqa: T201
            print(  # noqa: T201
                "Try running without --view to load data first."
            )


def load_openvid(limit: int = 100) -> None:
    """Load OpenVid data into LanceDB with embeddings on captions."""
    load_info = pipeline.run(
        lancedb_adapter(
            openvid_videos(limit=limit),
            embed=["caption"],
        ),
        table_name="videos",
    )
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OpenVid Pipeline - Load video metadata from HuggingFace"
    )
    parser.add_argument(
        "--view",
        action="store_true",
        help="View loaded data without running the pipeline",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of rows to load (default: 100)",
    )
    args = parser.parse_args()

    match args.view:
        case True:
            print_loaded_data()

        case _:
            load_openvid(limit=args.limit)
