"""LanceDB Pipeline - Load data into LanceDB with vector embeddings for semantic search"""
import argparse
import csv
from pathlib import Path

import dlt
from dlt.destinations.adapters import lancedb_adapter
from openai import OpenAI
from tabulate import tabulate


# Global pipeline configuration
DATASET_NAME = "octolens_mention"
pipeline = dlt.pipeline(
    pipeline_name="octolens_mention",
    destination="lance",
    dataset_name=DATASET_NAME,
)


def embed_query(query: str) -> list[float]:
    """Embed a query text using OpenAI embeddings configured in dlt.

    Args:
        query: The text query to embed

    Returns:
        A list of floats representing the embedding vector
    """
    # Get OpenAI credentials and model config from dlt
    # Use "lance" to match the destination name in the pipeline configuration
    api_key = dlt.secrets[
        "destination.lance.credentials.embedding_model_provider_api_key"
    ]
    embedding_model = dlt.config["destination.lance.embedding_model"]

    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)

    # Generate embedding
    response = client.embeddings.create(
        input=query,
        model=embedding_model,
    )

    return response.data[0].embedding


def vector_search_descriptions(
    pipeline,
    dataset_name: str,
    table_name: str,
    query: str,
    limit: int = 3,
) -> None:
    """Perform vector search on mention bodies."""
    print("\n" + "=" * 50)  # noqa: T201
    print(f"Vector search for: '{query}'")  # noqa: T201
    print("=" * 50)  # noqa: T201

    with pipeline.destination_client() as client:
        db = client.db_client
        full_table_name = f"{dataset_name}___{table_name}"
        mentions_table = db.open_table(full_table_name)

        # Embed the query text using OpenAI
        query_vector = embed_query(query)

        # Perform vector search using the embedded query vector
        results = (
            mentions_table.search(
                query_vector,
                vector_column_name="vector",
            )
            .limit(limit)
            .to_arrow()
        )

        columns_to_show = [
            "url",
            "title",
            "body",
            "source",
            "timestamp",
            "_distance",
        ]
        # Filter to only show columns that exist in results
        existing_columns = [
            col for col in columns_to_show if col in results.column_names
        ]
        selected_table = results.select(existing_columns)
        data = selected_table.to_pylist()

        print(f"\nTop {limit} results:")  # noqa: T201
        print(
            tabulate(
                data,
                headers="keys",
                tablefmt="github",
            )
        )  # noqa: T201


def print_loaded_data(
    pipeline,
    dataset_name: str,
    table_name: str,
) -> None:
    print("\n" + "=" * 50)  # noqa: T201
    print("Accessing loaded data from LanceDB:")  # noqa: T201
    print("=" * 50)  # noqa: T201

    with pipeline.destination_client() as client:
        db = client.db_client
        full_table_name = f"{dataset_name}___{table_name}"

        try:
            mentions_table = db.open_table(full_table_name)
            row_count = mentions_table.count_rows()
            print(f"\n{full_table_name} table has {row_count} rows")  # noqa: T201
            print("\nShowing first 10 mentions:")  # noqa: T201

            try:
                # Always limit to first 10 rows for display
                arrow_table = mentions_table.head(10)

            except (NotImplementedError, AttributeError):
                arrow_table = mentions_table.search().limit(10).to_arrow()

            columns_to_show = ["url", "title", "body", "source", "timestamp"]
            # Filter to only show columns that exist in results
            existing_columns = [
                col for col in columns_to_show if col in arrow_table.column_names
            ]
            selected_table = arrow_table.select(existing_columns)
            data = selected_table.to_pylist()
            print(
                tabulate(
                    data,
                    headers="keys",
                    tablefmt="github",
                )
            )  # noqa: T201

        except Exception as e:
            print(f"\nError opening table '{full_table_name}': {e}")  # noqa: T201
            print(
                "The table may not exist yet. Try running without --view to load data first."
            )  # noqa: T201


@dlt.resource(primary_key="URL")
def octolens_mentions():
    """Load Octolens mentions from CSV file."""
    csv_path = Path(__file__).parent / "sample-octolens-mention.csv"

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows with empty Body field
            if not row.get("Body", "").strip():
                continue

            # Truncate Body to 32000 characters to avoid token limits
            # (32000 chars ≈ 8000 tokens, matching standard embedding model limits)
            # Since dlt processes items one at a time, we can use the full model limit
            if len(row["Body"]) > 32000:
                row["Body"] = row["Body"][:32000] + "..."

            yield row


def load_to_lancedb() -> None:
    load_info = pipeline.run(
        lancedb_adapter(
            octolens_mentions(),
            embed=["body"],
        ),
        table_name="mentions",
        destination="lance",
    )
    print(load_info)  # noqa: T201

    print_loaded_data(
        pipeline,
        dataset_name=DATASET_NAME,
        table_name="mentions",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LanceDB Pipeline - Load or view data")
    parser.add_argument(
        "--view",
        action="store_true",
        help="View the loaded data without running the pipeline",
    )
    parser.add_argument(
        "--search",
        type=str,
        help="Perform vector search on mention bodies (e.g., --search 'data loading')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="Number of search results to return (default: 3)",
    )
    args = parser.parse_args()

    match (args.search, args.view):
        case (str(), _):
            # Perform vector search
            vector_search_descriptions(
                pipeline,
                dataset_name=DATASET_NAME,
                table_name="mentions",
                query=args.search,
                limit=args.limit,
            )

        case (None, True):
            # Just view the data without loading
            print_loaded_data(
                pipeline,
                dataset_name=DATASET_NAME,
                table_name="mentions",
            )

        case _:
            load_to_lancedb()
