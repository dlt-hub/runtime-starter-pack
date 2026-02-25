"""HuggingFace Pipeline - Load OpenVid-Lance dataset into LanceDB for vector and full-text search"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import argparse

import lancedb
from tabulate import tabulate


# HuggingFace LanceDB URI defined in .agent/skills/openvid-lance/SKILL.md
HF_DATASET_URI = "hf://datasets/lance-format/openvid-lance/data"
TABLE_NAME = "train"

# Columns to show in tabular output (skip heavy binary/embedding columns)
DISPLAY_COLUMNS = [
    "video_path",
    "caption",
    "aesthetic_score",
    "motion_score",
    "temporal_consistency_score",
    "camera_motion",
    "frame",
    "fps",
    "seconds",
]


def connect_table() -> lancedb.table.Table:
    """Connect to the OpenVid-Lance LanceDB table on HuggingFace.

    Returns:
        An open LanceDB table handle for the train split.
    """
    db = lancedb.connect(HF_DATASET_URI)
    return db.open_table(TABLE_NAME)


def _truncate_captions(rows: list[dict], max_len: int = 80) -> list[dict]:
    """Truncate long captions in a list of row dicts for display."""
    for row in rows:
        caption = row.get("caption")
        if caption and len(caption) > max_len:
            row["caption"] = caption[:max_len] + "..."
    return rows


def print_table_info(tbl: lancedb.table.Table) -> None:
    """Print schema and row count for the opened table."""
    print("\n" + "=" * 50)  # noqa: T201
    print("OpenVid-Lance dataset info:")  # noqa: T201
    print("=" * 50)  # noqa: T201
    print(f"Total videos : {len(tbl)}")  # noqa: T201
    print(f"Schema       :\n{tbl.schema}")  # noqa: T201


def print_sample_rows(tbl: lancedb.table.Table, limit: int = 5) -> None:
    """Print a sample of rows from the dataset (metadata only, no blobs).

    Args:
        tbl: Open LanceDB table handle.
        limit: Number of rows to display.
    """
    print("\n" + "=" * 50)  # noqa: T201
    print(f"Showing first {limit} rows (metadata only):")  # noqa: T201
    print("=" * 50)  # noqa: T201

    # head() returns an Arrow table; select columns before converting
    rows = tbl.head(limit).select(DISPLAY_COLUMNS).to_pylist()
    print(tabulate(_truncate_captions(rows), headers="keys", tablefmt="github"))  # noqa: T201


def vector_search(tbl: lancedb.table.Table, limit: int = 5) -> None:
    """Perform vector similarity search using the first row's embedding as the reference query.

    Args:
        tbl: Open LanceDB table handle.
        limit: Number of similar videos to return.
    """
    print("\n" + "=" * 50)  # noqa: T201
    print("Vector similarity search (reference: first video embedding):")  # noqa: T201
    print("=" * 50)  # noqa: T201

    ref_row = tbl.head(1).select(["embedding", "caption"]).to_pylist()[0]
    query_embedding = ref_row["embedding"]

    results = (
        tbl.search(query_embedding, vector_column_name="embedding")
        .metric("L2")
        .nprobes(1)
        .limit(limit)
        .select(["video_path", "caption", "aesthetic_score", "_distance"])
        .to_list()
    )
    query_embedding = ref_row["embedding"]

    results = (
        tbl.search(query_embedding, vector_column_name="embedding")
        .metric("L2")
        .nprobes(1)
        .limit(limit)
        .select(["video_path", "caption", "aesthetic_score", "_distance"])
        .to_list()
    )

    print(tabulate(_truncate_captions(results), headers="keys", tablefmt="github"))  # noqa: T201


def full_text_search(tbl: lancedb.table.Table, query: str, limit: int = 5) -> None:
    """Perform full-text search on video captions.

    Args:
        tbl: Open LanceDB table handle.
        query: Text query to search for in captions.
        limit: Number of results to return.
    """
    print("\n" + "=" * 50)  # noqa: T201
    print(f"Full-text search for: '{query}'")  # noqa: T201
    print("=" * 50)  # noqa: T201

    results = (
        tbl.search(query)
        .select(["video_path", "caption", "aesthetic_score"])
        .limit(limit)
        .to_list()
    )

    print(tabulate(_truncate_captions(results), headers="keys", tablefmt="github"))  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="HuggingFace Pipeline - Browse and search the OpenVid-Lance dataset via "
        f"LanceDB URI: {HF_DATASET_URI}"
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show dataset schema and row count",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Show a sample of metadata rows from the dataset",
    )
    parser.add_argument(
        "--vector-search",
        action="store_true",
        dest="vector_search",
        help="Run vector similarity search using the first video as the reference query",
    )
    parser.add_argument(
        "--search",
        type=str,
        help="Perform full-text search on captions (e.g., --search 'sunset beach')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows / results to return (default: 5)",
    )
    args = parser.parse_args()

    tbl = connect_table()

    match (args.search, args.vector_search, args.sample, args.info):
        case (str(), _, _, _):
            full_text_search(tbl, args.search, limit=args.limit)

        case (None, True, _, _):
            vector_search(tbl, limit=args.limit)

        case (None, False, True, _):
            print_sample_rows(tbl, limit=args.limit)

        case (None, False, False, True):
            print_table_info(tbl)

        case _:
            # Default: show info then sample rows
            print_table_info(tbl)
            print_sample_rows(tbl, limit=args.limit)
