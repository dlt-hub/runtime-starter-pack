---
name: openvid-lance
description: Reference guide for the OpenVid-Lance dataset (lance-format/openvid-lance on HuggingFace) - a Lance-format video dataset with 937,957 videos, inline blobs, embeddings, and rich metadata. Use when the user asks about the OpenVid dataset, how to load/query/search it, Lance blob API, vector search, full-text search, dataset evolution, or LanceDB integration.
---

Help the user work with the OpenVid-Lance dataset. Use the reference material below to answer accurately. Tailor your response to the user's specific question if they provided one via `$ARGUMENTS`, otherwise give a general overview.

If the user asked a specific question: **$ARGUMENTS**

---

## Reference: OpenVid-Lance Dataset

### Overview

Lance format version of the [OpenVid dataset](https://huggingface.co/datasets/nkp37/OpenVid-1M) with **937,957 high-quality videos** stored with inline video blobs, embeddings, and rich metadata.

**HuggingFace:** `lance-format/openvid-lance`
**License:** CC-BY-4.0
**Paper:** arXiv:2407.02371

### Why Lance?

- **Blazing Fast Random Access**: Optimized for fetching scattered rows, ideal for random sampling, real-time ML serving, and interactive applications
- **Native Multimodal Support**: Store text, embeddings, and other data types together. Large binary objects loaded lazily, vectors optimized for fast similarity search
- **Efficient Data Evolution**: Add new columns and backfill data without rewriting the entire dataset
- **Versatile Querying**: Combines vector similarity search, full-text search, and SQL-style filtering in a single query

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `video_path` | Text | S3 path to video file |
| `caption` | Text | Detailed text description (212-2,790 chars) |
| `aesthetic_score` | Float64 | Quality metric (5.23-5.24) |
| `motion_score` | Float64 | Motion intensity (0.02-131) |
| `temporal_consistency_score` | Float64 | Temporal coherence (0.94-1.0) |
| `camera_motion` | Text | Type of camera movement (31 classes) |
| `frame` | Int64 | Number of frames (9-900) |
| `fps` | Float64 | Frames per second (23-60) |
| `seconds` | Float64 | Video duration (0.3-34.8 seconds) |
| `embedding` | Float32 Array | 1024-dimensional vector embedding |
| `video_blob` | VideoObject | Inline video data |

### Quick Start

#### Load with HuggingFace `datasets`

```python
import datasets

hf_ds = datasets.load_dataset(
    "lance-format/openvid-lance",
    split="train",
    streaming=True,
)
for row in hf_ds.take(3):
    print(row["caption"])
```

#### Load with Lance

```python
import lance

lance_ds = lance.dataset("hf://datasets/lance-format/openvid-lance/data/train.lance")
blob_file = lance_ds.take_blobs("video_blob", ids=[0])[0]
video_bytes = blob_file.read()
```

#### Load with LanceDB

```python
import lancedb

db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")
tbl = db.open_table("train")
print(f"LanceDB table opened with {len(tbl)} videos")
```

### Blob API

Lance stores videos as **inline blobs** - binary data embedded directly in the dataset. Videos are only loaded when explicitly requested (lazy loading).

```python
import lance

ds = lance.dataset("hf://datasets/lance-format/openvid-lance")

# 1. Browse metadata without loading video data
metadata = ds.scanner(
    columns=["caption", "aesthetic_score"],
    filter="aesthetic_score >= 4.5",
    limit=10
).to_table().to_pylist()

# 2. Load only a specific video blob
blob_file = ds.take_blobs("video_blob", ids=[3])[0]
video_bytes = blob_file.read()

# 3. Save to disk
with open("video.mp4", "wb") as f:
    f.write(video_bytes)
```

#### Export multiple videos

```python
indices = [0, 100, 500]
blob_files = ds.take_blobs("video_blob", ids=indices)

for i, blob_file in enumerate(blob_files):
    with open(f"video_{i}.mp4", "wb") as f:
        f.write(blob_file.read())
```

#### Open inline videos with PyAV

```python
import av

blob_file = ds.take_blobs("video_blob", ids=[123])[0]

with av.open(blob_file) as container:
    stream = container.streams.video[0]

    for seconds in (0.0, 1.0, 2.5):
        target_pts = int(seconds / stream.time_base)
        container.seek(target_pts, stream=stream)

        frame = None
        for candidate in container.decode(stream):
            if candidate.time is None:
                continue
            frame = candidate
            if frame.time >= seconds:
                break

        print(
            f"Seek {seconds:.1f}s -> {frame.width}x{frame.height} "
            f"(pts={frame.pts}, time={frame.time:.2f}s)"
        )
```

### HuggingFace Streaming Note

When streaming from HuggingFace, use minimal parameters to avoid rate limits:
- `nprobes=1` for vector search
- Column selection to reduce I/O

For best performance, **download the dataset locally**:

```bash
huggingface-cli download lance-format/openvid-lance --repo-type dataset --local-dir ./openvid

# Then load locally
ds = lance.dataset("./openvid")
```

### Vector Similarity Search (Lance)

```python
import pyarrow as pa

ref_video = ds.take([0], columns=["embedding"]).to_pylist()[0]
query_vector = pa.array([ref_video['embedding']], type=pa.list_(pa.float32(), 1024))

results = ds.scanner(
    nearest={
        "column": "embedding",
        "q": query_vector[0],
        "k": 5,
        "nprobes": 1,
        "refine_factor": 1
    }
).to_table().to_pylist()

for video in results[1:]:  # Skip first (query itself)
    print(video['caption'])
```

### Full-Text Search (Lance)

```python
results = ds.scanner(
    full_text_query="sunset beach",
    columns=["caption", "aesthetic_score"],
    limit=10,
    fast_search=True
).to_table().to_pylist()

for video in results:
    print(f"{video['caption']} - {video['aesthetic_score']:.2f}")
```

### LanceDB Vector Similarity Search

```python
import lancedb

db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")
tbl = db.open_table("train")

ref_video = tbl.limit(1).select(["embedding", "caption"]).to_pandas().to_dict('records')[0]
query_embedding = ref_video["embedding"]

results = tbl.search(query_embedding, vector_column_name="embedding") \
    .metric("L2") \
    .nprobes(1) \
    .limit(5) \
    .to_list()

for video in results[1:]:
    print(f"{video['caption'][:60]}...")
```

### LanceDB Full-Text Search

```python
import lancedb

db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")
tbl = db.open_table("train")

results = tbl.search("sunset beach") \
    .select(["caption", "aesthetic_score"]) \
    .limit(10) \
    .to_list()

for video in results:
    print(f"{video['caption']} - {video['aesthetic_score']:.2f}")
```

### Inspecting Indices

```python
dataset = lance.dataset("hf://datasets/lance-format/openvid-lance/data/train.lance")
indices = dataset.list_indices()
print(indices)
```

### Creating Custom Indices

```python
# ds is a local Lance dataset
ds.create_index(
    "embedding",
    index_type="IVF_PQ",
    num_partitions=256,
    num_sub_vectors=96,
    replace=True,
)
```

### Dataset Evolution

Lance supports flexible schema and data evolution - add/drop columns, backfill with SQL or Python, rename fields, or change data types without rewriting the whole dataset.

```python
import lance
import pyarrow as pa
import numpy as np

base = pa.table({"id": pa.array([1, 2, 3])})
dataset = lance.write_dataset(base, "openvid_evolution", mode="overwrite")

# 1. Grow the schema instantly (metadata-only)
dataset.add_columns(pa.field("quality_bucket", pa.string()))

# 2. Backfill with SQL expressions or constants
dataset.add_columns({"status": "'active'"})

# 3. Generate rich columns via Python batch UDFs
@lance.batch_udf()
def random_embedding(batch):
    arr = np.random.rand(batch.num_rows, 128).astype("float32")
    return pa.RecordBatch.from_arrays(
        [pa.FixedSizeListArray.from_arrays(arr.ravel(), 128)],
        names=["embedding"],
    )

dataset.add_columns(random_embedding)

# 4. Bring in offline annotations with merge
labels = pa.table({
    "id": pa.array([1, 2, 3]),
    "label": pa.array(["horse", "rabbit", "cat"]),
})
dataset.merge(labels, "id")

# 5. Rename or cast columns as needs change
dataset.alter_columns({"path": "quality_bucket", "name": "quality_tier"})
dataset.alter_columns({"path": "embedding", "data_type": pa.list_(pa.float16(), 128)})
```

### Key Statistics

- **Total Videos:** 937,957
- **Embedding Dimension:** 1024
- **Average Video Duration:** ~2.7 seconds
- **Frame Range:** 9-900 frames
- **FPS Range:** 23-60 fps
- **Caption Length:** 212-2,790 characters
- **Camera Motion Classes:** 31

### Citation

```bibtex
@article{nan2024openvid,
  title={OpenVid-1M: A Large-Scale High-Quality Dataset for Text-to-video Generation},
  author={Nan, Kepan and Xie, Rui and Zhou, Penghao and Fan, Tiehan and Yang, Zhenheng and Chen, Zhijie and Li, Xiang and Yang, Jian and Tai, Ying},
  journal={arXiv preprint arXiv:2407.02371},
  year={2024}
}
```
