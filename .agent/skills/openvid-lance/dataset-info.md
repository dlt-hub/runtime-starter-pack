
# OpenVid Dataset (Lance Format)\
\
Lance format version of the [OpenVid dataset](https://huggingface.co/datasets/nkp37/OpenVid-1M) with **937,957 high-quality videos** stored with inline video blobs, embeddings, and rich metadata.\
\
## Why Lance?\
\
Lance is an open-source format designed for multimodal AI data, offering significant advantages over traditional formats for modern AI workloads.\
\
- **Blazing Fast Random Access**: Optimized for fetching scattered rows, making it ideal for random sampling, real-time ML serving, and interactive applications without performance degradation.\
- **Native Multimodal Support**: Store text, embeddings, and other data types together in a single file. Large binary objects are loaded lazily, and vectors are optimized for fast similarity search.\
- **Efficient Data Evolution**: Add new columns and backfill data without rewriting the entire dataset. This is perfect for evolving ML features, adding new embeddings, or introducing moderation tags over time.\
- **Versatile Querying**: Supports combining vector similarity search, full-text search, and SQL-style filtering in a single query, accelerated by on-disk indexes.\
\
## Key Features\
\
The OpenVid dataset is stored in Lance format with inline video blobs, video embeddings, and rich metadata.\
\
- **Videos stored inline as blobs**: No external files to manage\
- **Efficient column access**: Load metadata without touching video data\
- **Prebuilt indices available**: IVF\_PQ index for similarity search, FTS index on captions\
- **Fast random access**: Read any video instantly by index\
- **HuggingFace integration**: Load directly from the Hub\
\
## Quick Start\
\
### Load with `datasets.load_dataset`\
\
```python\
import datasets\
\
hf_ds = datasets.load_dataset(\
    "lance-format/openvid-lance",\
    split="train",\
    streaming=True,\
)\
# Take first three rows and print captions\
for row in hf_ds.take(3):\
    print(row["caption"])\
```\
\
### Load with Lance\
\
Use Lance for ANN search, retrieving specific blob bytes or advanced indexing, while still pointing at the dataset on the Hub:\
\
```python\
import lance\
\
lance_ds = lance.dataset("hf://datasets/lance-format/openvid-lance/data/train.lance")\
blob_file = lance_ds.take_blobs("video_blob", ids=[0])[0]\
video_bytes = blob_file.read()\
```\
\
### Load with LanceDB\
\
These tables can also be consumed by [LanceDB](https://docs.lancedb.com/), the multimodal lakehouse for AI (built on top of Lance).\
LanceDB provides several convenience APIs for search, index creation and data updates on top of the Lance format.\
\
```python\
import lancedb\
\
db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")\
tbl = db.open_table("train")\
print(f"LanceDB table opened with {len(tbl)} videos")\
```\
\
## Blob API\
\
Lance stores videos as **inline blobs** \- binary data embedded directly in the dataset. This provides:\
\
- **Single source of truth** \- Videos and metadata together in one dataset\
- **Lazy loading** \- Videos only loaded when you explicitly request them\
- **Efficient storage** \- Optimized encoding for large binary data\
\
```python\
import lance\
\
ds = lance.dataset("hf://datasets/lance-format/openvid-lance")\
\
# 1. Browse metadata without loading video data\
metadata = ds.scanner(\
    columns=["caption", "aesthetic_score"],  # No video_blob column!\
    filter="aesthetic_score >= 4.5",\
    limit=10\
).to_table().to_pylist()\
\
# 2. User selects video to watch\
selected_index = 3\
\
# 3. Load only that video blob\
blob_file = ds.take_blobs("video_blob", ids=[selected_index])[0]\
video_bytes = blob_file.read()\
\
# 4. Save to disk\
with open("video.mp4", "wb") as f:\
    f.write(video_bytes)\
```\
\
> **⚠️ HuggingFace Streaming Note**\
>\
> When streaming from HuggingFace (as shown above), some operations use minimal parameters to avoid rate limits:\
>\
> - `nprobes=1` for vector search (lowest value)\
> - Column selection to reduce I/O\
>\
> **You may still hit rate limits on HuggingFace's free tier.** For best performance and to avoid rate limits, **download the dataset locally**:\
>\
> ```bash\
> # Download once\
> huggingface-cli download lance-format/openvid-lance --repo-type dataset --local-dir ./openvid\
>\
> # Then load locally\
> ds = lance.dataset("./openvid")\
> ```\
>\
> Streaming is recommended only for quick exploration and testing.\
\
## Usage Examples\
\
### 1\. Browse Metadata quickly (fast, no video loading)\
\
```python\
# Load only metadata without heavy video blobs\
scanner = ds.scanner(\
    columns=["caption", "aesthetic_score", "motion_score"],\
    limit=10\
)\
videos = scanner.to_table().to_pylist()\
\
for video in videos:\
    print(f"{video['caption']} - Quality: {video['aesthetic_score']:.2f}")\
```\
\
### 2\. Export videos from blobs\
\
Retrieve specific video files if you want to work with subsets of the data. This is done by exporting them to files on your local machine.\
\
```python\
# Load specific videos by index\
indices = [0, 100, 500]\
blob_files = ds.take_blobs("video_blob", ids=indices)\
\
# Save to disk\
for i, blob_file in enumerate(blob_files):\
    with open(f"video_{i}.mp4", "wb") as f:\
        f.write(blob_file.read())\
```\
\
### 3\. Open inline videos with PyAV and run seeks directly on the blob file\
\
Using seeks, you can open a specific set of frames within a blob. The example below shows this.\
\
```python\
import av\
\
selected_index = 123\
blob_file = ds.take_blobs("video_blob", ids=[selected_index])[0]\
\
with av.open(blob_file) as container:\
    stream = container.streams.video[0]\
\
    for seconds in (0.0, 1.0, 2.5):\
        target_pts = int(seconds / stream.time_base)\
        container.seek(target_pts, stream=stream)\
\
        frame = None\
        for candidate in container.decode(stream):\
            if candidate.time is None:\
                continue\
            frame = candidate\
            if frame.time >= seconds:\
                break\
\
        print(\
            f"Seek {seconds:.1f}s -> {frame.width}x{frame.height} "\
            f"(pts={frame.pts}, time={frame.time:.2f}s)"\
        )\
```\
\
### 4\. Inspecting Existing Indices\
\
You can inspect the prebuilt indices on the dataset:\
\
```python\
import lance\
\
# Open the dataset\
dataset = lance.dataset("hf://datasets/lance-format/openvid-lance/data/train.lance")\
\
# List all indices\
indices = dataset.list_indices()\
print(indices)\
```\
\
### 5\. Create New Index\
\
While this dataset comes with pre-built indices, you can also create your own custom indices if needed.\
The example below creates a vector index on the `embedding` column.\
\
```python\
# ds is a local Lance dataset\
ds.create_index(\
    "embedding",\
    index_type="IVF_PQ",\
    num_partitions=256,\
    num_sub_vectors=96,\
    replace=True,\
)\
```\
\
### 6\. Vector Similarity Search\
\
```python\
import pyarrow as pa\
\
# Find similar videos\
ref_video = ds.take([0], columns=["embedding"]).to_pylist()[0]\
query_vector = pa.array([ref_video['embedding']], type=pa.list_(pa.float32(), 1024))\
\
results = ds.scanner(\
    nearest={\
        "column": "embedding",\
        "q": query_vector[0],\
        "k": 5,\
        "nprobes": 1,\
        "refine_factor": 1\
    }\
).to_table().to_pylist()\
\
for video in results[1:]:  # Skip first (query itself)\
    print(video['caption'])\
```\
\
### 7\. Full-Text Search\
\
```python\
# Search captions using FTS index\
results = ds.scanner(\
    full_text_query="sunset beach",\
    columns=["caption", "aesthetic_score"],\
    limit=10,\
    fast_search=True\
).to_table().to_pylist()\
\
for video in results:\
    print(f"{video['caption']} - {video['aesthetic_score']:.2f}")\
```\
\
## Dataset Evolution\
\
Lance supports flexible schema and data evolution ( [docs](https://lance.org/guide/data_evolution/?h=evol)). You can add/drop columns, backfill with SQL or Python, rename fields, or change data types without rewriting the whole dataset. In practice this lets you:\
\
- Introduce fresh metadata (moderation labels, embeddings, quality scores) as new signals become available.\
- Add new columns to existing datasets without re-exporting terabytes of video.\
- Adjust column names or shrink storage (e.g., cast embeddings to float16) while keeping previous snapshots queryable for reproducibility.\
\
```python\
import lance\
import pyarrow as pa\
import numpy as np\
\
base = pa.table({"id": pa.array([1, 2, 3])})\
dataset = lance.write_dataset(base, "openvid_evolution", mode="overwrite")\
\
# 1. Grow the schema instantly (metadata-only)\
dataset.add_columns(pa.field("quality_bucket", pa.string()))\
\
# 2. Backfill with SQL expressions or constants\
dataset.add_columns({"status": "'active'"})\
\
# 3. Generate rich columns via Python batch UDFs\
@lance.batch_udf()\
def random_embedding(batch):\
    arr = np.random.rand(batch.num_rows, 128).astype("float32")\
    return pa.RecordBatch.from_arrays(\
        [pa.FixedSizeListArray.from_arrays(arr.ravel(), 128)],\
        names=["embedding"],\
    )\
\
dataset.add_columns(random_embedding)\
\
# 4. Bring in offline annotations with merge\
labels = pa.table({\
    "id": pa.array([1, 2, 3]),\
    "label": pa.array(["horse", "rabbit", "cat"]),\
})\
dataset.merge(labels, "id")\
\
# 5. Rename or cast columns as needs change\
dataset.alter_columns({"path": "quality_bucket", "name": "quality_tier"})\
dataset.alter_columns({"path": "embedding", "data_type": pa.list_(pa.float16(), 128)})\
```\
\
These operations are automatically versioned, so prior experiments can still point to earlier versions while OpenVid keeps evolving.\
\
## LanceDB\
\
LanceDB users can follow the following examples to run search queries on the dataset.\
\
### LanceDB Vector Similarity Search\
\
```python\
import lancedb\
\
db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")\
tbl = db.open_table("train")\
\
# Get a video to use as a query\
ref_video = tbl.limit(1).select(["embedding", "caption"]).to_pandas().to_dict('records')[0]\
query_embedding = ref_video["embedding"]\
\
results = tbl.search(query_embedding, vector_column_name="embedding") \\
    .metric("L2") \\
    .nprobes(1) \\
    .limit(5) \\
    .to_list()\
\
for video in results[1:]: # Skip first (query itself)\
    print(f"{video['caption'][:60]}...")\
```\
\
### LanceDB Full-Text Search\
\
```python\
import lancedb\
\
db = lancedb.connect("hf://datasets/lance-format/openvid-lance/data")\
tbl = db.open_table("train")\
\
results = tbl.search("sunset beach") \\
    .select(["caption", "aesthetic_score"]) \\
    .limit(10) \\
    .to_list()\
\
for video in results:\
    print(f"{video['caption']} - {video['aesthetic_score']:.2f}")\
```\
\
## Citation\
\
@article{nan2024openvid,\
title={OpenVid-1M: A Large-Scale High-Quality Dataset for Text-to-video Generation},\
author={Nan, Kepan and Xie, Rui and Zhou, Penghao and Fan, Tiehan and Yang, Zhenheng and Chen, Zhijie and Li, Xiang and Yang, Jian and Tai, Ying},\
journal={arXiv preprint arXiv:2407.02371},\
year={2024}\
}\
\
## License\
\
Please check the original OpenVid dataset license for usage terms.\
\
Downloads last month\
\
403\
\
Use this dataset\
\
[Size of the auto-converted Parquet files (First 5GB):\\
\\
5.21 GB](https://huggingface.co/datasets/lance-format/openvid-lance/tree/refs%2Fconvert%2Fparquet/)\
Number of rows (First 5GB):\
\
1,792\
\
Estimated number of rows:\
\
937,957\
\
## Paper for  lance-format/openvid-lance\
\
[**OpenVid-1M: A Large-Scale High-Quality Dataset for Text-to-video  Generation**\\
\\
Paper\\
•\\
2407.02371\\
•Published\\
Jul 2, 2024•\\
\\
54](https://huggingface.co/papers/2407.02371)\
\
System theme\
\
Company\
\
[TOS](https://huggingface.co/terms-of-service) [Privacy](https://huggingface.co/privacy) [About](https://huggingface.co/huggingface) [Careers](https://apply.workable.com/huggingface/)  [Hugging Face](https://huggingface.co/)\
\
Website\
\
[Models](https://huggingface.co/models) [Datasets](https://huggingface.co/datasets) [Spaces](https://huggingface.co/spaces) [Pricing](https://huggingface.co/pricing) [Docs](https://huggingface.co/docs)\
\
The table contains 100 rows per page, up to 5GB.\
\
StripeM-Inner
