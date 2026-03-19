# Synthetic Node Expansion Pipeline

A Nextflow pipeline that iteratively copies high-connectivity anchor nodes
into low-dimensional synthetic nodes until no new high-dimensional edges
are created. Edge assignment uses Shannon entropy scoring to determine
whether a high-dim edge should migrate or duplicate across nodes.

---

## Pipeline Steps

| Step | Process | Description |
|------|---------|-------------|
| 1 | `VALIDATE_INPUT` | Parse and validate CSV/TSV input files |
| 2 | `BUILD_GRAPH` | Construct initial anchor graph |
| 3 | `EXPAND_NODES` | Iterative synthetic node generation |
| 4 | `SCORE_ENTROPY` | Shannon entropy scoring and edge reassignment |
| 5 | `SUMMARIZE` | Final output files and summary report |

---

## Requirements

- [Nextflow](https://www.nextflow.io/) >= 23.10
- [Docker](https://www.docker.com/) (or Singularity for HPC)
- RAPIDS (optional, for GPU acceleration)

---

## Input Files

### nodes.csv / nodes.tsv
| Column | Description |
|--------|-------------|
| `id` | Unique node identifier (int or string) |
| `high_dim_edge` | Identifier of the high-dim edge this anchor holds |
| `v_0`, `v_1`, ... | High-dimensional feature vector (one column per dimension) |

Example:
```
id,high_dim_edge,v_0,v_1,v_2,...
0,hd_edge_A,0.374,0.951,0.732,...
1,hd_edge_B,0.432,0.291,0.612,...
```

### edges.csv / edges.tsv
| Column | Description |
|--------|-------------|
| `source` | Source node ID |
| `target` | Target node ID |

Example:
```
source,target
0,1
0,2
1,3
```

---

## Usage

### Local (CPU)
```bash
nextflow run main.nf \
  --nodes data/nodes.csv \
  --edges data/edges.csv \
  -profile local
```

### Local with custom parameters
```bash
nextflow run main.nf \
  --nodes data/nodes.csv \
  --edges data/edges.csv \
  --similarity 0.85 \
  --entropy_threshold 0.95 \
  --low_dim 16 \
  --max_rounds 100 \
  --outdir my_results \
  -profile local
```

### HPC (SLURM)
```bash
nextflow run main.nf \
  --nodes data/nodes.csv \
  --edges data/edges.csv \
  -profile slurm
```

### AWS Batch
```bash
# Update nextflow.config with your queue name, region, and S3 bucket first
nextflow run main.nf \
  --nodes s3://your-bucket/nodes.csv \
  --edges s3://your-bucket/edges.csv \
  --outdir s3://your-bucket/results \
  -profile aws
```

### Google Cloud
```bash
# Update nextflow.config with your project and GCS bucket first
nextflow run main.nf \
  --nodes gs://your-bucket/nodes.csv \
  --edges gs://your-bucket/edges.csv \
  --outdir gs://your-bucket/results \
  -profile gcp
```

---

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--nodes` | required | Path to nodes CSV/TSV |
| `--edges` | required | Path to edges CSV/TSV |
| `--similarity` | `0.8` | Cosine similarity threshold for high-dim edge creation |
| `--entropy_threshold` | `0.95` | Shannon entropy cutoff for edge duplication |
| `--low_dim` | `8` | Dimensionality of synthetic node embeddings |
| `--max_rounds` | `50` | Maximum expansion rounds before forced stop |
| `--outdir` | `results` | Output directory |

---

## Output Files

```
results/
├── validated/
│   ├── validated_nodes.json      # Clean node data
│   ├── validated_edges.json      # Clean edge data
│   └── validation_report.txt     # Validation summary
├── graph/
│   ├── anchor_graph.json         # Initial anchor graph (node-link)
│   └── degree_table.tsv          # Node degrees
├── expansion/
│   ├── expanded_graph.json       # Full graph after expansion
│   ├── synthetic_nodes.json      # All synthetic nodes with metadata
│   ├── groups.json               # Anchor groups post-expansion
│   └── expansion_log.tsv         # Per-round expansion log
├── entropy/
│   ├── entropy_scores.tsv        # Per-node entropy scores
│   ├── reassigned_groups.json    # Groups after edge reassignment
│   └── duplicated_edges.tsv      # Edges eligible for duplication
├── final_nodes.tsv               # All nodes: kind, edge holder, degree
├── final_edges.tsv               # All edges
├── final_groups.tsv              # Anchor groups with entropy scores
├── summary_report.txt            # Human-readable summary
├── pipeline_report.html          # Nextflow execution report
├── pipeline_timeline.html        # Nextflow timeline
├── pipeline_trace.tsv            # Resource usage per task
└── pipeline_dag.svg              # Pipeline DAG diagram
```

---

## GPU Acceleration

The pipeline uses RAPIDS (cuGraph + cuPy) when available.
The Docker image defaults to CPU. For GPU:

1. Use the RAPIDS base image in `nextflow.config`:
   ```
   process.container = 'nvcr.io/nvidia/rapidsai/base:24.10-cuda12.5-py3.12'
   ```
2. Ensure your compute environment has NVIDIA drivers and `--gpus all`
   passed to Docker (already set in `nextflow.config`).
3. For SLURM, Singularity will be used automatically — update the
   `.sif` path in the `slurm` profile.

---

## Plugging in Real Co-occurrence Data

The entropy scoring step (`bin/score_entropy.py`) currently uses
neighbor-set overlap as a proxy for co-occurrence. To use real data:

1. Prepare a co-occurrence matrix or frequency table for your high-dim edges.
2. Replace the `cooccurrence_score()` function in `bin/score_entropy.py`
   with a lookup into your data.
3. Re-run the pipeline — all other steps are unaffected.
