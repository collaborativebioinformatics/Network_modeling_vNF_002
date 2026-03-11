# #!/usr/bin/env python3
“””
STEP 1 — VALIDATE_INPUT

Parse and validate CSV/TSV anchor node and edge files.
Outputs clean JSON files consumed by downstream processes.

Expected node file columns:
id            : unique node identifier
high_dim_edge : identifier of the high-dim edge this anchor holds
v_0, v_1, … : high-dimensional feature vector columns

Expected edge file columns:
source, target
“””

import argparse
import csv
import json
import sys

def detect_delimiter(path):
with open(path, newline=’’) as f:
sample = f.read(4096)
sniffer = csv.Sniffer()
try:
dialect = sniffer.sniff(sample, delimiters=’,\t’)
return dialect.delimiter
except csv.Error:
return ‘,’

def load_nodes(path):
delim = detect_delimiter(path)
nodes = []
errors = []

```
with open(path, newline='') as f:
    reader = csv.DictReader(f, delimiter=delim)
    required = {'id', 'high_dim_edge'}
    if not required.issubset(set(reader.fieldnames or [])):
        sys.exit(
            f"ERROR: nodes file must contain columns: {required}\n"
            f"Found: {reader.fieldnames}"
        )

    vector_cols = sorted(
        [c for c in reader.fieldnames if c.startswith('v_')],
        key=lambda c: int(c.split('_')[1])
    )
    if not vector_cols:
        sys.exit(
            "ERROR: nodes file must contain feature vector columns "
            "named v_0, v_1, v_2, ..."
        )

    for i, row in enumerate(reader, start=2):
        node_id_raw = row['id'].strip()
        # Convert to int if possible for consistency with graph operations
        try:
            node_id = int(node_id_raw)
        except ValueError:
            node_id = node_id_raw

        try:
            vector = [float(row[c]) for c in vector_cols]
        except ValueError as e:
            errors.append(f"Row {i}: invalid vector value — {e}")
            continue

        if not row['high_dim_edge'].strip():
            errors.append(f"Row {i}: missing high_dim_edge")
            continue

        nodes.append({
            'id': node_id,
            'high_dim_edge': row['high_dim_edge'].strip(),
            'vector': vector,
            'edges': []        # populated in BUILD_GRAPH from edges file
        })

return nodes, vector_cols, errors
```

def load_edges(path, valid_ids):
delim = detect_delimiter(path)
edges = []
errors = []

```
with open(path, newline='') as f:
    reader = csv.DictReader(f, delimiter=delim)
    required = {'source', 'target'}
    if not required.issubset(set(reader.fieldnames or [])):
        sys.exit(
            f"ERROR: edges file must contain columns: {required}\n"
            f"Found: {reader.fieldnames}"
        )

    for i, row in enumerate(reader, start=2):
        try:
            src_raw = row['source'].strip()
            tgt_raw = row['target'].strip()
            try:
                src = int(src_raw)
            except ValueError:
                src = src_raw
            try:
                tgt = int(tgt_raw)
            except ValueError:
                tgt = tgt_raw

            if src not in valid_ids:
                errors.append(f"Row {i}: source '{src}' not in nodes file")
                continue
            if tgt not in valid_ids:
                errors.append(f"Row {i}: target '{tgt}' not in nodes file")
                continue

            edges.append({'source': src, 'target': tgt})
        except Exception as e:
            errors.append(f"Row {i}: {e}")

return edges, errors
```

def main():
parser = argparse.ArgumentParser(description=“Validate pipeline inputs”)
parser.add_argument(’–nodes’,       required=True)
parser.add_argument(’–edges’,       required=True)
parser.add_argument(’–out_nodes’,   required=True)
parser.add_argument(’–out_edges’,   required=True)
parser.add_argument(’–report’,      required=True)
args = parser.parse_args()

```
all_errors = []

# Load and validate nodes
nodes, vector_cols, node_errors = load_nodes(args.nodes)
all_errors.extend(node_errors)

valid_ids = {n['id'] for n in nodes}

# Load and validate edges
edges, edge_errors = load_edges(args.edges, valid_ids)
all_errors.extend(edge_errors)

# Write validation report
with open(args.report, 'w') as f:
    f.write("VALIDATION REPORT\n")
    f.write("=" * 50 + "\n")
    f.write(f"Nodes loaded    : {len(nodes)}\n")
    f.write(f"Edges loaded    : {len(edges)}\n")
    f.write(f"Vector dims     : {len(vector_cols)}\n")
    f.write(f"Vector columns  : {', '.join(vector_cols)}\n")
    f.write(f"Errors          : {len(all_errors)}\n")
    if all_errors:
        f.write("\nERRORS:\n")
        for e in all_errors:
            f.write(f"  {e}\n")
    else:
        f.write("\nAll checks passed.\n")

if all_errors:
    print(f"VALIDATION FAILED — {len(all_errors)} error(s). "
          f"See {args.report}", file=sys.stderr)
    sys.exit(1)

# Write clean JSON outputs
with open(args.out_nodes, 'w') as f:
    json.dump(nodes, f, indent=2)

with open(args.out_edges, 'w') as f:
    json.dump(edges, f, indent=2)

print(f"Validation passed: {len(nodes)} nodes, {len(edges)} edges.")
```

if **name** == ‘**main**’:
main()
