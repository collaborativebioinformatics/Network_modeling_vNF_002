#!/usr/bin/env python3
"""
STEP 5 — SUMMARIZE
===================
Generate final output TSV files and a human-readable summary report.
"""

import argparse
import json
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Summarize expansion results")
    parser.add_argument('--graph',       required=True)
    parser.add_argument('--groups',      required=True)
    parser.add_argument('--scores',      required=True)
    parser.add_argument('--duplicates',  required=True)
    parser.add_argument('--log',         required=True)
    parser.add_argument('--out_nodes',   required=True)
    parser.add_argument('--out_edges',   required=True)
    parser.add_argument('--out_groups',  required=True)
    parser.add_argument('--out_report',  required=True)
    args = parser.parse_args()

    with open(args.graph) as f:
        node_link = json.load(f)
    with open(args.groups) as f:
        groups = json.load(f)
    with open(args.duplicates) as f:
        dup_lines = f.read().strip().splitlines()
    with open(args.log) as f:
        log_lines = f.read().strip().splitlines()

    nodes = node_link.get('nodes', [])
    links = node_link.get('links', [])

    # -------------------------------------------------------------------
    # final_nodes.tsv
    # -------------------------------------------------------------------
    with open(args.out_nodes, 'w') as f:
        f.write("node_id\tkind\thigh_dim_edge\tdegree\n")
        # Build degree map
        degree_map = {}
        for link in links:
            degree_map[link['source']] = degree_map.get(link['source'], 0) + 1
            degree_map[link['target']] = degree_map.get(link['target'], 0) + 1

        # Build edge holder map
        holder_map = {}
        for g in groups.values():
            holder_map[g['edge_holder']] = g['high_dim_edge']

        for n in nodes:
            nid = n['id']
            kind = n.get('kind', 'anchor')
            hde = holder_map.get(nid, '')
            deg = degree_map.get(nid, 0)
            f.write(f"{nid}\t{kind}\t{hde}\t{deg}\n")

    # -------------------------------------------------------------------
    # final_edges.tsv
    # -------------------------------------------------------------------
    with open(args.out_edges, 'w') as f:
        f.write("source\ttarget\n")
        for link in links:
            f.write(f"{link['source']}\t{link['target']}\n")

    # -------------------------------------------------------------------
    # final_groups.tsv
    # -------------------------------------------------------------------
    with open(args.out_groups, 'w') as f:
        f.write("anchor_id\thigh_dim_edge\tedge_holder\t"
                "synthetic_count\tentropy\tduplicate_edge\tmembers\n")
        for g in groups.values():
            members_str = ','.join(str(m) for m in g['members'])
            f.write(
                f"{g['anchor_id']}\t{g['high_dim_edge']}\t{g['edge_holder']}\t"
                f"{g['synthetic_count']}\t{g.get('entropy', '')}\t"
                f"{g.get('duplicate_edge', False)}\t{members_str}\n"
            )

    # -------------------------------------------------------------------
    # Summary statistics from expansion log
    # -------------------------------------------------------------------
    log_rows = []
    if len(log_lines) > 1:
        headers = log_lines[0].split('\t')
        for line in log_lines[1:]:
            vals = line.split('\t')
            log_rows.append(dict(zip(headers, vals)))

    rounds_run = max((int(r['round']) for r in log_rows), default=0)
    total_added = sum(1 for r in log_rows if r.get('action') == 'added')
    total_skipped = sum(1 for r in log_rows if r.get('action') == 'skipped')
    n_anchors = sum(1 for n in nodes if n.get('kind') == 'anchor')
    n_synthetic = sum(1 for n in nodes if n.get('kind') == 'synthetic')
    n_duplicated = len(dup_lines) - 1  # subtract header

    # -------------------------------------------------------------------
    # summary_report.txt
    # -------------------------------------------------------------------
    with open(args.out_report, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("  SYNTHETIC NODE EXPANSION — SUMMARY REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"  Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        f.write("GRAPH\n")
        f.write(f"  Anchor nodes      : {n_anchors}\n")
        f.write(f"  Synthetic nodes   : {n_synthetic}\n")
        f.write(f"  Total nodes       : {len(nodes)}\n")
        f.write(f"  Total edges       : {len(links)}\n")
        f.write("\n")
        f.write("EXPANSION\n")
        f.write(f"  Rounds run        : {rounds_run}\n")
        f.write(f"  Nodes added       : {total_added}\n")
        f.write(f"  Copies skipped    : {total_skipped}\n")
        f.write("\n")
        f.write("EDGE ASSIGNMENT\n")
        f.write(f"  Groups            : {len(groups)}\n")
        f.write(f"  Duplicated edges  : {n_duplicated}\n")
        f.write("\n")
        f.write("OUTPUT FILES\n")
        f.write(f"  final_nodes.tsv   — all nodes with kind, edge holder, degree\n")
        f.write(f"  final_edges.tsv   — all edges\n")
        f.write(f"  final_groups.tsv  — anchor groups with entropy scores\n")
        f.write("=" * 60 + "\n")

    print("Summary complete.")
    print(f"  {n_anchors} anchors | {n_synthetic} synthetic | "
          f"{len(links)} edges | {n_duplicated} duplicated edges")


if __name__ == '__main__':
    main()
