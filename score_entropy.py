#!/usr/bin/env python3
"""
STEP 4 — SCORE_ENTROPY
=======================
Shannon entropy scoring and edge reassignment.

For each anchor group:
  1. Compute co-occurrence affinity of the high-dim edge across
     the current holder vs each synthetic copy.
  2. Reassign the edge to the highest-affinity member.
  3. If entropy of the co-occurrence distribution >= theta,
     allow the edge to duplicate to two low-dim nodes.
"""

import argparse
import json
import math

try:
    import cupy as np_backend
    GPU = True
    print("[INFO] RAPIDS detected — running on GPU.")
except ImportError:
    import numpy as np_backend
    GPU = False
    print("[INFO] RAPIDS not found — falling back to CPU (NumPy).")


# ---------------------------------------------------------------------------
# Shannon entropy
# ---------------------------------------------------------------------------

def shannon_entropy(counts):
    """
    H = -sum(p * log2(p)) for p > 0.
    Maximum value for 2 equal groups = 1.0 bit.
    """
    arr = np_backend.array(counts, dtype=float)
    total = float(arr.sum())
    if total == 0:
        return 0.0
    p = arr / total
    h = 0.0
    for pi in p.tolist():
        if pi > 0:
            h -= pi * math.log2(pi)
    return h


# ---------------------------------------------------------------------------
# Co-occurrence affinity
# ---------------------------------------------------------------------------

def build_neighbor_sets(node_link):
    """Return dict {node_id: set of neighbor_ids} from node-link graph."""
    neighbors = {}
    for link in node_link.get('links', []):
        src = link['source']
        tgt = link['target']
        neighbors.setdefault(src, set()).add(tgt)
        neighbors.setdefault(tgt, set()).add(src)
    return neighbors


def cooccurrence_score(high_dim_edge, neighbor_set, all_edges_in_group):
    """
    Proxy co-occurrence: count how many of this node's neighbors
    share the same high_dim_edge cluster.

    In production: replace with actual edge co-occurrence frequency
    data from your dataset (e.g. transaction logs, occurrence matrices).
    """
    return sum(1 for nbr in neighbor_set if nbr in all_edges_in_group)


# ---------------------------------------------------------------------------
# Main scoring
# ---------------------------------------------------------------------------

def score_and_reassign(node_link, groups, synthetic_nodes, entropy_threshold):
    neighbor_sets = build_neighbor_sets(node_link)

    entropy_scores = []    # list of dicts → TSV
    duplicated_edges = []  # list of dicts → TSV
    updated_groups = {}

    for anchor_id_str, group in groups.items():
        # group keys are strings when loaded from JSON
        anchor_id = group['anchor_id']
        high_dim_edge = group['high_dim_edge']
        members = group['members']

        # Set of all member node IDs (for co-occurrence lookup)
        member_set = set(members)

        # Score each member
        member_scores = {}
        for member in members:
            nbrs = neighbor_sets.get(member, set())
            score = cooccurrence_score(high_dim_edge, nbrs, member_set)
            member_scores[member] = score

        scores_list = list(member_scores.values())
        h = shannon_entropy(scores_list)

        # Reassign edge to highest-scoring member
        best_member = max(member_scores, key=member_scores.get)
        current_holder = group['edge_holder']
        reassigned = best_member != current_holder

        # Record entropy scores
        for member, score in member_scores.items():
            entropy_scores.append({
                'anchor_id': anchor_id,
                'high_dim_edge': high_dim_edge,
                'member': member,
                'cooccurrence_score': score,
                'entropy': round(h, 6),
                'edge_holder': best_member,
                'reassigned': reassigned
            })

        # Check duplication eligibility
        duplicate = h >= entropy_threshold
        if duplicate:
            # Find the two highest-scoring members
            sorted_members = sorted(member_scores.items(),
                                    key=lambda x: -x[1])
            top_two = [m for m, _ in sorted_members[:2]]
            duplicated_edges.append({
                'anchor_id': anchor_id,
                'high_dim_edge': high_dim_edge,
                'holder_1': top_two[0],
                'holder_2': top_two[1] if len(top_two) > 1 else '',
                'entropy': round(h, 6)
            })
            # Mark synthetic nodes with duplicate edge
            for node_id in top_two:
                if node_id in synthetic_nodes:
                    synthetic_nodes[node_id]['duplicate_edge'] = True

        updated_groups[str(anchor_id)] = {
            **group,
            'edge_holder': best_member,
            'entropy': round(h, 6),
            'duplicate_edge': duplicate
        }

        status = "DUPLICATE" if duplicate else ("REASSIGNED" if reassigned
                                                else "unchanged")
        print(f"Group [{anchor_id}] edge='{high_dim_edge}' "
              f"H={h:.4f} holder={best_member} [{status}]")

    return updated_groups, entropy_scores, duplicated_edges


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Shannon entropy scoring")
    parser.add_argument('--graph',          required=True)
    parser.add_argument('--synthetic',      required=True)
    parser.add_argument('--groups',         required=True)
    parser.add_argument('--threshold',      type=float, required=True)
    parser.add_argument('--out_scores',     required=True)
    parser.add_argument('--out_groups',     required=True)
    parser.add_argument('--out_duplicates', required=True)
    args = parser.parse_args()

    with open(args.graph) as f:
        node_link = json.load(f)
    with open(args.synthetic) as f:
        synthetic_nodes = json.load(f)
    with open(args.groups) as f:
        groups = json.load(f)

    updated_groups, entropy_scores, duplicated_edges = score_and_reassign(
        node_link, groups, synthetic_nodes, args.threshold
    )

    # Write reassigned groups JSON
    with open(args.out_groups, 'w') as f:
        json.dump(updated_groups, f, indent=2)

    # Write entropy scores TSV
    score_cols = ['anchor_id', 'high_dim_edge', 'member',
                  'cooccurrence_score', 'entropy', 'edge_holder', 'reassigned']
    with open(args.out_scores, 'w') as f:
        f.write('\t'.join(score_cols) + '\n')
        for row in entropy_scores:
            f.write('\t'.join(str(row.get(c, '')) for c in score_cols) + '\n')

    # Write duplicated edges TSV
    dup_cols = ['anchor_id', 'high_dim_edge', 'holder_1', 'holder_2', 'entropy']
    with open(args.out_duplicates, 'w') as f:
        f.write('\t'.join(dup_cols) + '\n')
        for row in duplicated_edges:
            f.write('\t'.join(str(row.get(c, '')) for c in dup_cols) + '\n')

    print(f"Entropy scoring complete.")
    print(f"  Groups scored    : {len(updated_groups)}")
    print(f"  Edges duplicated : {len(duplicated_edges)}")


if __name__ == '__main__':
    main()
