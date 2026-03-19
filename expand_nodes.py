#!/usr/bin/env python3
"""
STEP 3 — EXPAND_NODES
======================
Core iterative expansion loop.
Copies high-connectivity anchor nodes into synthetic low-dim nodes
until no new high-dim edges would be created.
"""

import argparse
import json
import numpy as np

try:
    import cugraph as nx_backend
    import cupy as np_backend
    GPU = True
    print("[INFO] RAPIDS detected — running on GPU.")
except ImportError:
    import networkx as nx_backend
    GPU = False
    np_backend = np
    print("[INFO] RAPIDS not found — falling back to CPU (NetworkX / NumPy).")


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------

def build_nx_graph(node_link):
    """Reconstruct a NetworkX / cuGraph graph from node-link JSON."""
    if GPU:
        import cudf
        links = node_link.get('links', [])
        if links:
            edge_df = cudf.DataFrame(links)
            G = nx_backend.Graph()
            G.from_cudf_edgelist(edge_df, source='source', destination='target')
        else:
            G = nx_backend.Graph()
    else:
        G = nx_backend.Graph()
        for n in node_link['nodes']:
            G.add_node(n['id'], **{k: v for k, v in n.items() if k != 'id'})
        for e in node_link['links']:
            G.add_edge(e['source'], e['target'])
    return G


def get_degrees(G, node_ids):
    if GPU:
        deg_df = G.degree()
        deg_map = dict(zip(deg_df['vertex'].to_pandas(),
                           deg_df['degree'].to_pandas()))
    else:
        deg_map = dict(G.degree())
    return {n: deg_map.get(n, 0) for n in node_ids}


def get_neighbors(G, node):
    if GPU:
        return list(G.neighbors(node))
    return list(G.neighbors(node))


def add_node(G, node_id, **attrs):
    if not GPU:
        G.add_node(node_id, **attrs)


def add_edge(G, u, v):
    if GPU:
        # cuGraph: rebuild edge list — in practice use incremental approach
        G.add_edge(u, v)
    else:
        G.add_edge(u, v)


# ---------------------------------------------------------------------------
# High-dim similarity
# ---------------------------------------------------------------------------

def cosine_similarity(a, b):
    a = np_backend.array(a, dtype=float)
    b = np_backend.array(b, dtype=float)
    na = np_backend.linalg.norm(a)
    nb = np_backend.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np_backend.dot(a, b) / (na * nb))


def high_dim_similar(vec_a, vec_b, threshold):
    return cosine_similarity(vec_a, vec_b) >= threshold


# ---------------------------------------------------------------------------
# Synthetic vector generation
# ---------------------------------------------------------------------------

def generate_synthetic_vector(high_dim_vector, low_dim, seed=None):
    """
    Returns:
      high_dim : perturbed anchor vector (for edge checking)
      low_dim  : random projection (synthetic node's embedding)

    Replace the random projection with PCA / UMAP in production.
    """
    rng = np.random.default_rng(seed)
    vec = np.array(high_dim_vector)
    d = len(vec)

    noise = rng.standard_normal(d) * 0.05
    high_dim = vec + noise

    proj = rng.standard_normal((low_dim, d)) / np.sqrt(low_dim)
    low_dim_vec = proj @ vec

    return high_dim.tolist(), low_dim_vec.tolist()


# ---------------------------------------------------------------------------
# Anchor group
# ---------------------------------------------------------------------------

class AnchorGroup:
    def __init__(self, anchor_id, vector, high_dim_edge):
        self.anchor_id = anchor_id
        self.vector = vector
        self.high_dim_edge = high_dim_edge
        self.edge_holder = anchor_id
        self.members = [anchor_id]
        self.synthetic_count = 0

    def add_synthetic(self, node_id):
        self.members.append(node_id)
        self.synthetic_count += 1

    def to_dict(self):
        return {
            'anchor_id': self.anchor_id,
            'high_dim_edge': self.high_dim_edge,
            'edge_holder': self.edge_holder,
            'members': self.members,
            'synthetic_count': self.synthetic_count
        }


# ---------------------------------------------------------------------------
# Main expansion loop
# ---------------------------------------------------------------------------

def expand(node_link, nodes_list, similarity_threshold, low_dim, max_rounds):
    G = build_nx_graph(node_link)

    # Build anchor groups
    groups = {}
    for n in nodes_list:
        aid = n['id']
        groups[aid] = AnchorGroup(aid, n['vector'], n['high_dim_edge'])

    anchor_ids = list(groups.keys())
    synthetic_nodes = {}
    expansion_log = []   # list of dicts for TSV output

    for round_num in range(1, max_rounds + 1):
        degrees = get_degrees(G, anchor_ids)
        ranked = sorted(anchor_ids, key=lambda a: degrees.get(a, 0), reverse=True)

        new_this_round = []

        for anchor_id in ranked:
            group = groups[anchor_id]
            seed = round_num * 10000 + (anchor_id if isinstance(anchor_id, int)
                                        else hash(anchor_id) % 10000)
            syn_high, syn_low = generate_synthetic_vector(
                group.vector, low_dim=low_dim, seed=seed
            )
            syn_id = f"syn_{anchor_id}_r{round_num}"

            # Check against all other anchors for new high-dim edge
            creates_new = False
            matched_anchor = None
            for other_id, other_group in groups.items():
                if other_id == anchor_id:
                    continue
                if high_dim_similar(syn_high, other_group.vector,
                                    similarity_threshold):
                    creates_new = True
                    matched_anchor = other_id
                    break

            log_row = {
                'round': round_num,
                'anchor_id': anchor_id,
                'syn_id': syn_id,
                'degree': degrees.get(anchor_id, 0),
                'creates_new_edge': creates_new,
                'matched_anchor': matched_anchor if matched_anchor is not None else ''
            }

            if not creates_new:
                log_row['action'] = 'skipped'
                expansion_log.append(log_row)
                continue

            # Add to graph
            add_node(G, syn_id, kind='synthetic', vector=syn_low)
            add_edge(G, anchor_id, syn_id)
            group.add_synthetic(syn_id)

            synthetic_nodes[syn_id] = {
                'anchor_id': anchor_id,
                'high_dim_vector': syn_high,
                'low_dim_vector': syn_low,
                'round': round_num,
                'high_dim_edge': group.high_dim_edge,
                'duplicate_edge': False
            }
            new_this_round.append(syn_id)
            log_row['action'] = 'added'
            expansion_log.append(log_row)

        print(f"Round {round_num}: {len(new_this_round)} new synthetic node(s).")

        if not new_this_round:
            print(f"Converged after {round_num} round(s).")
            break
    else:
        print(f"WARNING: reached max_rounds={max_rounds} without convergence.")

    # Serialise final graph to node-link JSON
    final_nl = {
        'directed': False,
        'multigraph': False,
        'nodes': [],
        'links': []
    }
    if GPU:
        pass  # rebuild from cuGraph structures
    else:
        for nid, data in G.nodes(data=True):
            final_nl['nodes'].append({'id': nid, **data})
        for u, v in G.edges():
            final_nl['links'].append({'source': u, 'target': v})

    return final_nl, groups, synthetic_nodes, expansion_log


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Expand synthetic nodes")
    parser.add_argument('--graph',        required=True)
    parser.add_argument('--nodes',        required=True)
    parser.add_argument('--similarity',   type=float, required=True)
    parser.add_argument('--low_dim',      type=int,   required=True)
    parser.add_argument('--max_rounds',   type=int,   required=True)
    parser.add_argument('--out_graph',    required=True)
    parser.add_argument('--out_synthetic',required=True)
    parser.add_argument('--out_groups',   required=True)
    parser.add_argument('--out_log',      required=True)
    args = parser.parse_args()

    with open(args.graph) as f:
        node_link = json.load(f)
    with open(args.nodes) as f:
        nodes_list = json.load(f)

    final_nl, groups, synthetic_nodes, expansion_log = expand(
        node_link, nodes_list,
        similarity_threshold=args.similarity,
        low_dim=args.low_dim,
        max_rounds=args.max_rounds
    )

    with open(args.out_graph, 'w') as f:
        json.dump(final_nl, f, indent=2)

    with open(args.out_synthetic, 'w') as f:
        json.dump(synthetic_nodes, f, indent=2)

    with open(args.out_groups, 'w') as f:
        json.dump({k: v.to_dict() for k, v in groups.items()}, f, indent=2)

    # Write expansion log TSV
    with open(args.out_log, 'w') as f:
        cols = ['round', 'anchor_id', 'syn_id', 'degree',
                'creates_new_edge', 'matched_anchor', 'action']
        f.write('\t'.join(cols) + '\n')
        for row in expansion_log:
            f.write('\t'.join(str(row.get(c, '')) for c in cols) + '\n')

    print(f"Expansion complete: {len(synthetic_nodes)} synthetic nodes created.")


if __name__ == '__main__':
    main()
