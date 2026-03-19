#!/usr/bin/env python3
"""
STEP 2 — BUILD_GRAPH
====================
Construct the initial anchor graph from validated node and edge JSON files.
Outputs a node-link JSON graph and a degree table TSV.
"""

import argparse
import json

try:
    import cugraph as nx_backend
    GPU = True
    print("[INFO] RAPIDS detected — running on GPU.")
except ImportError:
    import networkx as nx_backend
    GPU = False
    print("[INFO] RAPIDS not found — falling back to CPU (NetworkX).")


def build_graph(nodes, edges):
    if GPU:
        import cudf
        if edges:
            edge_df = cudf.DataFrame(edges)
            G = nx_backend.Graph()
            G.from_cudf_edgelist(edge_df, source='source', destination='target')
        else:
            G = nx_backend.Graph()
    else:
        G = nx_backend.Graph()
        for n in nodes:
            G.add_node(n['id'], high_dim_edge=n['high_dim_edge'],
                       vector=n['vector'])
        for e in edges:
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


def graph_to_nodelink(G, nodes):
    """Serialise graph to node-link JSON (networkx compatible format)."""
    node_index = {n['id']: n for n in nodes}
    node_ids = list(node_index.keys())
    degrees = get_degrees(G, node_ids)

    nl = {
        'directed': False,
        'multigraph': False,
        'nodes': [],
        'links': []
    }

    for n in nodes:
        nl['nodes'].append({
            'id': n['id'],
            'high_dim_edge': n['high_dim_edge'],
            'vector': n['vector'],
            'kind': 'anchor',
            'degree': degrees.get(n['id'], 0)
        })

    if GPU:
        edges_df = G.view_edge_list()
        for _, row in edges_df.to_pandas().iterrows():
            nl['links'].append({'source': int(row['src']),
                                'target': int(row['dst'])})
    else:
        for u, v in G.edges():
            nl['links'].append({'source': u, 'target': v})

    return nl


def main():
    parser = argparse.ArgumentParser(description="Build initial anchor graph")
    parser.add_argument('--nodes',       required=True)
    parser.add_argument('--edges',       required=True)
    parser.add_argument('--out_graph',   required=True)
    parser.add_argument('--out_degrees', required=True)
    args = parser.parse_args()

    with open(args.nodes) as f:
        nodes = json.load(f)
    with open(args.edges) as f:
        edges = json.load(f)

    G = build_graph(nodes, edges)
    nl = graph_to_nodelink(G, nodes)

    # Write node-link graph JSON
    with open(args.out_graph, 'w') as f:
        json.dump(nl, f, indent=2)

    # Write degree table TSV
    node_ids = [n['id'] for n in nodes]
    degrees = get_degrees(G, node_ids)
    with open(args.out_degrees, 'w') as f:
        f.write("node_id\tdegree\n")
        for nid, deg in sorted(degrees.items(), key=lambda x: -x[1]):
            f.write(f"{nid}\t{deg}\n")

    n_nodes = len(nodes)
    n_edges = len(edges)
    print(f"Graph built: {n_nodes} anchor nodes, {n_edges} edges.")
    top = sorted(degrees.items(), key=lambda x: -x[1])[:5]
    print(f"Top-5 by degree: {top}")


if __name__ == '__main__':
    main()
