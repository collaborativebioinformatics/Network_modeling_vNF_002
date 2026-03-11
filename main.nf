#!/usr/bin/env nextflow

/*
================================================================================
  Synthetic Node Expansion Pipeline
================================================================================
  Iteratively copies high-connectivity anchor nodes into low-dimensional
  synthetic nodes until no new high-dimensional edges are created.

  Steps:
    1. VALIDATE_INPUT   — parse and validate CSV/TSV anchor data
    2. BUILD_GRAPH      — construct initial anchor graph from validated data
    3. EXPAND_NODES     — iterative synthetic node generation (core loop)
    4. SCORE_ENTROPY    — Shannon entropy scoring and edge reassignment
    5. SUMMARIZE        — final report and output files

  Usage:
    nextflow run main.nf --nodes nodes.csv --edges edges.tsv

  Full options:
    nextflow run main.nf \
      --nodes             nodes.csv     \
      --edges             edges.tsv     \
      --similarity        0.8           \
      --entropy_threshold 0.95          \
      --low_dim           8             \
      --max_rounds        50            \
      --outdir            results/
================================================================================
*/

nextflow.enable.dsl = 2

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------
params.nodes              = null          // path to nodes CSV/TSV
params.edges              = null          // path to edges CSV/TSV
params.similarity         = 0.8          // cosine similarity threshold
params.entropy_threshold  = 0.95         // Shannon entropy duplication cutoff
params.low_dim            = 8            // synthetic node embedding dimensions
params.max_rounds         = 50           // max expansion rounds
params.outdir             = "results"    // output directory

// ---------------------------------------------------------------------------
// Help message
// ---------------------------------------------------------------------------
if (params.help) {
    log.info """
    ╔══════════════════════════════════════════════════════╗
    ║        Synthetic Node Expansion Pipeline             ║
    ╚══════════════════════════════════════════════════════╝

    Usage:
      nextflow run main.nf --nodes nodes.csv --edges edges.tsv [options]

    Required:
      --nodes             Path to nodes CSV/TSV file
                          Columns: id, vector_* (high-dim features),
                                   high_dim_edge
      --edges             Path to edges CSV/TSV file
                          Columns: source, target

    Optional:
      --similarity        Cosine similarity threshold for high-dim edges
                          [default: ${params.similarity}]
      --entropy_threshold Shannon entropy cutoff for edge duplication
                          [default: ${params.entropy_threshold}]
      --low_dim           Dimensionality of synthetic node embeddings
                          [default: ${params.low_dim}]
      --max_rounds        Maximum expansion rounds
                          [default: ${params.max_rounds}]
      --outdir            Output directory
                          [default: ${params.outdir}]

    Profiles:
      -profile local      CPU only (default)
      -profile slurm      HPC with SLURM scheduler + GPU support
      -profile aws        AWS Batch
      -profile gcp        Google Cloud Life Sciences
    """.stripIndent()
    exit 0
}

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------
if (!params.nodes) error "ERROR: --nodes is required. Run with --help for usage."
if (!params.edges) error "ERROR: --edges is required. Run with --help for usage."

// ---------------------------------------------------------------------------
// Processes
// ---------------------------------------------------------------------------

/*
 * STEP 1: Validate and normalise input CSV/TSV files.
 * Outputs clean JSON representations consumed by downstream processes.
 */
process VALIDATE_INPUT {
    tag "validate"
    label "process_low"

    publishDir "${params.outdir}/validated", mode: 'copy'

    input:
    path nodes_file
    path edges_file

    output:
    path "validated_nodes.json",  emit: nodes
    path "validated_edges.json",  emit: edges
    path "validation_report.txt", emit: report

    script:
    """
    python3 ${projectDir}/bin/validate_input.py \
        --nodes   ${nodes_file}  \
        --edges   ${edges_file}  \
        --out_nodes  validated_nodes.json \
        --out_edges  validated_edges.json \
        --report     validation_report.txt
    """
}

/*
 * STEP 2: Build the initial anchor graph from validated input.
 * Outputs a serialised graph (node-link JSON) and degree table.
 */
process BUILD_GRAPH {
    tag "build_graph"
    label "process_medium"

    publishDir "${params.outdir}/graph", mode: 'copy'

    input:
    path nodes_json
    path edges_json

    output:
    path "anchor_graph.json",  emit: graph
    path "degree_table.tsv",   emit: degrees

    script:
    """
    python3 ${projectDir}/bin/build_graph.py \
        --nodes  ${nodes_json}  \
        --edges  ${edges_json}  \
        --out_graph   anchor_graph.json \
        --out_degrees degree_table.tsv
    """
}

/*
 * STEP 3: Core expansion loop.
 * Copies high-connectivity anchors iteratively until no new synthetic
 * nodes form new high-dimensional edges.
 */
process EXPAND_NODES {
    tag "expand"
    label "process_high"

    publishDir "${params.outdir}/expansion", mode: 'copy'

    input:
    path graph_json
    path nodes_json

    output:
    path "expanded_graph.json",    emit: graph
    path "synthetic_nodes.json",   emit: synthetic
    path "groups.json",            emit: groups
    path "expansion_log.tsv",      emit: log

    script:
    """
    python3 ${projectDir}/bin/expand_nodes.py \
        --graph       ${graph_json}   \
        --nodes       ${nodes_json}   \
        --similarity  ${params.similarity} \
        --low_dim     ${params.low_dim}    \
        --max_rounds  ${params.max_rounds} \
        --out_graph     expanded_graph.json  \
        --out_synthetic synthetic_nodes.json \
        --out_groups    groups.json          \
        --out_log       expansion_log.tsv
    """
}

/*
 * STEP 4: Shannon entropy scoring and edge reassignment.
 * For each anchor group, scores co-occurrence affinity and determines
 * whether the high-dim edge should be duplicated to two low-dim nodes.
 */
process SCORE_ENTROPY {
    tag "entropy"
    label "process_medium"

    publishDir "${params.outdir}/entropy", mode: 'copy'

    input:
    path expanded_graph
    path synthetic_nodes
    path groups_json

    output:
    path "entropy_scores.tsv",      emit: scores
    path "reassigned_groups.json",  emit: groups
    path "duplicated_edges.tsv",    emit: duplicates

    script:
    """
    python3 ${projectDir}/bin/score_entropy.py \
        --graph       ${expanded_graph}   \
        --synthetic   ${synthetic_nodes}  \
        --groups      ${groups_json}      \
        --threshold   ${params.entropy_threshold} \
        --out_scores      entropy_scores.tsv    \
        --out_groups      reassigned_groups.json \
        --out_duplicates  duplicated_edges.tsv
    """
}

/*
 * STEP 5: Summarise results and write final output files.
 */
process SUMMARIZE {
    tag "summarize"
    label "process_low"

    publishDir "${params.outdir}", mode: 'copy'

    input:
    path expanded_graph
    path reassigned_groups
    path entropy_scores
    path duplicated_edges
    path expansion_log

    output:
    path "final_nodes.tsv",        emit: nodes
    path "final_edges.tsv",        emit: edges
    path "final_groups.tsv",       emit: groups
    path "summary_report.txt",     emit: report

    script:
    """
    python3 ${projectDir}/bin/summarize.py \
        --graph      ${expanded_graph}     \
        --groups     ${reassigned_groups}  \
        --scores     ${entropy_scores}     \
        --duplicates ${duplicated_edges}   \
        --log        ${expansion_log}      \
        --out_nodes   final_nodes.tsv      \
        --out_edges   final_edges.tsv      \
        --out_groups  final_groups.tsv     \
        --out_report  summary_report.txt
    """
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------
workflow {

    log.info """
    ╔══════════════════════════════════════════════════════╗
    ║        Synthetic Node Expansion Pipeline             ║
    ╚══════════════════════════════════════════════════════╝
    nodes             : ${params.nodes}
    edges             : ${params.edges}
    similarity        : ${params.similarity}
    entropy_threshold : ${params.entropy_threshold}
    low_dim           : ${params.low_dim}
    max_rounds        : ${params.max_rounds}
    outdir            : ${params.outdir}
    profile           : ${workflow.profile}
    """.stripIndent()

    // Input channels
    nodes_ch = Channel.fromPath(params.nodes, checkIfExists: true)
    edges_ch = Channel.fromPath(params.edges, checkIfExists: true)

    // Pipeline
    VALIDATE_INPUT(nodes_ch, edges_ch)

    BUILD_GRAPH(
        VALIDATE_INPUT.out.nodes,
        VALIDATE_INPUT.out.edges
    )

    EXPAND_NODES(
        BUILD_GRAPH.out.graph,
        VALIDATE_INPUT.out.nodes
    )

    SCORE_ENTROPY(
        EXPAND_NODES.out.graph,
        EXPAND_NODES.out.synthetic,
        EXPAND_NODES.out.groups
    )

    SUMMARIZE(
        EXPAND_NODES.out.graph,
        SCORE_ENTROPY.out.groups,
        SCORE_ENTROPY.out.scores,
        SCORE_ENTROPY.out.duplicates,
        EXPAND_NODES.out.log
    )
}
