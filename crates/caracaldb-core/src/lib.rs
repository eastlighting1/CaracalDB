pub mod bundle;
pub mod checkpoint;
pub mod column;
pub mod error;
pub mod exec;
pub mod graph;
pub mod header;
pub mod ir;
pub mod parser;
pub mod storage;
pub mod tx;
pub mod wal;

pub use bundle::{create_bundle, open_bundle, Bundle, Manifest, BUNDLE_DIRS, BUNDLE_SUFFIX};
pub use checkpoint::{checkpoint, reload_manifest, CheckpointResult, CHECKPOINT_KIND};
pub use column::{
    decode_column_segment, read_column_segment_info, read_column_segment_ipc_stream,
    write_column_segment_ipc_stream, ColumnBatchInfo, ColumnSegmentDecoded, ColumnSegmentFooter,
    ColumnSegmentInfo,
};
pub use error::{CaracalError, Result};
pub use exec::{
    execute_to_batches, execution_error, unsupported_shape, BatchSourceOperator, ExecCtx,
    ExpandOperator, FilterOperator, HashAggregateOperator, HashJoinOperator,
    NeighborSampleOperator, NodeScanOperator, PhysicalOperator, ProjectOperator, TopKOperator,
    VarPathOperator,
};
pub use graph::{
    build_csc_arrays, build_csr_arrays, csr_k_hop, csr_neighbor_sample, csr_neighbors_of,
    csr_shortest_path, hnsw_manifest_boundary, read_csr, typed_adjacency, typed_neighbors,
    write_csr, CsrFile, CsrNeighbors, GraphPath, KHopRow, SampleRow, TypedAdjacency,
    CSR_FLAG_HAS_EIDS,
};
pub use ir::{
    logical_plan_to_json, physical_plan_to_json, Direction, Expr, Literal, LogicalPlan, OrderExpr,
    PhysicalPlan,
};
pub use parser::{parse_diagnostic, parse_tuft_subset, Diagnostic, ParsedQuery};
pub use storage::{
    append_edge_batch_ipc_stream, append_node_batch_ipc_stream, list_edge_stores, list_node_stores,
    open_edge_store, open_node_store, record_batches_to_ipc_stream, scan_edge_store_ipc_streams,
    scan_edge_store_summary, scan_node_store_ipc_streams, scan_node_store_summary, AppendResult,
    EdgeStore, EdgeStoreManifest, NodeStore, NodeStoreManifest, StoreScanSummary,
};
pub use tx::{ensure_no_write_conflict, Transaction};
pub use wal::{iter_all_records, iter_segment_records, Wal, WalRecord};
