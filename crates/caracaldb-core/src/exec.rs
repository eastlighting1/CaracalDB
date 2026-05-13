use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take_record_batch;

use crate::error::{CaracalError, Result};
use crate::graph::{csr_k_hop, csr_neighbor_sample, csr_neighbors_of, CsrFile};
use crate::storage::{scan_node_store_ipc_streams, NodeStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecCtx {
    pub snapshot_id: Option<String>,
    pub snapshot_lsn: Option<u64>,
    pub seed: u64,
    pub batch_size: usize,
    pub metadata: BTreeMap<String, String>,
}

impl Default for ExecCtx {
    fn default() -> Self {
        Self {
            snapshot_id: None,
            snapshot_lsn: None,
            seed: 0,
            batch_size: 1024,
            metadata: BTreeMap::new(),
        }
    }
}

pub trait PhysicalOperator {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()>;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
    fn close(&mut self) -> Result<()>;
}

impl<T: PhysicalOperator + ?Sized> PhysicalOperator for Box<T> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.as_mut().open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.as_mut().next_batch()
    }

    fn close(&mut self) -> Result<()> {
        self.as_mut().close()
    }
}

#[derive(Debug, Clone)]
pub struct NodeScanOperator {
    store: NodeStore,
    batches: Vec<RecordBatch>,
    cursor: usize,
    opened: bool,
}

#[derive(Debug, Clone)]
pub struct BatchSourceOperator {
    batches: Vec<RecordBatch>,
    cursor: usize,
    opened: bool,
}

impl BatchSourceOperator {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            cursor: 0,
            opened: false,
        }
    }
}

impl PhysicalOperator for BatchSourceOperator {
    fn open(&mut self, _ctx: &ExecCtx) -> Result<()> {
        self.cursor = 0;
        self.opened = true;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.opened {
            return Err(execution_error("operator must be opened before next_batch"));
        }
        let Some(batch) = self.batches.get(self.cursor).cloned() else {
            return Ok(None);
        };
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn close(&mut self) -> Result<()> {
        self.cursor = 0;
        self.opened = false;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FilterOperator<C: PhysicalOperator> {
    child: C,
    column: String,
    equals_u64: u64,
}

impl<C: PhysicalOperator> FilterOperator<C> {
    pub fn eq_u64(child: C, column: impl Into<String>, value: u64) -> Self {
        Self {
            child,
            column: column.into(),
            equals_u64: value,
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for FilterOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let index = batch
            .schema()
            .index_of(&self.column)
            .map_err(|_| execution_error(format!("filter column not found: {}", self.column)))?;
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| execution_error("Filter currently supports UInt64 equality"))?;
        let mask = BooleanArray::from(
            (0..array.len())
                .map(|row| !array.is_null(row) && array.value(row) == self.equals_u64)
                .collect::<Vec<_>>(),
        );
        Ok(Some(filter_record_batch(&batch, &mask).map_err(|err| {
            execution_error(format!("Filter failed: {err}"))
        })?))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct ProjectOperator<C: PhysicalOperator> {
    child: C,
    columns: Vec<String>,
}

impl<C: PhysicalOperator> ProjectOperator<C> {
    pub fn new(child: C, columns: Vec<String>) -> Self {
        Self { child, columns }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for ProjectOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let indices = self
            .columns
            .iter()
            .map(|name| {
                batch
                    .schema()
                    .index_of(name)
                    .map_err(|err| execution_error(format!("Project failed: {err}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(batch.project(&indices).map_err(|err| {
            execution_error(format!("Project failed: {err}"))
        })?))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct TopKOperator<C: PhysicalOperator> {
    child: C,
    order_by: String,
    skip: usize,
    limit: Option<usize>,
}

impl<C: PhysicalOperator> TopKOperator<C> {
    pub fn new(child: C, order_by: impl Into<String>, skip: usize, limit: Option<usize>) -> Self {
        Self {
            child,
            order_by: order_by.into(),
            skip,
            limit,
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for TopKOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let index = batch
            .schema()
            .index_of(&self.order_by)
            .map_err(|_| execution_error(format!("TopK column not found: {}", self.order_by)))?;
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| execution_error("TopK currently supports UInt64 ordering"))?;
        let mut rows = (0..batch.num_rows()).collect::<Vec<_>>();
        rows.sort_by_key(|row| {
            if array.is_null(*row) {
                u64::MAX
            } else {
                array.value(*row)
            }
        });
        let end = self
            .limit
            .map(|limit| self.skip.saturating_add(limit))
            .unwrap_or(rows.len())
            .min(rows.len());
        let indices = UInt64Array::from(
            rows[self.skip.min(rows.len())..end]
                .iter()
                .map(|row| *row as u64)
                .collect::<Vec<_>>(),
        );
        Ok(Some(take_record_batch(&batch, &indices).map_err(
            |err| execution_error(format!("TopK failed: {err}")),
        )?))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct ExpandOperator<C: PhysicalOperator> {
    child: C,
    csr: CsrFile,
    source_column: String,
}

impl<C: PhysicalOperator> ExpandOperator<C> {
    pub fn new(child: C, csr: CsrFile, source_column: impl Into<String>) -> Self {
        Self {
            child,
            csr,
            source_column: source_column.into(),
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for ExpandOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let index = batch
            .schema()
            .index_of(&self.source_column)
            .map_err(|_| execution_error("Expand source column not found"))?;
        let sources = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| execution_error("Expand source column must be UInt64"))?;
        let mut src_out = Vec::new();
        let mut dst_out = Vec::new();
        for row in 0..sources.len() {
            if sources.is_null(row) {
                continue;
            }
            let source = sources.value(row);
            for dst in csr_neighbors_of(&self.csr, source)?.neighbors {
                src_out.push(source);
                dst_out.push(dst);
            }
        }
        Ok(Some(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("src", DataType::UInt64, false),
                    Field::new("dst", DataType::UInt64, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from(src_out)),
                    Arc::new(UInt64Array::from(dst_out)),
                ],
            )
            .map_err(|err| execution_error(format!("Expand failed: {err}")))?,
        ))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct NeighborSampleOperator<C: PhysicalOperator> {
    child: C,
    csr: CsrFile,
    source_column: String,
    fanout: Option<usize>,
    with_replacement: bool,
}

impl<C: PhysicalOperator> NeighborSampleOperator<C> {
    pub fn new(
        child: C,
        csr: CsrFile,
        source_column: impl Into<String>,
        fanout: Option<usize>,
        with_replacement: bool,
    ) -> Self {
        Self {
            child,
            csr,
            source_column: source_column.into(),
            fanout,
            with_replacement,
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for NeighborSampleOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let seeds = collect_u64_column(&batch, &self.source_column, "NeighborSample")?;
        let rows = csr_neighbor_sample(&self.csr, &seeds, self.fanout, self.with_replacement)?;
        let src = rows.iter().map(|row| row.src).collect::<Vec<_>>();
        let dst = rows.iter().map(|row| row.dst).collect::<Vec<_>>();
        let eid = rows.iter().map(|row| row.eid).collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("eid", DataType::UInt64, true),
            ])),
            vec![
                Arc::new(UInt64Array::from(src)),
                Arc::new(UInt64Array::from(dst)),
                Arc::new(UInt64Array::from(eid)),
            ],
        )
        .map(Some)
        .map_err(|err| execution_error(format!("NeighborSample failed: {err}")))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct VarPathOperator<C: PhysicalOperator> {
    child: C,
    csr: CsrFile,
    source_column: String,
    min_depth: u32,
    max_depth: u32,
}

impl<C: PhysicalOperator> VarPathOperator<C> {
    pub fn new(
        child: C,
        csr: CsrFile,
        source_column: impl Into<String>,
        min_depth: u32,
        max_depth: u32,
    ) -> Self {
        Self {
            child,
            csr,
            source_column: source_column.into(),
            min_depth,
            max_depth,
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for VarPathOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let seeds = collect_u64_column(&batch, &self.source_column, "VarPath")?;
        let rows = csr_k_hop(&self.csr, &seeds, self.min_depth, self.max_depth)?;
        let seed = rows.iter().map(|row| row.seed).collect::<Vec<_>>();
        let node = rows.iter().map(|row| row.node).collect::<Vec<_>>();
        let depth = rows.iter().map(|row| row.depth).collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("seed", DataType::UInt64, false),
                Field::new("node", DataType::UInt64, false),
                Field::new("depth", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(seed)),
                Arc::new(UInt64Array::from(node)),
                Arc::new(UInt32Array::from(depth)),
            ],
        )
        .map(Some)
        .map_err(|err| execution_error(format!("VarPath failed: {err}")))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

#[derive(Debug)]
pub struct HashJoinOperator<L: PhysicalOperator, R: PhysicalOperator> {
    left: L,
    right: R,
    left_key: String,
    right_key: String,
    emitted: bool,
}

impl<L: PhysicalOperator, R: PhysicalOperator> HashJoinOperator<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self::on(left, right, "key", "key")
    }

    pub fn on(
        left: L,
        right: R,
        left_key: impl Into<String>,
        right_key: impl Into<String>,
    ) -> Self {
        Self {
            left,
            right,
            left_key: left_key.into(),
            right_key: right_key.into(),
            emitted: false,
        }
    }
}

impl<L: PhysicalOperator, R: PhysicalOperator> PhysicalOperator for HashJoinOperator<L, R> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.left.open(ctx)?;
        self.right.open(ctx)?;
        self.emitted = false;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        let right_batches = drain_operator(&mut self.right)?;
        let mut right_by_key: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        let mut right_row = 0u64;
        for batch in &right_batches {
            let keys = u64_column(batch, &self.right_key, "HashJoin right")?;
            for row in 0..batch.num_rows() {
                if !keys.is_null(row) {
                    right_by_key
                        .entry(keys.value(row))
                        .or_default()
                        .push(right_row);
                }
                right_row += 1;
            }
        }

        let left_batches = drain_operator(&mut self.left)?;
        let mut left_take = Vec::new();
        let mut right_take = Vec::new();
        let mut left_row = 0u64;
        for batch in &left_batches {
            let keys = u64_column(batch, &self.left_key, "HashJoin left")?;
            for row in 0..batch.num_rows() {
                if !keys.is_null(row) {
                    let key = keys.value(row);
                    if let Some(matches) = right_by_key.get(&key) {
                        for matched_right_row in matches {
                            left_take.push(left_row);
                            right_take.push(*matched_right_row);
                        }
                    }
                }
                left_row += 1;
            }
        }

        join_batches(&left_batches, &right_batches, &left_take, &right_take)
            .map(Some)
            .map_err(|err| execution_error(format!("HashJoin failed: {err}")))
    }

    fn close(&mut self) -> Result<()> {
        self.left.close()?;
        self.right.close()
    }
}

#[derive(Debug)]
pub struct HashAggregateOperator<C: PhysicalOperator> {
    child: C,
    emitted: bool,
}

impl<C: PhysicalOperator> HashAggregateOperator<C> {
    pub fn count(child: C) -> Self {
        Self {
            child,
            emitted: false,
        }
    }
}

impl<C: PhysicalOperator> PhysicalOperator for HashAggregateOperator<C> {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.child.open(ctx)?;
        self.emitted = false;
        Ok(())
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let mut count = 0u64;
        while let Some(batch) = self.child.next_batch()? {
            count += batch.num_rows() as u64;
        }
        Ok(Some(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "count",
                    DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(UInt64Array::from(vec![count]))],
            )
            .map_err(|err| execution_error(format!("HashAggregate failed: {err}")))?,
        ))
    }
    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

pub fn execute_to_batches<O: PhysicalOperator>(
    operator: &mut O,
    ctx: &ExecCtx,
) -> Result<Vec<RecordBatch>> {
    operator.open(ctx)?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch()? {
        batches.push(batch);
    }
    operator.close()?;
    Ok(batches)
}

fn drain_operator<O: PhysicalOperator>(operator: &mut O) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch()? {
        batches.push(batch);
    }
    Ok(batches)
}

fn collect_u64_column(batch: &RecordBatch, column: &str, operator: &str) -> Result<Vec<u64>> {
    let values = u64_column(batch, column, operator)?;
    let mut out = Vec::new();
    for row in 0..values.len() {
        if !values.is_null(row) {
            out.push(values.value(row));
        }
    }
    Ok(out)
}

fn u64_column<'a>(batch: &'a RecordBatch, column: &str, operator: &str) -> Result<&'a UInt64Array> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| execution_error(format!("{operator} column not found: {column}")))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| execution_error(format!("{operator} column must be UInt64: {column}")))
}

fn join_batches(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_take: &[u64],
    right_take: &[u64],
) -> std::result::Result<RecordBatch, String> {
    let Some(left_schema) = left_batches.first().map(|batch| batch.schema()) else {
        return RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("left_row", DataType::UInt64, false),
                Field::new("right_row", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
            ],
        )
        .map_err(|err| err.to_string());
    };
    let Some(right_schema) = right_batches.first().map(|batch| batch.schema()) else {
        return RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("left_row", DataType::UInt64, false),
                Field::new("right_row", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
            ],
        )
        .map_err(|err| err.to_string());
    };

    let left_all = concat_batches(&left_schema, left_batches).map_err(|err| err.to_string())?;
    let right_all = concat_batches(&right_schema, right_batches).map_err(|err| err.to_string())?;
    let left_indices = UInt64Array::from(left_take.to_vec());
    let right_indices = UInt64Array::from(right_take.to_vec());
    let left_joined = take_record_batch(&left_all, &left_indices).map_err(|err| err.to_string())?;
    let right_joined =
        take_record_batch(&right_all, &right_indices).map_err(|err| err.to_string())?;

    let mut names = left_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut fields = left_schema.fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = left_joined.columns().to_vec();

    for (field, column) in right_schema.fields().iter().zip(right_joined.columns()) {
        let mut name = field.name().clone();
        if names.contains(&name) {
            name = format!("right.{name}");
        }
        names.insert(name.clone());
        fields.push(Arc::new(Field::new(
            name,
            field.data_type().clone(),
            field.is_nullable(),
        )));
        columns.push(Arc::clone(column) as ArrayRef);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| err.to_string())
}

impl NodeScanOperator {
    pub fn new(store: NodeStore) -> Self {
        Self {
            store,
            batches: Vec::new(),
            cursor: 0,
            opened: false,
        }
    }
}

impl PhysicalOperator for NodeScanOperator {
    fn open(&mut self, ctx: &ExecCtx) -> Result<()> {
        self.batches.clear();
        self.cursor = 0;
        for stream in scan_node_store_ipc_streams(&self.store, ctx.snapshot_lsn)? {
            let reader = StreamReader::try_new(Cursor::new(stream), None).map_err(|err| {
                CaracalError::new("CDB-6001", format!("invalid NodeScan Arrow stream: {err}"))
            })?;
            for batch in reader {
                self.batches.push(batch.map_err(|err| {
                    CaracalError::new("CDB-6001", format!("invalid NodeScan batch: {err}"))
                })?);
            }
        }
        self.opened = true;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.opened {
            return Err(execution_error("operator must be opened before next_batch"));
        }
        let Some(batch) = self.batches.get(self.cursor).cloned() else {
            return Ok(None);
        };
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn close(&mut self) -> Result<()> {
        self.batches.clear();
        self.cursor = 0;
        self.opened = false;
        Ok(())
    }
}

pub fn execution_error(message: impl Into<String>) -> CaracalError {
    CaracalError::new("CDB-6001", message)
}

pub fn unsupported_shape(message: impl Into<String>) -> CaracalError {
    CaracalError::new("CDB-6020", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_errors_use_execution_code_family() {
        assert_eq!(execution_error("boom").code, "CDB-6001");
        assert_eq!(unsupported_shape("later").code, "CDB-6020");
    }

    #[test]
    fn exec_ctx_carries_snapshot_and_batch_metadata() {
        let mut ctx = ExecCtx {
            snapshot_id: Some("snapshot:v1@7".to_string()),
            snapshot_lsn: Some(7),
            batch_size: 64,
            ..ExecCtx::default()
        };
        ctx.metadata
            .insert("logical".to_string(), "scan".to_string());
        assert_eq!(ctx.snapshot_lsn, Some(7));
        assert_eq!(ctx.batch_size, 64);
        assert_eq!(ctx.metadata["logical"], "scan");
    }

    #[test]
    fn node_scan_requires_open_before_next_batch() {
        let store = NodeStore {
            root: std::path::PathBuf::from("unused"),
            manifest: crate::storage::NodeStoreManifest {
                class_iri: "http://example.org/Gene".to_string(),
                local_name: "Gene".to_string(),
                schema_json: String::new(),
                next_nid: 0,
                chunks: Vec::new(),
            },
        };
        let mut op = NodeScanOperator::new(store);
        let err = op.next_batch().expect_err("must open");
        assert_eq!(err.code, "CDB-6001");
    }

    #[test]
    fn core_exec_operators_return_arrow_batches_and_basic_semantics() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("nid", DataType::UInt64, true),
                Field::new("score", DataType::UInt64, true),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![Some(0), Some(1), Some(2), None])),
                Arc::new(UInt64Array::from(vec![
                    Some(30),
                    Some(10),
                    Some(20),
                    Some(40),
                ])),
            ],
        )
        .expect("batch");

        let mut filter =
            FilterOperator::eq_u64(BatchSourceOperator::new(vec![batch.clone()]), "nid", 1);
        let filtered = execute_to_batches(&mut filter, &ExecCtx::default()).expect("filter");
        assert_eq!(filtered[0].num_rows(), 1);

        let mut project = ProjectOperator::new(
            BatchSourceOperator::new(vec![batch.clone()]),
            vec!["score".to_string()],
        );
        let projected = execute_to_batches(&mut project, &ExecCtx::default()).expect("project");
        assert_eq!(projected[0].schema().fields().len(), 1);

        let mut topk = TopKOperator::new(
            BatchSourceOperator::new(vec![batch.clone()]),
            "score",
            1,
            Some(2),
        );
        let ordered = execute_to_batches(&mut topk, &ExecCtx::default()).expect("topk");
        assert_eq!(
            ordered[0]
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("score")
                .values(),
            &[20, 30]
        );

        let csr = crate::graph::build_csr_arrays(&[0, 0, 1], &[1, 2, 2], 3, None).expect("csr");
        let mut expand = ExpandOperator::new(
            BatchSourceOperator::new(vec![batch.clone()]),
            csr.clone(),
            "nid",
        );
        let expanded = execute_to_batches(&mut expand, &ExecCtx::default()).expect("expand");
        assert_eq!(expanded[0].num_rows(), 3);

        let mut sample = NeighborSampleOperator::new(
            BatchSourceOperator::new(vec![batch.clone()]),
            csr.clone(),
            "nid",
            Some(1),
            false,
        );
        let sampled = execute_to_batches(&mut sample, &ExecCtx::default()).expect("sample");
        assert_eq!(
            sampled[0]
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("dst")
                .values(),
            &[1, 2]
        );

        let mut var_path =
            VarPathOperator::new(BatchSourceOperator::new(vec![batch]), csr, "nid", 1, 2);
        let var_paths = execute_to_batches(&mut var_path, &ExecCtx::default()).expect("varpath");
        assert_eq!(var_paths[0].num_rows(), 4);
        assert_eq!(
            var_paths[0]
                .column(2)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("depth")
                .values(),
            &[1, 1, 2, 1]
        );

        let mut aggregate = HashAggregateOperator::count(BatchSourceOperator::new(vec![
            expanded[0].clone(),
            sampled[0].clone(),
        ]));
        let counted = execute_to_batches(&mut aggregate, &ExecCtx::default()).expect("aggregate");
        assert_eq!(
            counted[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("count")
                .value(0),
            5
        );

        let left = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("lhs", DataType::UInt64, true)])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(10),
                Some(20),
                Some(10),
                None,
            ]))],
        )
        .expect("left");
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("rhs", DataType::UInt64, true)])),
            vec![Arc::new(UInt64Array::from(vec![Some(10), Some(30)]))],
        )
        .expect("right");
        let mut join = HashJoinOperator::on(
            BatchSourceOperator::new(vec![left]),
            BatchSourceOperator::new(vec![right]),
            "lhs",
            "rhs",
        );
        let joined = execute_to_batches(&mut join, &ExecCtx::default()).expect("join");
        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("key")
                .values(),
            &[10, 10]
        );
    }
}
