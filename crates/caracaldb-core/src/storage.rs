use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use serde::{Deserialize, Serialize};

use crate::bundle::Bundle;
use crate::column::{
    decode_column_segment, read_column_segment_ipc_stream, write_column_segment_ipc_stream,
};
use crate::error::{CaracalError, Result};

pub const NODE_MANIFEST_NAME: &str = "_manifest.json";
pub const EDGE_MANIFEST_NAME: &str = "_manifest.json";
pub const CHUNKS_DIRNAME: &str = "chunks";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeChunkRef {
    pub path: String,
    pub row_count: u64,
    pub start_nid: u64,
    pub end_nid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStoreManifest {
    pub class_iri: String,
    pub local_name: String,
    #[serde(rename = "schema")]
    pub schema_json: String,
    #[serde(default)]
    pub next_nid: u64,
    #[serde(default)]
    pub chunks: Vec<NodeChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStore {
    pub root: PathBuf,
    pub manifest: NodeStoreManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeChunkRef {
    pub path: String,
    pub row_count: u64,
    pub start_eid: u64,
    pub end_eid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeStoreManifest {
    pub property_iri: String,
    pub local_name: String,
    pub src_class_iri: Option<String>,
    pub dst_class_iri: Option<String>,
    #[serde(default)]
    pub next_eid: u64,
    #[serde(default)]
    pub chunks: Vec<EdgeChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeStore {
    pub root: PathBuf,
    pub manifest: EdgeStoreManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreScanSummary {
    pub chunk_count: usize,
    pub batch_count: usize,
    pub row_count: usize,
    pub field_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub path: String,
    pub row_count: u64,
    pub start_id: u64,
    pub end_id: u64,
}

pub fn open_node_store(
    bundle: &Bundle,
    class_iri: &str,
    local_name: &str,
    create: bool,
) -> Result<NodeStore> {
    assert_local_name(local_name, "CDB-7010", "class")?;
    let root = bundle.child(&["nodes", local_name]);
    let manifest_file = root.join(NODE_MANIFEST_NAME);
    if manifest_file.is_file() {
        let manifest = load_node_manifest(&root)?;
        if manifest.class_iri != class_iri {
            return Err(CaracalError::new(
                "CDB-7013",
                format!(
                    "node store {local_name:?} class mismatch: expected {class_iri}, found {}",
                    manifest.class_iri
                ),
            ));
        }
        return Ok(NodeStore { root, manifest });
    }
    if !create {
        return Err(CaracalError::with_hint(
            "CDB-7012",
            format!("node store not found for class {class_iri:?}"),
            "pass create=True to initialise a fresh node store",
        ));
    }
    fs::create_dir_all(root.join(CHUNKS_DIRNAME))?;
    let manifest = NodeStoreManifest {
        class_iri: class_iri.to_string(),
        local_name: local_name.to_string(),
        schema_json: String::new(),
        next_nid: 0,
        chunks: Vec::new(),
    };
    save_node_manifest(&root, &manifest)?;
    Ok(NodeStore { root, manifest })
}

pub fn open_edge_store(
    bundle: &Bundle,
    property_iri: &str,
    local_name: &str,
    src_class_iri: Option<&str>,
    dst_class_iri: Option<&str>,
    create: bool,
) -> Result<EdgeStore> {
    assert_local_name(local_name, "CDB-7020", "property")?;
    let root = bundle.child(&["edges", local_name]);
    let manifest_file = root.join(EDGE_MANIFEST_NAME);
    if manifest_file.is_file() {
        let manifest = load_edge_manifest(&root)?;
        if manifest.property_iri != property_iri {
            return Err(CaracalError::new(
                "CDB-7023",
                format!(
                    "edge store {local_name:?} property mismatch: expected {property_iri}, found {}",
                    manifest.property_iri
                ),
            ));
        }
        return Ok(EdgeStore { root, manifest });
    }
    if !create {
        return Err(CaracalError::with_hint(
            "CDB-7022",
            format!("edge store not found for property {property_iri:?}"),
            "pass create=True to initialise a fresh edge store",
        ));
    }
    fs::create_dir_all(root.join(CHUNKS_DIRNAME))?;
    let manifest = EdgeStoreManifest {
        property_iri: property_iri.to_string(),
        local_name: local_name.to_string(),
        src_class_iri: src_class_iri.map(str::to_string),
        dst_class_iri: dst_class_iri.map(str::to_string),
        next_eid: 0,
        chunks: Vec::new(),
    };
    save_edge_manifest(&root, &manifest)?;
    Ok(EdgeStore { root, manifest })
}

pub fn list_node_stores(bundle: &Bundle) -> Result<Vec<String>> {
    list_store_dirs(&bundle.child(&["nodes"]))
}

pub fn list_edge_stores(bundle: &Bundle) -> Result<Vec<String>> {
    list_store_dirs(&bundle.child(&["edges"]))
}

pub fn scan_node_store_summary(store: &NodeStore) -> Result<StoreScanSummary> {
    scan_store_chunks(
        &store.root,
        store
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.path.as_str()),
    )
}

pub fn scan_edge_store_summary(store: &EdgeStore) -> Result<StoreScanSummary> {
    scan_store_chunks(
        &store.root,
        store
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.path.as_str()),
    )
}

pub fn scan_node_store_ipc_streams(
    store: &NodeStore,
    snapshot_lsn: Option<u64>,
) -> Result<Vec<Vec<u8>>> {
    scan_store_ipc_streams(
        &store.root,
        store
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.path.as_str()),
        snapshot_lsn,
    )
}

pub fn scan_edge_store_ipc_streams(
    store: &EdgeStore,
    snapshot_lsn: Option<u64>,
) -> Result<Vec<Vec<u8>>> {
    scan_store_ipc_streams(
        &store.root,
        store
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.path.as_str()),
        snapshot_lsn,
    )
}

pub fn append_node_batch_ipc_stream(
    store: &mut NodeStore,
    ipc_stream: &[u8],
    created_lsn: u64,
) -> Result<AppendResult> {
    let input = read_ipc_batches(ipc_stream)?;
    validate_node_input(&input.schema, input.row_count)?;
    if let Some(existing) = first_chunk_schema(
        &store.root,
        store.manifest.chunks.first().map(|c| c.path.as_str()),
    )? {
        let expected = schema_without_field(&existing, "nid");
        ensure_public_schema_compatible(&expected, &input.schema, "CDB-7011", "node batch")?;
    }

    let start_id = store.manifest.next_nid;
    let batches = add_store_columns(&input.batches, "nid", start_id, created_lsn)?;
    let row_count = input.row_count as u64;
    let end_id = start_id + row_count;
    let chunk_path = format!("{CHUNKS_DIRNAME}/{:08}.col", store.manifest.chunks.len());
    let target = store.root.join(&chunk_path);
    let output_stream = record_batches_to_ipc_stream(&batches)?;
    let footer = write_column_segment_ipc_stream(target, &output_stream, "none")?;

    store.manifest.chunks.push(NodeChunkRef {
        path: chunk_path.clone(),
        row_count: footer.footer.row_count,
        start_nid: start_id,
        end_nid: end_id,
    });
    store.manifest.next_nid = end_id;
    if store.manifest.schema_json.is_empty() {
        store.manifest.schema_json = schema_to_manifest_json(batches[0].schema().as_ref())?;
    }
    save_node_manifest(&store.root, &store.manifest)?;
    Ok(AppendResult {
        path: chunk_path,
        row_count,
        start_id,
        end_id,
    })
}

pub fn append_edge_batch_ipc_stream(
    store: &mut EdgeStore,
    ipc_stream: &[u8],
    created_lsn: u64,
) -> Result<AppendResult> {
    let input = read_ipc_batches(ipc_stream)?;
    validate_edge_input(&input.schema, input.row_count)?;
    if let Some(existing) = first_chunk_schema(
        &store.root,
        store.manifest.chunks.first().map(|c| c.path.as_str()),
    )? {
        let expected = schema_without_field(&existing, "eid");
        ensure_public_schema_compatible(&expected, &input.schema, "CDB-7021", "edge batch")?;
    }

    let start_id = store.manifest.next_eid;
    let batches = add_store_columns(&input.batches, "eid", start_id, created_lsn)?;
    let row_count = input.row_count as u64;
    let end_id = start_id + row_count;
    let chunk_path = format!("{CHUNKS_DIRNAME}/{:08}.col", store.manifest.chunks.len());
    let target = store.root.join(&chunk_path);
    let output_stream = record_batches_to_ipc_stream(&batches)?;
    let footer = write_column_segment_ipc_stream(target, &output_stream, "none")?;

    store.manifest.chunks.push(EdgeChunkRef {
        path: chunk_path.clone(),
        row_count: footer.footer.row_count,
        start_eid: start_id,
        end_eid: end_id,
    });
    store.manifest.next_eid = end_id;
    save_edge_manifest(&store.root, &store.manifest)?;
    Ok(AppendResult {
        path: chunk_path,
        row_count,
        start_id,
        end_id,
    })
}

pub fn load_node_manifest(root: &Path) -> Result<NodeStoreManifest> {
    let target = root.join(NODE_MANIFEST_NAME);
    if !target.is_file() {
        return Err(CaracalError::new(
            "CDB-7012",
            format!("node store manifest missing: {}", target.display()),
        ));
    }
    Ok(serde_json::from_str(&fs::read_to_string(target)?)?)
}

pub fn save_node_manifest(root: &Path, manifest: &NodeStoreManifest) -> Result<()> {
    write_manifest_atomic(root, NODE_MANIFEST_NAME, manifest)
}

pub fn load_edge_manifest(root: &Path) -> Result<EdgeStoreManifest> {
    let target = root.join(EDGE_MANIFEST_NAME);
    if !target.is_file() {
        return Err(CaracalError::new(
            "CDB-7022",
            format!("edge store manifest missing: {}", target.display()),
        ));
    }
    Ok(serde_json::from_str(&fs::read_to_string(target)?)?)
}

pub fn save_edge_manifest(root: &Path, manifest: &EdgeStoreManifest) -> Result<()> {
    write_manifest_atomic(root, EDGE_MANIFEST_NAME, manifest)
}

fn write_manifest_atomic<T: Serialize>(root: &Path, name: &str, manifest: &T) -> Result<()> {
    fs::create_dir_all(root)?;
    let target = root.join(name);
    let tmp = target.with_file_name(format!("{name}.tmp"));
    let text = serde_json::to_string_pretty(manifest)? + "\n";
    fs::write(&tmp, text)?;
    fs::rename(tmp, target)?;
    Ok(())
}

fn list_store_dirs(root: &Path) -> Result<Vec<String>> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            out.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    out.sort();
    Ok(out)
}

fn scan_store_chunks<'a>(
    root: &Path,
    chunks: impl Iterator<Item = &'a str>,
) -> Result<StoreScanSummary> {
    let mut chunk_count = 0usize;
    let mut batch_count = 0usize;
    let mut row_count = 0usize;
    let mut field_names = Vec::new();
    for chunk_path in chunks {
        let decoded = decode_column_segment(root.join(chunk_path))?;
        if field_names.is_empty() {
            field_names = decoded.field_names;
        } else if field_names != decoded.field_names {
            return Err(CaracalError::new(
                "CDB-7001",
                "column segment schema drift while scanning store",
            ));
        }
        chunk_count += 1;
        batch_count += decoded.batches.len();
        row_count += decoded
            .batches
            .iter()
            .map(|batch| batch.row_count)
            .sum::<usize>();
    }
    Ok(StoreScanSummary {
        chunk_count,
        batch_count,
        row_count,
        field_names,
    })
}

fn scan_store_ipc_streams<'a>(
    root: &Path,
    chunks: impl Iterator<Item = &'a str>,
    snapshot_lsn: Option<u64>,
) -> Result<Vec<Vec<u8>>> {
    chunks
        .map(|chunk_path| {
            let stream = read_column_segment_ipc_stream(root.join(chunk_path))?;
            match snapshot_lsn {
                Some(lsn) => filter_ipc_stream_visible(&stream, lsn),
                None => Ok(stream),
            }
        })
        .collect()
}

struct IpcBatchInput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

fn read_ipc_batches(ipc_stream: &[u8]) -> Result<IpcBatchInput> {
    let reader = StreamReader::try_new(Cursor::new(ipc_stream.to_vec()), None)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC stream: {err}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    let mut row_count = 0usize;
    for batch in reader {
        let batch = batch.map_err(|err| {
            CaracalError::new("CDB-7001", format!("invalid Arrow IPC batch: {err}"))
        })?;
        row_count += batch.num_rows();
        batches.push(batch);
    }
    Ok(IpcBatchInput {
        schema,
        batches,
        row_count,
    })
}

fn validate_node_input(schema: &SchemaRef, row_count: usize) -> Result<()> {
    if row_count == 0 {
        return Err(CaracalError::new(
            "CDB-7011",
            "cannot append empty node batch",
        ));
    }
    reject_field(
        schema,
        "nid",
        "CDB-7011",
        "node batches must not include a 'nid' column; it is assigned by the store",
    )?;
    reject_field(
        schema,
        "_created_lsn",
        "CDB-7011",
        "node batches must not include reserved column '_created_lsn'",
    )?;
    reject_field(
        schema,
        "_deleted_lsn",
        "CDB-7011",
        "node batches must not include reserved column '_deleted_lsn'",
    )?;
    Ok(())
}

fn validate_edge_input(schema: &SchemaRef, row_count: usize) -> Result<()> {
    if row_count == 0 {
        return Err(CaracalError::new(
            "CDB-7021",
            "cannot append empty edge batch",
        ));
    }
    reject_field(
        schema,
        "eid",
        "CDB-7021",
        "edge batches must not include an 'eid' column; it is assigned by the store",
    )?;
    reject_field(
        schema,
        "_created_lsn",
        "CDB-7021",
        "edge batches must not include reserved column '_created_lsn'",
    )?;
    reject_field(
        schema,
        "_deleted_lsn",
        "CDB-7021",
        "edge batches must not include reserved column '_deleted_lsn'",
    )?;
    for name in ["src", "dst"] {
        let field = schema.field_with_name(name).map_err(|_| {
            CaracalError::new(
                "CDB-7021",
                format!("edge batch is missing required column {name:?}"),
            )
        })?;
        if field.data_type() != &DataType::UInt64 {
            return Err(CaracalError::new(
                "CDB-7021",
                "edge 'src' and 'dst' must be UInt64 (nid) columns",
            ));
        }
    }
    Ok(())
}

fn reject_field(
    schema: &SchemaRef,
    name: &str,
    code: &'static str,
    message: &'static str,
) -> Result<()> {
    if schema.index_of(name).is_ok() {
        return Err(CaracalError::new(code, message));
    }
    Ok(())
}

fn add_store_columns(
    batches: &[RecordBatch],
    id_column: &str,
    start_id: u64,
    created_lsn: u64,
) -> Result<Vec<RecordBatch>> {
    let mut next_id = start_id;
    batches
        .iter()
        .map(|batch| {
            let row_count = batch.num_rows();
            let ids = UInt64Array::from((next_id..next_id + row_count as u64).collect::<Vec<_>>());
            next_id += row_count as u64;
            let created = UInt64Array::from(vec![created_lsn; row_count]);
            let deleted = UInt64Array::from(vec![None; row_count]);

            let mut fields = Vec::with_capacity(batch.num_columns() + 3);
            fields.push(Field::new(id_column, DataType::UInt64, false));
            fields.extend(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone()),
            );
            fields.push(Field::new("_created_lsn", DataType::UInt64, false));
            fields.push(Field::new("_deleted_lsn", DataType::UInt64, true));

            let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 3);
            columns.push(Arc::new(ids));
            columns.extend(batch.columns().iter().cloned());
            columns.push(Arc::new(created));
            columns.push(Arc::new(deleted));

            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
                CaracalError::new(
                    "CDB-7001",
                    format!("record batch construction failed: {err}"),
                )
            })
        })
        .collect()
}

pub fn record_batches_to_ipc_stream(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Err(CaracalError::new(
            "CDB-7001",
            "cannot write an empty Arrow IPC stream",
        ));
    };
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &first.schema())
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC schema: {err}")))?;
    for batch in batches {
        writer.write(batch).map_err(|err| {
            CaracalError::new("CDB-7001", format!("Arrow IPC write failed: {err}"))
        })?;
    }
    writer
        .finish()
        .map_err(|err| CaracalError::new("CDB-7001", format!("Arrow IPC write failed: {err}")))?;
    drop(writer);
    Ok(out)
}

fn first_chunk_schema(root: &Path, chunk_path: Option<&str>) -> Result<Option<SchemaRef>> {
    let Some(chunk_path) = chunk_path else {
        return Ok(None);
    };
    let stream = read_column_segment_ipc_stream(root.join(chunk_path))?;
    let reader = StreamReader::try_new(Cursor::new(stream), None)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC stream: {err}")))?;
    Ok(Some(reader.schema()))
}

fn ensure_public_schema_compatible(
    expected: &SchemaRef,
    actual: &SchemaRef,
    code: &'static str,
    label: &str,
) -> Result<()> {
    let expected_fields = public_fields(expected);
    let actual_fields = public_fields(actual);
    let expected_names = expected_fields
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();
    let actual_names = actual_fields
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();
    if expected_names != actual_names {
        return Err(CaracalError::new(
            code,
            format!("{label} column drift: expected {expected_names:?}, got {actual_names:?}"),
        ));
    }
    for (expected, actual) in expected_fields.iter().zip(actual_fields.iter()) {
        if expected.data_type() != actual.data_type() {
            return Err(CaracalError::new(
                code,
                format!(
                    "{label} column {:?} type mismatch: {} vs {}",
                    expected.name(),
                    expected.data_type(),
                    actual.data_type()
                ),
            ));
        }
    }
    Ok(())
}

fn public_fields(schema: &SchemaRef) -> Vec<&Field> {
    schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .filter(|field| !matches!(field.name().as_str(), "_created_lsn" | "_deleted_lsn"))
        .collect()
}

fn schema_without_field(schema: &SchemaRef, removed: &str) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref())
            .filter(|field| field.name() != removed)
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

fn schema_to_manifest_json(schema: &Schema) -> Result<String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            serde_json::json!({
                "name": field.name(),
                "type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::to_string(&fields)?)
}

fn filter_ipc_stream_visible(stream: &[u8], snapshot_lsn: u64) -> Result<Vec<u8>> {
    let reader = StreamReader::try_new(Cursor::new(stream.to_vec()), None)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC stream: {err}")))?;
    let schema = reader.schema();
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &schema)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC schema: {err}")))?;
    for batch in reader {
        let batch = batch.map_err(|err| {
            CaracalError::new("CDB-7001", format!("invalid Arrow IPC batch: {err}"))
        })?;
        let filtered = filter_record_batch_visible(&batch, snapshot_lsn)?;
        writer.write(&filtered).map_err(|err| {
            CaracalError::new("CDB-7001", format!("Arrow IPC write failed: {err}"))
        })?;
    }
    writer
        .finish()
        .map_err(|err| CaracalError::new("CDB-7001", format!("Arrow IPC write failed: {err}")))?;
    drop(writer);
    Ok(out)
}

fn filter_record_batch_visible(batch: &RecordBatch, snapshot_lsn: u64) -> Result<RecordBatch> {
    let schema = batch.schema();
    let Ok(created_index) = schema.index_of("_created_lsn") else {
        return Ok(batch.clone());
    };
    let created = batch
        .column(created_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CaracalError::new("CDB-7001", "_created_lsn must be UInt64"))?;
    let deleted = match schema.index_of("_deleted_lsn") {
        Ok(index) => Some(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| CaracalError::new("CDB-7001", "_deleted_lsn must be UInt64"))?,
        ),
        Err(_) => None,
    };
    let mut visible = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let created_ok = !created.is_null(row) && created.value(row) <= snapshot_lsn;
        let deleted_ok =
            deleted.is_none_or(|array| array.is_null(row) || array.value(row) > snapshot_lsn);
        visible.push(created_ok && deleted_ok);
    }
    let mask = BooleanArray::from(visible);
    filter_record_batch(batch, &mask).map_err(|err| {
        CaracalError::new("CDB-7001", format!("Arrow visibility filter failed: {err}"))
    })
}

fn assert_local_name(name: &str, code: &'static str, label: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(invalid_local_name(code, label, name));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(invalid_local_name(code, label, name));
    }
    if !chars.all(|ch| ch == '_' || ch == '-' || ch.is_ascii_alphanumeric()) {
        return Err(invalid_local_name(code, label, name));
    }
    Ok(())
}

fn invalid_local_name(code: &'static str, label: &str, name: &str) -> CaracalError {
    CaracalError::with_hint(
        code,
        format!("invalid {label} local name: {name:?}"),
        "local names must match [A-Za-z_][A-Za-z0-9_-]*",
    )
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::bundle::create_bundle;

    use super::*;

    #[test]
    fn node_store_manifest_create_open_and_list_round_trips() {
        let bundle = temp_bundle("node");
        let store = open_node_store(&bundle, "http://example.org/Gene", "Gene", true)
            .expect("create node store");
        assert_eq!(store.manifest.next_nid, 0);
        assert_eq!(list_node_stores(&bundle).expect("list"), vec!["Gene"]);

        let reopened = open_node_store(&bundle, "http://example.org/Gene", "Gene", false)
            .expect("open node store");
        assert_eq!(reopened.manifest.class_iri, "http://example.org/Gene");
        fs::remove_dir_all(bundle.path).ok();
    }

    #[test]
    fn edge_store_manifest_create_open_and_list_round_trips() {
        let bundle = temp_bundle("edge");
        let store = open_edge_store(
            &bundle,
            "http://example.org/interactsWith",
            "interactsWith",
            Some("http://example.org/Gene"),
            Some("http://example.org/Gene"),
            true,
        )
        .expect("create edge store");
        assert_eq!(store.manifest.next_eid, 0);
        assert_eq!(
            list_edge_stores(&bundle).expect("list"),
            vec!["interactsWith"]
        );

        let reopened = open_edge_store(
            &bundle,
            "http://example.org/interactsWith",
            "interactsWith",
            None,
            None,
            false,
        )
        .expect("open edge store");
        assert_eq!(
            reopened.manifest.property_iri,
            "http://example.org/interactsWith"
        );
        fs::remove_dir_all(bundle.path).ok();
    }

    #[test]
    fn store_open_rejects_identity_mismatch() {
        let bundle = temp_bundle("mismatch");
        open_node_store(&bundle, "http://example.org/Gene", "Gene", true).expect("create");
        let err = open_node_store(&bundle, "http://example.org/Other", "Gene", false)
            .expect_err("mismatch");
        assert_eq!(err.code, "CDB-7013");
        fs::remove_dir_all(bundle.path).ok();
    }

    #[test]
    fn empty_store_scan_summary_has_zero_counts() {
        let bundle = temp_bundle("empty-scan");
        let store = open_node_store(&bundle, "http://example.org/Gene", "Gene", true)
            .expect("create node store");
        let summary = scan_node_store_summary(&store).expect("scan summary");
        assert_eq!(summary.chunk_count, 0);
        assert_eq!(summary.batch_count, 0);
        assert_eq!(summary.row_count, 0);
        assert!(summary.field_names.is_empty());
        fs::remove_dir_all(bundle.path).ok();
    }

    fn temp_bundle(label: &str) -> Bundle {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        create_bundle(
            std::env::temp_dir().join(format!("caracaldb-rust-{label}-{unique}")),
            false,
        )
        .expect("bundle")
    }
}
