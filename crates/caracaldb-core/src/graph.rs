use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use memmap2::MmapOptions;

use crate::error::{CaracalError, Result};
use crate::header::{pack_header, HEADER_SIZE, MAGIC};

const CSR_HEAD_SIZE: usize = 24;
const CSR_FOOTER_SIZE: usize = 4;
pub const CSR_FLAG_HAS_EIDS: u32 = 1 << 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsrFile {
    pub num_vertices: u64,
    pub num_edges: u64,
    pub flags: u32,
    pub offsets: Vec<u64>,
    pub neighbors: Vec<u64>,
    pub eids: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsrNeighbors {
    pub vertex: u64,
    pub neighbors: Vec<u64>,
    pub eids: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPath {
    pub nodes: Vec<u64>,
    pub eids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KHopRow {
    pub seed: u64,
    pub node: u64,
    pub depth: u32,
    pub path: GraphPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedAdjacency {
    pub edge_type: String,
    pub csr: CsrFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleRow {
    pub src: u64,
    pub dst: u64,
    pub eid: Option<u64>,
}

pub fn build_csr_arrays(
    src: &[u64],
    dst: &[u64],
    num_vertices: u64,
    eids: Option<&[u64]>,
) -> Result<CsrFile> {
    if src.len() != dst.len() {
        return Err(CaracalError::new(
            "CDB-7080",
            "CSR src and dst arrays must have the same length",
        ));
    }
    if let Some(values) = eids {
        if values.len() != src.len() {
            return Err(CaracalError::new(
                "CDB-7080",
                "CSR eids must have the same length as neighbors",
            ));
        }
    }

    let vertex_count = usize::try_from(num_vertices).map_err(|_| {
        CaracalError::new("CDB-7080", "CSR num_vertices does not fit this platform")
    })?;
    let mut rows = Vec::with_capacity(src.len());
    for index in 0..src.len() {
        let source = src[index];
        if source >= num_vertices || dst[index] >= num_vertices {
            return Err(CaracalError::new(
                "CDB-7080",
                "CSR edge endpoint is outside num_vertices",
            ));
        }
        rows.push((source, dst[index], eids.map(|values| values[index])));
    }
    rows.sort_by_key(|(source, _, _)| *source);

    let mut offsets = vec![0u64; vertex_count + 1];
    for (source, _, _) in &rows {
        offsets[usize::try_from(*source).expect("validated source") + 1] += 1;
    }
    for index in 1..offsets.len() {
        offsets[index] += offsets[index - 1];
    }

    let neighbors = rows
        .iter()
        .map(|(_, target, _)| *target)
        .collect::<Vec<_>>();
    let eids = eids.map(|_| {
        rows.iter()
            .map(|(_, _, eid)| eid.expect("eid present"))
            .collect::<Vec<_>>()
    });
    Ok(CsrFile {
        num_vertices,
        num_edges: neighbors.len() as u64,
        flags: if eids.is_some() { CSR_FLAG_HAS_EIDS } else { 0 },
        offsets,
        neighbors,
        eids,
    })
}

pub fn build_csc_arrays(
    src: &[u64],
    dst: &[u64],
    num_vertices: u64,
    eids: Option<&[u64]>,
) -> Result<CsrFile> {
    build_csr_arrays(dst, src, num_vertices, eids)
}

pub fn write_csr(path: impl AsRef<Path>, csr: &CsrFile) -> Result<PathBuf> {
    validate_csr(csr)?;
    let target = path.as_ref();
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut body = Vec::new();
    body.extend_from_slice(&csr.num_vertices.to_le_bytes());
    body.extend_from_slice(&csr.num_edges.to_le_bytes());
    body.extend_from_slice(&csr.flags.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    for value in &csr.offsets {
        body.extend_from_slice(&value.to_le_bytes());
    }
    for value in &csr.neighbors {
        body.extend_from_slice(&value.to_le_bytes());
    }
    if let Some(eids) = &csr.eids {
        for value in eids {
            body.extend_from_slice(&value.to_le_bytes());
        }
    }
    let crc = crc32(&body);
    let tmp = target.with_file_name(format!(
        "{}.tmp",
        target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("csr")
    ));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&pack_header())?;
    file.write_all(&body)?;
    file.write_all(&crc.to_le_bytes())?;
    drop(file);
    fs::rename(&tmp, target)?;
    Ok(target.to_path_buf())
}

pub fn read_csr(path: impl AsRef<Path>) -> Result<CsrFile> {
    let target = path.as_ref();
    let file = fs::File::open(target)?;
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .map_err(|err| CaracalError::new("CDB-7081", format!("CSR mmap failed: {err}")))?;
    let data = &mmap[..];
    if data.len() < HEADER_SIZE + CSR_HEAD_SIZE + CSR_FOOTER_SIZE {
        return Err(CaracalError::new(
            "CDB-7081",
            format!("CSR file too small: {}", target.display()),
        ));
    }
    if &data[..MAGIC.len()] != MAGIC {
        return Err(CaracalError::new(
            "CDB-7081",
            format!("invalid CSR magic: {}", target.display()),
        ));
    }

    let body_start = HEADER_SIZE;
    let body_end = data.len() - CSR_FOOTER_SIZE;
    let body = &data[body_start..body_end];
    let expected_crc = read_u32_le(&data[body_end..body_end + 4]);
    let actual_crc = crc32(body);
    if actual_crc != expected_crc {
        return Err(CaracalError::new(
            "CDB-7081",
            format!("CSR checksum mismatch: {}", target.display()),
        ));
    }

    let num_vertices = read_u64_le(&data[HEADER_SIZE..HEADER_SIZE + 8]);
    let num_edges = read_u64_le(&data[HEADER_SIZE + 8..HEADER_SIZE + 16]);
    let flags = read_u32_le(&data[HEADER_SIZE + 16..HEADER_SIZE + 20]);
    let offsets_len = usize::try_from(num_vertices + 1)
        .map_err(|_| CaracalError::new("CDB-7081", "CSR vertex count is too large"))?;
    let edges_len = usize::try_from(num_edges)
        .map_err(|_| CaracalError::new("CDB-7081", "CSR edge count is too large"))?;

    let mut cursor = HEADER_SIZE + CSR_HEAD_SIZE;
    let offsets = read_u64_vec(data, &mut cursor, offsets_len, target)?;
    let neighbors = read_u64_vec(data, &mut cursor, edges_len, target)?;
    let eids = if flags & CSR_FLAG_HAS_EIDS != 0 {
        Some(read_u64_vec(data, &mut cursor, edges_len, target)?)
    } else {
        None
    };

    let csr = CsrFile {
        num_vertices,
        num_edges,
        flags,
        offsets,
        neighbors,
        eids,
    };
    validate_csr(&csr)?;
    Ok(csr)
}

pub fn csr_neighbors_of(csr: &CsrFile, vertex: u64) -> Result<CsrNeighbors> {
    if vertex >= csr.num_vertices {
        return Err(CaracalError::new(
            "CDB-7083",
            format!("vertex id out of range: {vertex} (n={})", csr.num_vertices),
        ));
    }
    let start = usize::try_from(csr.offsets[vertex as usize])
        .map_err(|_| CaracalError::new("CDB-7083", "CSR offset is too large"))?;
    let end = usize::try_from(csr.offsets[vertex as usize + 1])
        .map_err(|_| CaracalError::new("CDB-7083", "CSR offset is too large"))?;
    let neighbors = csr.neighbors[start..end].to_vec();
    let eids = csr.eids.as_ref().map(|values| values[start..end].to_vec());
    Ok(CsrNeighbors {
        vertex,
        neighbors,
        eids,
    })
}

pub fn csr_k_hop(
    csr: &CsrFile,
    seeds: &[u64],
    min_depth: u32,
    max_depth: u32,
) -> Result<Vec<KHopRow>> {
    if min_depth > max_depth {
        return Err(CaracalError::new(
            "CDB-7083",
            "min_depth must be <= max_depth",
        ));
    }
    let mut rows = Vec::new();
    for &seed in seeds {
        if seed >= csr.num_vertices {
            return Err(CaracalError::new(
                "CDB-7083",
                format!("vertex id out of range: {seed} (n={})", csr.num_vertices),
            ));
        }
        let mut queue = VecDeque::from([(
            seed,
            0u32,
            GraphPath {
                nodes: vec![seed],
                eids: Vec::new(),
            },
        )]);
        let mut seen = BTreeSet::from([(seed, 0u32)]);
        while let Some((node, depth, path)) = queue.pop_front() {
            if depth >= min_depth && depth <= max_depth {
                rows.push(KHopRow {
                    seed,
                    node,
                    depth,
                    path: path.clone(),
                });
            }
            if depth == max_depth {
                continue;
            }
            let start = csr.offsets[node as usize] as usize;
            let end = csr.offsets[node as usize + 1] as usize;
            for idx in start..end {
                let next = csr.neighbors[idx];
                let next_depth = depth + 1;
                if !seen.insert((next, next_depth)) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.nodes.push(next);
                if let Some(eids) = &csr.eids {
                    next_path.eids.push(eids[idx]);
                }
                queue.push_back((next, next_depth, next_path));
            }
        }
    }
    Ok(rows)
}

pub fn csr_shortest_path(
    csr: &CsrFile,
    source: u64,
    target: u64,
    max_depth: u32,
) -> Result<Option<GraphPath>> {
    if source >= csr.num_vertices || target >= csr.num_vertices {
        return Err(CaracalError::new("CDB-7083", "vertex id out of range"));
    }
    let mut queue = VecDeque::from([(
        source,
        GraphPath {
            nodes: vec![source],
            eids: Vec::new(),
        },
    )]);
    let mut seen = BTreeSet::from([source]);
    while let Some((node, path)) = queue.pop_front() {
        if node == target {
            return Ok(Some(path));
        }
        if path.nodes.len().saturating_sub(1) >= max_depth as usize {
            continue;
        }
        let start = csr.offsets[node as usize] as usize;
        let end = csr.offsets[node as usize + 1] as usize;
        for idx in start..end {
            let next = csr.neighbors[idx];
            if !seen.insert(next) {
                continue;
            }
            let mut next_path = path.clone();
            next_path.nodes.push(next);
            if let Some(eids) = &csr.eids {
                next_path.eids.push(eids[idx]);
            }
            queue.push_back((next, next_path));
        }
    }
    Ok(None)
}

pub fn typed_adjacency(edge_type: &str, csr: CsrFile) -> TypedAdjacency {
    TypedAdjacency {
        edge_type: edge_type.to_string(),
        csr,
    }
}

pub fn typed_neighbors(
    adjacency: &TypedAdjacency,
    vertex: u64,
) -> Result<Vec<(String, u64, Option<u64>)>> {
    let neighbors = csr_neighbors_of(&adjacency.csr, vertex)?;
    Ok(neighbors
        .neighbors
        .iter()
        .enumerate()
        .map(|(idx, dst)| {
            (
                adjacency.edge_type.clone(),
                *dst,
                neighbors.eids.as_ref().map(|eids| eids[idx]),
            )
        })
        .collect())
}

pub fn csr_neighbor_sample(
    csr: &CsrFile,
    seeds: &[u64],
    fanout: Option<usize>,
    with_replacement: bool,
) -> Result<Vec<SampleRow>> {
    let mut rows = Vec::new();
    for &seed in seeds {
        let neighbors = csr_neighbors_of(csr, seed)?;
        let take = fanout.unwrap_or(neighbors.neighbors.len());
        if take == 0 || neighbors.neighbors.is_empty() {
            continue;
        }
        for i in 0..take {
            let idx = if with_replacement {
                i % neighbors.neighbors.len()
            } else {
                i
            };
            if idx >= neighbors.neighbors.len() {
                break;
            }
            rows.push(SampleRow {
                src: seed,
                dst: neighbors.neighbors[idx],
                eid: neighbors.eids.as_ref().map(|eids| eids[idx]),
            });
        }
    }
    Ok(rows)
}

pub fn hnsw_manifest_boundary(index_name: &str, vector_column: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("index_name".to_string(), index_name.to_string()),
        ("vector_column".to_string(), vector_column.to_string()),
        ("storage_boundary".to_string(), "manifest-only".to_string()),
    ])
}

fn validate_csr(csr: &CsrFile) -> Result<()> {
    if csr.offsets.len() != usize::try_from(csr.num_vertices + 1).unwrap_or(usize::MAX) {
        return Err(CaracalError::new(
            "CDB-7080",
            "CSR offsets length must equal num_vertices + 1",
        ));
    }
    if csr.neighbors.len() != usize::try_from(csr.num_edges).unwrap_or(usize::MAX) {
        return Err(CaracalError::new(
            "CDB-7080",
            "CSR neighbors length must equal num_edges",
        ));
    }
    if csr.offsets.last().copied() != Some(csr.num_edges) {
        return Err(CaracalError::new(
            "CDB-7080",
            "CSR offsets[-1] must equal num_edges",
        ));
    }
    if let Some(eids) = &csr.eids {
        if eids.len() != csr.neighbors.len() {
            return Err(CaracalError::new(
                "CDB-7080",
                "CSR eids must be the same length as neighbors",
            ));
        }
    }
    Ok(())
}

fn read_u64_vec(data: &[u8], cursor: &mut usize, count: usize, path: &Path) -> Result<Vec<u64>> {
    let byte_count = count
        .checked_mul(8)
        .ok_or_else(|| CaracalError::new("CDB-7081", "CSR array is too large"))?;
    if *cursor + byte_count > data.len() - CSR_FOOTER_SIZE {
        return Err(CaracalError::new(
            "CDB-7081",
            format!("CSR file truncated: {}", path.display()),
        ));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in data[*cursor..*cursor + byte_count].chunks_exact(8) {
        out.push(read_u64_le(chunk));
    }
    *cursor += byte_count;
    Ok(out)
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 slice"))
}

fn read_u32_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("u32 slice"))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn csr_build_write_read_round_trips() {
        let csr =
            build_csr_arrays(&[1, 0, 1], &[2, 1, 0], 3, Some(&[11, 10, 12])).expect("build csr");
        assert_eq!(csr.offsets, vec![0, 1, 3, 3]);
        assert_eq!(csr.neighbors, vec![1, 2, 0]);
        assert_eq!(csr.eids, Some(vec![10, 11, 12]));

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("caracaldb-rust-csr-{unique}.csr"));
        write_csr(&path, &csr).expect("write csr");
        let read = read_csr(&path).expect("read csr");
        assert_eq!(read, csr);
        fs::remove_file(path).ok();
    }

    #[test]
    fn csc_build_swaps_source_and_destination() {
        let csc =
            build_csc_arrays(&[1, 0, 1], &[2, 1, 0], 3, Some(&[11, 10, 12])).expect("build csc");
        assert_eq!(csc.offsets, vec![0, 1, 2, 3]);
        assert_eq!(csc.neighbors, vec![1, 0, 1]);
        assert_eq!(csc.eids, Some(vec![12, 10, 11]));
    }

    #[test]
    fn csr_neighbors_of_returns_neighbor_slice() {
        let csr =
            build_csr_arrays(&[1, 0, 1], &[2, 1, 0], 3, Some(&[11, 10, 12])).expect("build csr");
        let neighbors = csr_neighbors_of(&csr, 1).expect("neighbors");
        assert_eq!(neighbors.vertex, 1);
        assert_eq!(neighbors.neighbors, vec![2, 0]);
        assert_eq!(neighbors.eids, Some(vec![11, 12]));
    }

    #[test]
    fn csr_builder_preserves_core_invariants_across_shapes() {
        let cases = [
            (vec![], vec![], 0, None),
            (vec![0], vec![0], 1, Some(vec![7])),
            (
                vec![2, 0, 2, 1],
                vec![1, 2, 0, 2],
                3,
                Some(vec![4, 1, 3, 2]),
            ),
            (vec![3, 1, 3, 0, 2], vec![0, 2, 1, 3, 3], 4, None),
        ];
        for (src, dst, num_vertices, eids) in cases {
            let csr = build_csr_arrays(&src, &dst, num_vertices, eids.as_deref()).expect("build");
            assert_eq!(csr.offsets.len(), num_vertices as usize + 1);
            assert_eq!(csr.offsets.first().copied(), Some(0));
            assert_eq!(csr.offsets.last().copied(), Some(csr.num_edges));
            assert!(csr.offsets.windows(2).all(|pair| pair[0] <= pair[1]));
            assert_eq!(csr.neighbors.len(), csr.num_edges as usize);
            if let Some(values) = &csr.eids {
                assert_eq!(values.len(), csr.neighbors.len());
            }
            validate_csr(&csr).expect("valid csr");
        }
    }

    #[test]
    fn graph_traversal_helpers_cover_khop_shortest_sampling_and_typed_adjacency() {
        let csr = build_csr_arrays(&[0, 0, 1, 2], &[1, 2, 3, 3], 4, Some(&[10, 11, 12, 13]))
            .expect("build");
        let khop = csr_k_hop(&csr, &[0], 1, 2).expect("k-hop");
        assert_eq!(
            khop.iter()
                .map(|row| (row.node, row.depth))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 1), (3, 2)]
        );

        let path = csr_shortest_path(&csr, 0, 3, 2)
            .expect("shortest")
            .expect("reachable");
        assert_eq!(path.nodes, vec![0, 1, 3]);
        assert_eq!(path.eids, vec![10, 12]);

        let typed = typed_adjacency("RELATED_TO", csr.clone());
        assert_eq!(
            typed_neighbors(&typed, 0).expect("typed"),
            vec![
                ("RELATED_TO".to_string(), 1, Some(10)),
                ("RELATED_TO".to_string(), 2, Some(11))
            ]
        );

        let sample = csr_neighbor_sample(&csr, &[0], Some(1), false).expect("sample");
        assert_eq!(
            sample,
            vec![SampleRow {
                src: 0,
                dst: 1,
                eid: Some(10)
            }]
        );

        let boundary = hnsw_manifest_boundary("vec_idx", "embedding");
        assert_eq!(boundary["storage_boundary"], "manifest-only");
    }
}
