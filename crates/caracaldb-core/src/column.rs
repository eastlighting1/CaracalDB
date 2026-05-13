use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::Path;

use arrow_ipc::reader::StreamReader;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{CaracalError, Result};
use crate::header::{pack_header, HEADER_SIZE, MAGIC};

const FOOTER_TRAILER_SIZE: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSegmentFooter {
    pub format_version: u32,
    pub codec: String,
    pub row_count: u64,
    pub batch_count: u64,
    pub schema: String,
    pub uncompressed_size: u64,
    pub payload_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSegmentInfo {
    pub footer: ColumnSegmentFooter,
    pub payload_offset: u64,
    pub payload_size: u64,
    pub footer_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBatchInfo {
    pub row_count: usize,
    pub column_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSegmentDecoded {
    pub info: ColumnSegmentInfo,
    pub field_names: Vec<String>,
    pub batches: Vec<ColumnBatchInfo>,
}

pub fn read_column_segment_info(path: impl AsRef<Path>) -> Result<ColumnSegmentInfo> {
    Ok(read_column_segment_parts(path.as_ref())?.0)
}

pub fn read_column_segment_ipc_stream(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let (info, payload) = read_column_segment_parts(path.as_ref())?;
    decompress_payload(&payload, &info.footer.codec)
}

pub fn write_column_segment_ipc_stream(
    path: impl AsRef<Path>,
    ipc_stream: &[u8],
    codec: &str,
) -> Result<ColumnSegmentInfo> {
    validate_codec(codec)?;
    let target = path.as_ref();
    let (schema, batch_count, row_count) = inspect_ipc_stream(ipc_stream)?;
    if batch_count == 0 {
        return Err(CaracalError::new(
            "CDB-7001",
            "cannot write an empty column segment",
        ));
    }
    let payload = compress_payload(ipc_stream, codec)?;
    let footer = ColumnSegmentFooter {
        format_version: 1,
        codec: codec.to_string(),
        row_count,
        batch_count,
        schema,
        uncompressed_size: ipc_stream.len() as u64,
        payload_size: payload.len() as u64,
    };
    write_column_segment_parts(target, &payload, &footer)?;
    read_column_segment_info(target)
}

pub fn decode_column_segment(path: impl AsRef<Path>) -> Result<ColumnSegmentDecoded> {
    let (info, payload) = read_column_segment_parts(path.as_ref())?;
    let stream = decompress_payload(&payload, &info.footer.codec)?;
    let reader = StreamReader::try_new(Cursor::new(stream), None)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC stream: {err}")))?;
    let schema = reader.schema();
    let field_names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|err| {
            CaracalError::new("CDB-7001", format!("invalid Arrow IPC batch: {err}"))
        })?;
        batches.push(ColumnBatchInfo {
            row_count: batch.num_rows(),
            column_count: batch.num_columns(),
        });
    }
    let rows = batches
        .iter()
        .map(|batch| batch.row_count as u64)
        .sum::<u64>();
    if rows != info.footer.row_count {
        return Err(CaracalError::new(
            "CDB-7001",
            format!(
                "column row count mismatch: footer={}, decoded={rows}",
                info.footer.row_count
            ),
        ));
    }
    if batches.len() as u64 != info.footer.batch_count {
        return Err(CaracalError::new(
            "CDB-7001",
            format!(
                "column batch count mismatch: footer={}, decoded={}",
                info.footer.batch_count,
                batches.len()
            ),
        ));
    }
    Ok(ColumnSegmentDecoded {
        info,
        field_names,
        batches,
    })
}

fn inspect_ipc_stream(ipc_stream: &[u8]) -> Result<(String, u64, u64)> {
    let reader = StreamReader::try_new(Cursor::new(ipc_stream.to_vec()), None)
        .map_err(|err| CaracalError::new("CDB-7001", format!("invalid Arrow IPC stream: {err}")))?;
    let schema = format!("{:?}", reader.schema());
    let mut batch_count = 0u64;
    let mut row_count = 0u64;
    for batch in reader {
        let batch = batch.map_err(|err| {
            CaracalError::new("CDB-7001", format!("invalid Arrow IPC batch: {err}"))
        })?;
        batch_count += 1;
        row_count += batch.num_rows() as u64;
    }
    Ok((schema, batch_count, row_count))
}

fn read_column_segment_parts(path: &Path) -> Result<(ColumnSegmentInfo, Vec<u8>)> {
    let target = path;
    let data = fs::read(target)?;
    if data.len() < HEADER_SIZE + FOOTER_TRAILER_SIZE {
        return Err(CaracalError::new(
            "CDB-7001",
            format!("column segment is too small: {}", target.display()),
        ));
    }
    if &data[..MAGIC.len()] != MAGIC {
        return Err(CaracalError::new(
            "CDB-7001",
            format!("invalid column segment magic: {}", target.display()),
        ));
    }

    let trailer_start = data.len() - FOOTER_TRAILER_SIZE;
    let footer_offset = read_u64_le(&data[trailer_start..trailer_start + 8]);
    let expected_crc = read_u32_le(&data[trailer_start + 8..]);
    let footer_offset_usize = usize::try_from(footer_offset).map_err(|_| {
        CaracalError::new(
            "CDB-7001",
            format!("invalid column footer offset: {}", target.display()),
        )
    })?;
    if footer_offset_usize < HEADER_SIZE || footer_offset_usize > trailer_start {
        return Err(CaracalError::new(
            "CDB-7001",
            format!("invalid column footer offset: {}", target.display()),
        ));
    }

    let footer_bytes = &data[footer_offset_usize..trailer_start];
    let actual_crc = crc32(footer_bytes);
    if actual_crc != expected_crc {
        return Err(CaracalError::new(
            "CDB-7001",
            format!("column footer checksum mismatch: {}", target.display()),
        ));
    }
    let footer: ColumnSegmentFooter = serde_json::from_slice(footer_bytes)?;
    let payload = &data[HEADER_SIZE..footer_offset_usize];
    if payload.len() as u64 != footer.payload_size {
        return Err(CaracalError::new(
            "CDB-7001",
            format!("column payload size mismatch: {}", target.display()),
        ));
    }
    validate_codec(&footer.codec)?;

    let info = ColumnSegmentInfo {
        payload_offset: HEADER_SIZE as u64,
        payload_size: payload.len() as u64,
        footer_offset,
        footer,
    };
    Ok((info, payload.to_vec()))
}

fn write_column_segment_parts(
    target: &Path,
    payload: &[u8],
    footer: &ColumnSegmentFooter,
) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let footer_bytes = serde_json::to_vec(footer)?;
    let footer_crc = crc32(&footer_bytes);
    let footer_offset = HEADER_SIZE as u64 + payload.len() as u64;
    let tmp = target.with_file_name(format!(
        "{}.tmp",
        target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("column")
    ));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&pack_header())?;
    file.write_all(payload)?;
    file.write_all(&footer_bytes)?;
    file.write_all(&footer_offset.to_le_bytes())?;
    file.write_all(&footer_crc.to_le_bytes())?;
    drop(file);
    fs::rename(tmp, target)?;
    Ok(())
}

fn validate_codec(codec: &str) -> Result<()> {
    match codec {
        "none" | "zstd" | "lz4" => Ok(()),
        _ => Err(CaracalError::new(
            "CDB-7002",
            format!("unknown column codec: {codec}"),
        )),
    }
}

fn decompress_payload(payload: &[u8], codec: &str) -> Result<Vec<u8>> {
    match codec {
        "none" => Ok(payload.to_vec()),
        "zstd" => zstd::stream::decode_all(Cursor::new(payload)).map_err(|err| {
            CaracalError::new(
                "CDB-7001",
                format!("zstd column decompression failed: {err}"),
            )
        }),
        "lz4" => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(payload));
            let mut out = Vec::new();
            decoder.read_to_end(&mut out).map_err(|err| {
                CaracalError::new(
                    "CDB-7001",
                    format!("lz4 column decompression failed: {err}"),
                )
            })?;
            Ok(out)
        }
        _ => Err(CaracalError::new(
            "CDB-7002",
            format!("unknown column codec: {codec}"),
        )),
    }
}

fn compress_payload(payload: &[u8], codec: &str) -> Result<Vec<u8>> {
    match codec {
        "none" => Ok(payload.to_vec()),
        "zstd" => {
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).map_err(|err| {
                CaracalError::new("CDB-7001", format!("zstd column compression failed: {err}"))
            })?;
            encoder.include_contentsize(true).map_err(|err| {
                CaracalError::new("CDB-7001", format!("zstd column compression failed: {err}"))
            })?;
            encoder
                .set_pledged_src_size(Some(payload.len() as u64))
                .map_err(|err| {
                    CaracalError::new("CDB-7001", format!("zstd column compression failed: {err}"))
                })?;
            encoder.write_all(payload).map_err(|err| {
                CaracalError::new("CDB-7001", format!("zstd column compression failed: {err}"))
            })?;
            encoder.finish().map_err(|err| {
                CaracalError::new("CDB-7001", format!("zstd column compression failed: {err}"))
            })
        }
        "lz4" => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(payload).map_err(|err| {
                CaracalError::new("CDB-7001", format!("lz4 column compression failed: {err}"))
            })?;
            encoder.finish().map_err(|err| {
                CaracalError::new("CDB-7001", format!("lz4 column compression failed: {err}"))
            })
        }
        _ => Err(CaracalError::new(
            "CDB-7002",
            format!("unknown column codec: {codec}"),
        )),
    }
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
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::header::pack_header;

    use super::*;

    #[test]
    fn reads_column_segment_footer() {
        let footer = ColumnSegmentFooter {
            format_version: 1,
            codec: "none".to_string(),
            row_count: 2,
            batch_count: 1,
            schema: "a: int64".to_string(),
            uncompressed_size: 4,
            payload_size: 4,
        };
        let footer_bytes = serde_json::to_vec(&footer).expect("footer json");
        let footer_crc = crc32(&footer_bytes);
        let footer_offset = HEADER_SIZE as u64 + 4;
        let path = temp_path();
        let mut file = fs::File::create(&path).expect("create");
        file.write_all(&pack_header()).expect("header");
        file.write_all(&[1, 2, 3, 4]).expect("payload");
        file.write_all(&footer_bytes).expect("footer");
        file.write_all(&footer_offset.to_le_bytes())
            .expect("offset");
        file.write_all(&footer_crc.to_le_bytes()).expect("crc");
        drop(file);

        let info = read_column_segment_info(&path).expect("read info");
        assert_eq!(info.footer, footer);
        assert_eq!(info.payload_offset, HEADER_SIZE as u64);
        assert_eq!(info.payload_size, 4);
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_non_ipc_payload_when_decoding() {
        let footer = ColumnSegmentFooter {
            format_version: 1,
            codec: "none".to_string(),
            row_count: 2,
            batch_count: 1,
            schema: "a: int64".to_string(),
            uncompressed_size: 4,
            payload_size: 4,
        };
        let path = write_fake_segment(&footer, &[1, 2, 3, 4]);
        let err = decode_column_segment(&path).expect_err("invalid IPC payload");
        assert_eq!(err.code, "CDB-7001");
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_footer_checksum_mismatch() {
        let footer = ColumnSegmentFooter {
            format_version: 1,
            codec: "none".to_string(),
            row_count: 2,
            batch_count: 1,
            schema: "a: int64".to_string(),
            uncompressed_size: 4,
            payload_size: 4,
        };
        let path = write_fake_segment(&footer, &[1, 2, 3, 4]);
        let mut data = fs::read(&path).expect("read segment");
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, data).expect("write corrupted segment");

        let err = read_column_segment_info(&path).expect_err("checksum mismatch");
        assert_eq!(err.code, "CDB-7001");
        assert!(err.message.contains("checksum mismatch"));
        fs::remove_file(path).ok();
    }

    fn write_fake_segment(footer: &ColumnSegmentFooter, payload: &[u8]) -> std::path::PathBuf {
        let footer_bytes = serde_json::to_vec(footer).expect("footer json");
        let footer_crc = crc32(&footer_bytes);
        let footer_offset = HEADER_SIZE as u64 + payload.len() as u64;
        let path = temp_path();
        let mut file = fs::File::create(&path).expect("create");
        file.write_all(&pack_header()).expect("header");
        file.write_all(payload).expect("payload");
        file.write_all(&footer_bytes).expect("footer");
        file.write_all(&footer_offset.to_le_bytes())
            .expect("offset");
        file.write_all(&footer_crc.to_le_bytes()).expect("crc");
        drop(file);
        path
    }

    fn temp_path() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("caracaldb-rust-col-{unique}.col"))
    }
}
