use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::error::{CaracalError, Result};
use crate::header::{pack_header, HEADER_SIZE, MAGIC};

const SEGMENT_SUFFIX: &str = "wal";
const RECORD_HEAD_SIZE: usize = 16;
const CRC_SIZE: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub kind: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wal {
    pub directory: PathBuf,
    pub last_lsn: u64,
}

impl Wal {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let last_lsn = iter_all_records(&directory)?
            .last()
            .map(|record| record.lsn)
            .unwrap_or(0);
        Ok(Self {
            directory,
            last_lsn,
        })
    }

    pub fn append(&mut self, kind: &str, payload: &[u8]) -> Result<u64> {
        let lsn = self.last_lsn + 1;
        let record = WalRecord {
            lsn,
            prev_lsn: self.last_lsn,
            kind: kind.to_string(),
            payload: payload.to_vec(),
        };
        let path = self.segment_path()?;
        let mut file = if path.is_file() {
            OpenOptions::new().append(true).open(&path)?
        } else {
            let mut file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)?;
            file.write_all(&pack_header())?;
            file
        };
        file.write_all(&record.encode()?)?;
        file.flush()?;
        self.last_lsn = lsn;
        Ok(lsn)
    }

    pub fn truncate_before(&self, lsn: u64) -> Result<usize> {
        let mut removed = 0usize;
        for segment in wal_segments(&self.directory)? {
            let max_lsn = iter_segment_records(&segment)?
                .last()
                .map(|record| record.lsn);
            if max_lsn.is_some_and(|value| value <= lsn) {
                fs::remove_file(segment)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn segment_path(&self) -> Result<PathBuf> {
        let mut segments = wal_segments(&self.directory)?;
        if let Some(path) = segments.pop() {
            return Ok(path);
        }
        Ok(self.directory.join("000001.wal"))
    }
}

impl WalRecord {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let kind = self.kind.as_bytes();
        let kind_len = u32::try_from(kind.len())
            .map_err(|_| CaracalError::new("CDB-7050", "WAL kind is too large"))?;
        let payload_len = u32::try_from(self.payload.len())
            .map_err(|_| CaracalError::new("CDB-7050", "WAL payload is too large"))?;
        let mut body = Vec::with_capacity(RECORD_HEAD_SIZE + kind.len() + self.payload.len() + 8);
        body.extend_from_slice(&self.lsn.to_le_bytes());
        body.extend_from_slice(&self.prev_lsn.to_le_bytes());
        body.extend_from_slice(&kind_len.to_le_bytes());
        body.extend_from_slice(kind);
        body.extend_from_slice(&payload_len.to_le_bytes());
        body.extend_from_slice(&self.payload);
        let crc = crc32(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        Ok(body)
    }
}

pub fn iter_all_records(directory: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    let directory = directory.as_ref();
    if !directory.is_dir() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for segment in wal_segments(directory)? {
        records.extend(iter_segment_records(segment)?);
    }
    Ok(records)
}

pub fn iter_segment_records(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    let path = path.as_ref();
    let data = fs::read(path)?;
    if data.len() < HEADER_SIZE || &data[..MAGIC.len()] != MAGIC {
        return Err(CaracalError::new(
            "CDB-7053",
            format!("invalid WAL segment header: {}", path.display()),
        ));
    }
    let mut cursor = HEADER_SIZE;
    let mut records = Vec::new();
    while cursor < data.len() {
        let Some((record, next)) = read_record(&data, cursor)? else {
            break;
        };
        records.push(record);
        cursor = next;
    }
    Ok(records)
}

fn read_record(data: &[u8], offset: usize) -> Result<Option<(WalRecord, usize)>> {
    if offset + RECORD_HEAD_SIZE > data.len() {
        return Ok(None);
    }
    let lsn = read_u64_le(&data[offset..offset + 8]);
    let prev_lsn = read_u64_le(&data[offset + 8..offset + 16]);
    let mut cursor = offset + RECORD_HEAD_SIZE;
    let Some(kind_len) = read_u32_len(data, &mut cursor)? else {
        return Ok(None);
    };
    if cursor + kind_len + 4 > data.len() {
        return Ok(None);
    }
    let kind = std::str::from_utf8(&data[cursor..cursor + kind_len])
        .map_err(|err| CaracalError::new("CDB-7052", format!("invalid WAL kind: {err}")))?
        .to_string();
    cursor += kind_len;
    let Some(payload_len) = read_u32_len(data, &mut cursor)? else {
        return Ok(None);
    };
    if cursor + payload_len + CRC_SIZE > data.len() {
        return Ok(None);
    }
    let payload = data[cursor..cursor + payload_len].to_vec();
    cursor += payload_len;
    let expected_crc = read_u32_le(&data[cursor..cursor + CRC_SIZE]);
    cursor += CRC_SIZE;
    let actual_crc = crc32(&data[offset..cursor - CRC_SIZE]);
    if actual_crc != expected_crc {
        return Err(CaracalError::new("CDB-7052", "WAL record CRC mismatch"));
    }
    Ok(Some((
        WalRecord {
            lsn,
            prev_lsn,
            kind,
            payload,
        },
        cursor,
    )))
}

fn read_u32_len(data: &[u8], cursor: &mut usize) -> Result<Option<usize>> {
    if *cursor + 4 > data.len() {
        return Ok(None);
    }
    let value = read_u32_le(&data[*cursor..*cursor + 4]);
    *cursor += 4;
    usize::try_from(value)
        .map(Some)
        .map_err(|_| CaracalError::new("CDB-7052", "WAL length is too large"))
}

fn wal_segments(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some(SEGMENT_SUFFIX) {
            segments.push(path);
        }
    }
    segments.sort();
    Ok(segments)
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
    fn wal_appends_and_reads_records() {
        let root = temp_dir("append");
        let mut wal = Wal::open(&root).expect("open wal");
        assert_eq!(wal.append("INSERT_NODE", b"a").expect("append"), 1);
        assert_eq!(wal.append("COMMIT", b"").expect("append"), 2);

        let records = iter_all_records(&root).expect("read wal");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 1);
        assert_eq!(records[1].prev_lsn, 1);
        assert_eq!(records[0].kind, "INSERT_NODE");
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn wal_ignores_partial_tail_record() {
        let root = temp_dir("partial");
        let mut wal = Wal::open(&root).expect("open wal");
        wal.append("INSERT_NODE", b"a").expect("append");
        wal.append("INSERT_NODE", b"b").expect("append");
        let segment = root.join("000001.wal");
        let mut data = fs::read(&segment).expect("read segment");
        data.truncate(data.len() - 3);
        fs::write(&segment, data).expect("write truncated segment");

        let records = iter_all_records(&root).expect("read wal");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, 1);
        fs::remove_dir_all(root).ok();
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("caracaldb-rust-wal-{label}-{unique}"))
    }
}
