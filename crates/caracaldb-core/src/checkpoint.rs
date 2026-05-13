use crate::bundle::{Bundle, Manifest};
use crate::error::{CaracalError, Result};
use crate::wal::Wal;

pub const CHECKPOINT_KIND: &str = "CHECKPOINT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointResult {
    pub checkpoint_lsn: u64,
    pub wal_segments_removed: usize,
}

pub fn checkpoint(bundle: &mut Bundle, wal: &mut Wal) -> Result<CheckpointResult> {
    if wal.last_lsn < bundle.manifest.checkpoint_lsn {
        return Err(CaracalError::new(
            "CDB-7060",
            format!(
                "WAL last_lsn={} is behind manifest checkpoint_lsn={}; refusing to regress",
                wal.last_lsn, bundle.manifest.checkpoint_lsn
            ),
        ));
    }
    let marker_lsn = wal.append(CHECKPOINT_KIND, &[])?;
    let mut manifest = bundle.manifest.clone();
    manifest.last_lsn = marker_lsn;
    manifest.checkpoint_lsn = marker_lsn;
    manifest.write_atomic(&bundle.manifest_path())?;
    bundle.manifest = manifest;
    let wal_segments_removed = wal.truncate_before(marker_lsn)?;
    Ok(CheckpointResult {
        checkpoint_lsn: marker_lsn,
        wal_segments_removed,
    })
}

pub fn reload_manifest(bundle: &mut Bundle) -> Result<Manifest> {
    let manifest = Manifest::read(&bundle.manifest_path())?;
    bundle.manifest = manifest.clone();
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::bundle::create_bundle;

    use super::*;

    #[test]
    fn checkpoint_records_marker_and_updates_manifest() {
        let root = temp_root("checkpoint");
        let mut bundle = create_bundle(&root, false).expect("bundle");
        let mut wal = Wal::open(bundle.path.join("wal")).expect("wal");
        wal.append("INSERT_NODE", b"a").expect("append");

        let result = checkpoint(&mut bundle, &mut wal).expect("checkpoint");

        assert_eq!(result.checkpoint_lsn, 2);
        assert_eq!(bundle.manifest.last_lsn, 2);
        assert_eq!(bundle.manifest.checkpoint_lsn, 2);
        let manifest = reload_manifest(&mut bundle).expect("reload");
        assert_eq!(manifest.checkpoint_lsn, 2);
        fs::remove_dir_all(bundle.path).ok();
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("caracaldb-rust-{label}-{unique}"))
    }
}
