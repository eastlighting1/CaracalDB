use std::fs;
use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{CaracalError, Result};

pub const BUNDLE_SUFFIX: &str = ".crcl";
pub const MANIFEST_NAME: &str = "MANIFEST";
pub const BUNDLE_DIRS: &[&str] = &[
    "dict",
    "nodes",
    "edges",
    "vec",
    "closure",
    "wal",
    "snapshots",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub size: u64,
    pub crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub created_at: String,
    #[serde(default)]
    pub last_lsn: u64,
    #[serde(default)]
    pub checkpoint_lsn: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot: Option<String>,
    #[serde(default = "default_catalog_file")]
    pub catalog_file: String,
    #[serde(default)]
    pub files: Vec<ManifestFile>,
    #[serde(default)]
    pub snapshots: Vec<String>,
}

impl Manifest {
    pub fn empty() -> Self {
        Self {
            format_version: 1,
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            last_lsn: 0,
            checkpoint_lsn: 0,
            current_snapshot: None,
            catalog_file: default_catalog_file(),
            files: Vec::new(),
            snapshots: Vec::new(),
        }
    }

    pub fn read(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_file_name(format!(
            "{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(MANIFEST_NAME)
        ));
        let text = serde_json::to_string_pretty(self)? + "\n";
        fs::write(&tmp, text)?;
        fs::rename(tmp, path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    pub path: PathBuf,
    pub manifest: Manifest,
}

impl Bundle {
    pub fn manifest_path(&self) -> PathBuf {
        self.path.join(MANIFEST_NAME)
    }

    pub fn child(&self, parts: &[&str]) -> PathBuf {
        let mut path = self.path.clone();
        for part in parts {
            path.push(part);
        }
        path
    }
}

pub fn create_bundle(path: impl AsRef<Path>, exist_ok: bool) -> Result<Bundle> {
    let root = normalize_bundle_path(path.as_ref());
    if root.exists() && !exist_ok {
        return Err(CaracalError::with_hint(
            "CDB-9001",
            format!("bundle already exists: {}", root.display()),
            "pass exist_ok=True to open or reuse an existing bundle",
        ));
    }
    if root.exists() && !root.is_dir() {
        return Err(CaracalError::new(
            "CDB-9002",
            format!("bundle path is not a directory: {}", root.display()),
        ));
    }

    fs::create_dir_all(&root)?;
    for name in BUNDLE_DIRS {
        fs::create_dir_all(root.join(name))?;
    }

    let manifest_path = root.join(MANIFEST_NAME);
    let manifest = if manifest_path.exists() {
        Manifest::read(&manifest_path)?
    } else {
        let manifest = Manifest::empty();
        manifest.write_atomic(&manifest_path)?;
        manifest
    };
    Ok(Bundle {
        path: root,
        manifest,
    })
}

pub fn open_bundle(path: impl AsRef<Path>) -> Result<Bundle> {
    let root = normalize_bundle_path(path.as_ref());
    if !root.is_dir() {
        return Err(CaracalError::new(
            "CDB-9003",
            format!("bundle directory not found: {}", root.display()),
        ));
    }

    let manifest_path = root.join(MANIFEST_NAME);
    if !manifest_path.is_file() {
        return Err(CaracalError::new(
            "CDB-9004",
            format!("manifest not found: {}", manifest_path.display()),
        ));
    }

    let missing = BUNDLE_DIRS
        .iter()
        .filter(|name| !root.join(name).is_dir())
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CaracalError::new(
            "CDB-9005",
            format!(
                "bundle is missing required directories: {}",
                missing.join(", ")
            ),
        ));
    }

    Ok(Bundle {
        path: root,
        manifest: Manifest::read(&manifest_path)?,
    })
}

fn normalize_bundle_path(path: &Path) -> PathBuf {
    let mut root = path.to_path_buf();
    if root.extension().and_then(|ext| ext.to_str()) != Some("crcl") {
        root.set_extension("crcl");
    }
    root
}

fn default_catalog_file() -> String {
    "catalog.fb".to_string()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn create_and_open_bundle_round_trips_manifest() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("caracaldb-rust-bundle-{unique}"));
        let bundle = create_bundle(&root, false).expect("create bundle");
        assert_eq!(
            bundle.path.extension().and_then(|ext| ext.to_str()),
            Some("crcl")
        );
        for name in BUNDLE_DIRS {
            assert!(bundle.path.join(name).is_dir());
        }
        let opened = open_bundle(&root).expect("open bundle");
        assert_eq!(opened.manifest.format_version, 1);
        fs::remove_dir_all(bundle.path).ok();
    }
}
