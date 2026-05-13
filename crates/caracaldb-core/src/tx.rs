use crate::error::{CaracalError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub tx_id: u64,
    pub snapshot_lsn: u64,
}

pub fn ensure_no_write_conflict(snapshot_lsn: u64, last_committed_lsn: Option<u64>) -> Result<()> {
    if last_committed_lsn.is_some_and(|committed| committed > snapshot_lsn) {
        return Err(CaracalError::with_hint(
            "CDB-8002",
            format!(
                "write conflict: key was committed after this transaction's snapshot lsn={snapshot_lsn}"
            ),
            "re-read the latest snapshot and retry the transaction",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_conflict_uses_cdb_8002() {
        let err = ensure_no_write_conflict(3, Some(4)).expect_err("conflict");
        assert_eq!(err.code, "CDB-8002");
        assert!(err.hint.is_some());
    }

    #[test]
    fn tx_allows_commits_at_or_before_snapshot() {
        ensure_no_write_conflict(3, None).expect("no previous commit");
        ensure_no_write_conflict(3, Some(3)).expect("same boundary");
        ensure_no_write_conflict(3, Some(2)).expect("older commit");
    }
}
