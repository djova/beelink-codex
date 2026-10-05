//! Core-owned queue metadata; forwarding transfers the residency guard with the operation.

use codex_protocol::protocol::Op;
use codex_protocol::protocol::W3cTraceContext;
use tokio::sync::OwnedRwLockReadGuard;

#[derive(Debug)]
#[expect(dead_code, reason = "Turn ancestry is retained in Debug diagnostics.")]
pub(crate) struct Submission {
    pub id: String,
    /// Local acceptance stamp; never an interruption-ownership proof.
    pub recovery_stamp: Option<super::recovery_admission::RecoveryStamp>,
    /// Retired on real actor dispatch or dropped submission; never replay authority.
    pub admission_receipt: Option<super::recovery_admission::AcceptanceReceipt>,
    pub op: Op,
    /// Optional W3C trace carrier propagated across async submission handoffs.
    pub trace: Option<W3cTraceContext>,
    pub parent_turn_id: Option<String>,
    pub root_turn_id: Option<String>,
    /// Keeps a V2 recipient resident until this submission is handled or dropped.
    pub residency_guard: Option<OwnedRwLockReadGuard<()>>,
}
