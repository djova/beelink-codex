//! Acceptance-time local fencing, not a complete recovery authority.
//!
//! Queue stores, child creation, background jobs and request callback ownership
//! are not sealed here. Recovery therefore remains held in the production path.

use codex_protocol::protocol::Op;
use codex_protocol::turn_input::TurnInputMode;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct RecoveryAdmission {
    state: Arc<Mutex<AdmissionState>>,
}

#[derive(Debug)]
pub(crate) struct AdmissionState {
    incarnation: Uuid,
    generation: u64,
    exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryStamp {
    incarnation: Uuid,
    generation: u64,
    operation_id: String,
}

impl Default for RecoveryAdmission {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(AdmissionState {
                incarnation: Uuid::now_v7(),
                generation: 0,
                exhausted: false,
            })),
        }
    }
}

impl RecoveryAdmission {
    /// Invalidate before bounded-channel send, without holding the seal while
    /// waiting for channel space. A failed/dropped send conservatively invalidates;
    /// generations never roll back, including when settings change back.
    pub(crate) async fn accept(
        &self,
        operation_id: &str,
        op: &Op,
        stamp: &mut Option<RecoveryStamp>,
    ) {
        let mut state = self.state.lock().await;
        if is_recovery(op) {
            // Delegate forwarding preserves the original acceptance boundary.
            if stamp.is_none() {
                *stamp = Some(RecoveryStamp {
                    incarnation: state.incarnation,
                    generation: state.generation,
                    operation_id: operation_id.to_owned(),
                });
            }
        } else if let Some(next) = state.generation.checked_add(1) {
            if stamp.as_ref().is_some_and(|stamp| {
                stamp.incarnation == state.incarnation && stamp.operation_id == operation_id
            }) {
                return;
            }
            state.generation = next;
            *stamp = Some(RecoveryStamp {
                incarnation: state.incarnation,
                generation: next,
                operation_id: operation_id.to_owned(),
            });
        } else {
            state.exhausted = true;
        }
    }

    /// The returned seal excludes further local acceptance until commit/hold.
    /// A fresh session incarnation never accepts an old runtime's stamp.
    pub(crate) async fn seal(
        &self,
        operation_id: &str,
        stamp: Option<&RecoveryStamp>,
    ) -> Option<OwnedMutexGuard<AdmissionState>> {
        let state = Arc::clone(&self.state).lock_owned().await;
        let stamp = stamp?;
        (!state.exhausted
            && stamp.incarnation == state.incarnation
            && stamp.generation == state.generation
            && stamp.operation_id == operation_id)
            .then_some(state)
    }
}

pub(crate) fn is_recovery(op: &Op) -> bool {
    matches!(
        op,
        Op::RecoverTurn { .. }
            | Op::SuspendTurnAndShutdown { .. }
            | Op::TurnInput {
                mode: TurnInputMode::ContinueIfIdle { .. },
                ..
            }
    )
}

#[cfg(test)]
#[path = "recovery_admission_tests.rs"]
mod tests;
