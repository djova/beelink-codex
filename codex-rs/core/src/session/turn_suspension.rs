//! Suspension is held until complete runtime eligibility and durable ownership exist.

use super::session::Session;
use crate::state::TaskKind;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::turn_input::SuspendTurnOutcome;
use codex_protocol::turn_input::SuspendTurnTarget;
use std::sync::Arc;

pub(super) async fn suspend_turn_and_shutdown(
    session: &Arc<Session>,
    _submission_id: String,
    target: SuspendTurnTarget,
) -> CodexResult<SuspendTurnOutcome> {
    let active = session.active_turn.lock().await;
    let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
        return Ok(SuspendTurnOutcome::NotActive);
    };
    if task.kind != TaskKind::Regular {
        return Ok(SuspendTurnOutcome::UnsupportedTask);
    }
    if let SuspendTurnTarget::Expected { turn_id } = &target
        && turn_id != &task.turn_context.sub_id
    {
        return Ok(SuspendTurnOutcome::Superseded);
    }
    // A live-descendant snapshot and a connection's empty request list cannot
    // seal spawn, queue, approval or external-job admission. Never drop pending
    // input/waiters or cancel a task based on that incomplete evidence. The
    // startup reservation records inspected IDs, not atomic interruption ownership.
    Ok(SuspendTurnOutcome::RecoveryInventoryUnknown)
}

#[cfg(test)]
#[path = "turn_suspension_tests.rs"]
mod tests;
