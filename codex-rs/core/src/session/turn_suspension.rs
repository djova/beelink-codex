use super::handlers;
use super::session::Session;
use crate::state::ActiveTurn;
use crate::state::TaskKind;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::SuspendTurnOutcome;
use codex_protocol::turn_input::SuspendTurnTarget;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub(super) async fn suspend_turn_and_shutdown(
    session: &Arc<Session>,
    submission_id: String,
    target: SuspendTurnTarget,
) -> CodexResult<SuspendTurnOutcome> {
    let inspected_turn_id = {
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
        task.turn_context.sub_id.clone()
    };

    // This is a snapshot of currently loaded descendants, not a spawn-admission seal.
    // Previously closed descendants and concurrent future spawns remain best effort.
    if session
        .services
        .local_agent_runtime
        .list_live_agent_subtree_thread_ids(session.thread_id)
        .await?
        .len()
        > 1
    {
        return Ok(SuspendTurnOutcome::HasLiveDescendants);
    }

    let live_thread = session
        .live_thread_for_persistence("suspend an unfinished root turn")
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
    // Flush before canceling execution so a persistence failure leaves the original turn running.
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush before root turn suspension failed: {error}"))
    })?;

    // The flush can yield while the active turn completes or changes. Recheck its
    // identity and kind under the same lock used to remove it. Even the legacy
    // unconditional API must not suspend a different turn after this yield.
    let mut turn = {
        let mut active = session.active_turn.lock().await;
        match take_inspected_turn(&mut active, &inspected_turn_id) {
            Ok(turn) => turn,
            Err(outcome) => return Ok(outcome),
        }
    };

    let task = turn.task.take().ok_or_else(|| {
        CodexErr::Fatal("accepted root turn suspension had no running task".to_string())
    })?;
    let turn_id = task.turn_context.sub_id.clone();
    // Normal shutdown records a terminal turn event, preventing another worker from
    // recovering this turn under its original ID. Cancel the task without that event.
    task.cancellation_token.cancel();
    task.turn_context
        .turn_metadata_state
        .cancel_git_enrichment_task();
    let mut task_handle = task.handle.detach();
    match tokio::time::timeout(
        Duration::from_millis(crate::tasks::GRACEFULL_INTERRUPTION_TIMEOUT_MS),
        &mut task_handle,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(thread_id = %session.thread_id, %error, "suspended turn task exited abnormally");
        }
        Err(_) => {
            warn!(
                thread_id = %session.thread_id,
                "suspended turn task did not stop gracefully; aborting it"
            );
            task_handle.abort();
            let _ = task_handle.await;
        }
    }
    // Pending accepted input and interactive waiters live only in this process. Handoff
    // intentionally drops that state; persisting or replaying it needs a separate protocol.
    session.input_queue.clear_pending(&turn).await;

    // Stop all producers before flushing their final history and closing its writer.
    // If either persistence step fails, do not report success: the current worker
    // retains ownership until worker-failure recovery can take responsibility.
    handlers::shutdown_session_runtime(session).await;
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush after root turn suspension failed: {error}"))
    })?;
    live_thread.shutdown().await.map_err(|error| {
        CodexErr::Fatal(format!("close suspended root turn writer failed: {error}"))
    })?;
    // Announce completion only after extension cleanup and writer closure so a
    // replacement worker cannot write the same thread concurrently.
    session
        .deliver_event_raw(Event {
            id: submission_id,
            msg: EventMsg::ShutdownComplete,
        })
        .await;
    Ok(SuspendTurnOutcome::Suspended { turn_id })
}

// The caller holds the active-turn lock throughout this comparison and removal.
fn take_inspected_turn(
    active: &mut Option<ActiveTurn>,
    inspected_turn_id: &str,
) -> Result<ActiveTurn, SuspendTurnOutcome> {
    let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
        return Err(SuspendTurnOutcome::NotActive);
    };
    if task.kind != TaskKind::Regular {
        return Err(SuspendTurnOutcome::UnsupportedTask);
    }
    if task.turn_context.sub_id != inspected_turn_id {
        return Err(SuspendTurnOutcome::Superseded);
    }
    active.take().ok_or(SuspendTurnOutcome::NotActive)
}

#[cfg(test)]
#[path = "turn_suspension_tests.rs"]
mod tests;
