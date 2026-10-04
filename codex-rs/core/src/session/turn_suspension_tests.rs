use super::*;
use crate::session::tests::make_session_and_context_with_rx;
use crate::tasks::SessionTask;
use crate::tasks::SessionTaskResult;
use codex_protocol::protocol::TurnAbortReason;
use pretty_assertions::assert_eq;
use tokio_util::sync::CancellationToken;

struct WaitingTask;

impl SessionTask for WaitingTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.suspension_fixture"
    }

    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _context: Arc<super::super::TurnContext>,
        _input: Vec<crate::session::TurnInput>,
        cancellation: CancellationToken,
    ) -> SessionTaskResult {
        cancellation.cancelled().await;
        Ok(None)
    }
}

#[tokio::test]
async fn suspension_after_flush_rejects_replacement_turn_without_taking_or_cancelling_it() {
    let (session, mut context, _events) = make_session_and_context_with_rx().await;
    Arc::get_mut(&mut context).unwrap().sub_id = "new-manual-turn".into();
    session
        .spawn_task(Arc::clone(&context), Vec::new(), WaitingTask)
        .await;
    // Model the exact lock boundary after the pre-suspension flush has yielded
    // and a replacement task became active. The production removal uses this
    // same function while holding the same active-turn lock.
    {
        let mut active = session.active_turn.lock().await;
        let task = active.as_ref().unwrap().task.as_ref().unwrap();
        let cancellation = task.cancellation_token.clone();
        let task_context = Arc::clone(&task.turn_context);
        assert_eq!(
            take_inspected_turn(&mut active, "inspected-old-turn").err(),
            Some(SuspendTurnOutcome::Superseded),
        );
        let preserved = active.as_ref().unwrap().task.as_ref().unwrap();
        assert!(Arc::ptr_eq(&preserved.turn_context, &task_context));
        assert!(!cancellation.is_cancelled());
    }
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}
