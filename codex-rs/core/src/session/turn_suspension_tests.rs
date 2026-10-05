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
async fn recovery_admission_suspension_holds_unknown_and_preserves_exact_task() {
    let (session, mut context, _events) = make_session_and_context_with_rx().await;
    Arc::get_mut(&mut context).unwrap().sub_id = "new-manual-turn".into();
    session
        .spawn_task(Arc::clone(&context), Vec::new(), WaitingTask)
        .await;
    let cancellation = session
        .active_turn
        .lock()
        .await
        .as_ref()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .cancellation_token
        .clone();
    assert_eq!(
        suspend_turn_and_shutdown(
            &session,
            "owned-op".into(),
            SuspendTurnTarget::Expected {
                turn_id: "inspected-old-turn".into()
            }
        )
        .await
        .unwrap(),
        SuspendTurnOutcome::Superseded,
    );
    assert_eq!(
        suspend_turn_and_shutdown(
            &session,
            "owned-op".into(),
            SuspendTurnTarget::Expected {
                turn_id: "new-manual-turn".into()
            }
        )
        .await
        .unwrap(),
        SuspendTurnOutcome::RecoveryInventoryUnknown,
    );
    assert!(!cancellation.is_cancelled());
    assert_eq!(
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .task
            .as_ref()
            .unwrap()
            .turn_context
            .sub_id,
        "new-manual-turn"
    );
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
}
