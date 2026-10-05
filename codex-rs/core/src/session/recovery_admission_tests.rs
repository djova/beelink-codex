use super::*;
use crate::session::SessionIo;
use crate::session::completed_session_loop_termination;
use crate::session::handlers::submission_loop;
use crate::session::submission::Submission;
use crate::session::tests::make_session_and_context_with_rx;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::turn_input::NotSubmittedReason;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::sync::oneshot;

fn endpoint(
    admission: Arc<RecoveryAdmission>,
    capacity: usize,
) -> (SessionIo, async_channel::Receiver<Submission>) {
    let (tx_sub, rx_sub) = async_channel::bounded(capacity);
    let (_, rx_event) = async_channel::unbounded();
    let io = SessionIo {
        tx_sub,
        recovery_admission: admission,
        rx_event,
        agent_status: tokio::sync::watch::channel(crate::agent::AgentStatus::PendingInit).1,
        session_loop_termination: completed_session_loop_termination(),
    };
    (io, rx_sub)
}

fn continuation(id: &str) -> Submission {
    Submission {
        id: id.into(),
        recovery_stamp: None,
        op: Op::TurnInput {
            request: Box::new(TurnInputRequest::user_input(Vec::new())),
            mode: TurnInputMode::ContinueIfIdle {
                expected_previous_turn_id: "failed".into(),
            },
            reply: oneshot::channel().0,
        },
        trace: None,
        parent_turn_id: None,
        root_turn_id: None,
        residency_guard: None,
    }
}

#[tokio::test]
async fn recovery_admission_settings_change_back_invalidates_before_dispatch() {
    let (io, rx) = endpoint(Arc::new(Default::default()), 4);
    io.submit_with_id(continuation("stable-op")).await.unwrap();
    let candidate = rx.recv().await.unwrap();
    assert!(
        io.recovery_admission
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_some()
    );
    for model in ["different-model", "original-model"] {
        io.submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(model.into()),
                ..Default::default()
            },
            reply: None,
        })
        .await
        .unwrap();
    }
    assert_eq!(io.recovery_admission.state.lock().await.generation, 2);
    assert!(
        io.recovery_admission
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn recovery_admission_cancelled_backpressured_send_still_invalidates() {
    let (io, rx) = endpoint(Arc::new(Default::default()), 1);
    io.submit_with_id(continuation("stable-op")).await.unwrap();
    // The queue is full; acceptance invalidates before waiting for its space.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), io.submit(Op::Interrupt))
            .await
            .is_err()
    );
    let candidate = rx.recv().await.unwrap();
    assert_eq!(io.recovery_admission.state.lock().await.generation, 1);
    assert!(
        io.recovery_admission
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_none()
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn recovery_admission_seal_excludes_concurrent_local_acceptance() {
    let (io, rx) = endpoint(Arc::new(Default::default()), 1);
    io.submit_with_id(continuation("stable-op")).await.unwrap();
    let candidate = rx.recv().await.unwrap();
    let seal = io
        .recovery_admission
        .seal(&candidate.id, candidate.recovery_stamp.as_ref())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), io.submit(Op::Interrupt))
            .await
            .is_err()
    );
    assert_eq!(seal.generation, 0);
    drop(seal);
    io.submit(Op::Interrupt).await.unwrap();
    assert_eq!(io.recovery_admission.state.lock().await.generation, 1);
    assert!(
        io.recovery_admission
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn recovery_admission_forwarding_preserves_original_acceptance_and_operation() {
    let admission = Arc::new(RecoveryAdmission::default());
    let (first, first_rx) = endpoint(Arc::clone(&admission), 2);
    let (forward, forward_rx) = endpoint(Arc::clone(&admission), 2);
    first.submit(Op::Interrupt).await.unwrap();
    forward
        .submit_with_id(first_rx.recv().await.unwrap())
        .await
        .unwrap();
    assert_eq!(admission.state.lock().await.generation, 1);
    forward_rx.recv().await.unwrap();
    first
        .submit_with_id(continuation("stable-op"))
        .await
        .unwrap();
    let candidate = first_rx.recv().await.unwrap();
    let original = candidate.recovery_stamp.clone();
    first.submit(Op::Interrupt).await.unwrap();
    forward.submit_with_id(candidate).await.unwrap();
    let forwarded = forward_rx.recv().await.unwrap();
    assert_eq!(forwarded.recovery_stamp, original);
    assert!(
        admission
            .seal(&forwarded.id, forwarded.recovery_stamp.as_ref())
            .await
            .is_none()
    );
    assert!(
        admission
            .seal("different-operation", original.as_ref())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn recovery_admission_new_runtime_rejects_retained_stamp_and_exhaustion_holds() {
    let (io, rx) = endpoint(Arc::new(Default::default()), 1);
    io.submit_with_id(continuation("stable-op")).await.unwrap();
    let candidate = rx.recv().await.unwrap();
    assert!(
        RecoveryAdmission::default()
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_none()
    );
    io.recovery_admission.state.lock().await.generation = u64::MAX;
    io.submit(Op::Interrupt).await.unwrap();
    assert!(io.recovery_admission.state.lock().await.exhausted);
    assert!(
        io.recovery_admission
            .seal(&candidate.id, candidate.recovery_stamp.as_ref())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn recovery_admission_actual_actor_holds_duplicate_operations_without_settings_or_start() {
    let (session, _, _) = make_session_and_context_with_rx().await;
    session.state.lock().await.last_started_turn_id = Some("failed".into());
    let before = session.thread_settings_snapshot().await;
    let (io, rx) = endpoint(Arc::clone(&session.recovery_admission), 4);
    let actor = tokio::spawn(submission_loop(
        Arc::clone(&session),
        session.get_config().await,
        rx,
    ));
    for _ in 0..2 {
        assert_eq!(
            io.submit_recover_turn(
                ThreadSettingsOverrides {
                    model: Some("must-not-apply".into()),
                    ..Default::default()
                },
                Default::default(),
                None,
                "failed".into(),
            )
            .await
            .unwrap(),
            TurnInputSubmission::NotSubmitted {
                reason: NotSubmittedReason::RecoveryInventoryUnknown
            }
        );
    }
    assert_eq!(
        io.submit_recover_turn(
            Default::default(),
            Default::default(),
            None,
            "stale-turn".into()
        )
        .await
        .unwrap(),
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::Superseded
        }
    );
    assert_eq!(session.thread_settings_snapshot().await, before);
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(
        session.state.lock().await.last_started_turn_id.as_deref(),
        Some("failed")
    );
    io.submit(Op::Shutdown).await.unwrap();
    actor.await.unwrap();
}

#[tokio::test]
async fn recovery_admission_actual_actor_rejects_later_accepted_cancellation_before_processing() {
    let (session, _, _) = make_session_and_context_with_rx().await;
    session.state.lock().await.last_started_turn_id = Some("failed".into());
    let (io, rx) = endpoint(Arc::clone(&session.recovery_admission), 4);
    let (reply, result) = oneshot::channel();
    let mut candidate = continuation("stable-op");
    if let Op::TurnInput { reply: target, .. } = &mut candidate.op {
        *target = reply;
    }
    io.submit_with_id(candidate).await.unwrap();
    io.submit(Op::Interrupt).await.unwrap();
    let actor = tokio::spawn(submission_loop(
        Arc::clone(&session),
        session.get_config().await,
        rx,
    ));
    assert_eq!(
        result.await.unwrap().unwrap(),
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::Superseded
        }
    );
    io.submit(Op::Shutdown).await.unwrap();
    actor.await.unwrap();
}
