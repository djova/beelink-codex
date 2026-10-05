//! Durable inspected interruption reservations, never executable recovery claims.

use codex_app_server_transport::daemon_recovery;
use codex_app_server_transport::recovery_interlock::RecoveryStartupLease;
use std::io;
use std::sync::Arc;

pub(crate) async fn snapshot(
    owner: Arc<RecoveryStartupLease>,
    saved: daemon_recovery::RecoverySnapshot,
) -> io::Result<()> {
    // A forced exit must not wait for file I/O in Tokio's blocking pool.
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("daemon-snapshot".into())
        .spawn(move || {
            let result = owner
                .reserve_interruption(uuid::Uuid::now_v7(), &saved)
                .map(|_| ());
            let _ = result_tx.send(result);
        })?;
    result_rx.await.map_err(io::Error::other)?
}

/// Channel/router exits must meet the same durable boundary as requested exits.
/// Retain the process owner on failure; no deletion, acknowledgement or replay.
pub(crate) async fn guard_internal_exit(durable: bool) {
    if !durable {
        tracing::error!("managed exit held: interruption reservation is not durable");
        std::future::pending::<()>().await;
    }
}

#[cfg(all(test, unix))]
#[path = "daemon_thread_recovery_tests.rs"]
mod tests;
