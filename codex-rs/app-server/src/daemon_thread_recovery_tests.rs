use super::*;
use codex_app_server_transport::recovery_interlock::ReservationPhase;
use codex_app_server_transport::recovery_interlock::read_reservation;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

#[tokio::test]
async fn restart_interlock_common_entrypoint_fixture() {
    if std::env::var_os("CODEX_RESTART_ADMISSION_FIXTURE").is_none() {
        return;
    }
    let home = std::path::PathBuf::from(std::env::var_os("CODEX_HOME").unwrap());
    let socket = home.join("must-not-listen.sock");
    let result = crate::run_main_with_transport_options(
        Default::default(),
        Default::default(),
        Default::default(),
        /*strict_config*/ false,
        /*default_analytics_enabled*/ false,
        crate::AppServerTransport::UnixSocket {
            socket_path: codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(&socket)
                .unwrap(),
        },
        codex_protocol::protocol::SessionSource::Cli,
        codex_websocket_auth::WebsocketAuthSettings::default(),
        crate::AppServerRuntimeOptions {
            managed_daemon: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert!(!socket.exists());
}

#[test]
fn restart_interlock_real_entrypoint_retains_legacy_corrupt_and_unknown_state() {
    for bytes in [b"[\"thread\"]".as_slice(), b"{", b""] {
        let home = TempDir::new().unwrap();
        let path = codex_app_server_transport::daemon_recovery_file_path(home.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "daemon_thread_recovery::tests::restart_interlock_common_entrypoint_fixture",
                "--nocapture",
            ])
            .env("CODEX_RESTART_ADMISSION_FIXTURE", "1")
            .env("CODEX_HOME", home.path())
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[tokio::test]
async fn restart_interlock_snapshot_ack_requires_durable_unknown_reservation() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("handoff.json");
    let owner = Arc::new(RecoveryStartupLease::acquire(&path).unwrap());
    let saved = daemon_recovery::RecoverySnapshot {
        loaded: ["synthetic-thread".to_owned()].into_iter().collect(),
        ..Default::default()
    };
    snapshot(Arc::clone(&owner), saved).await.unwrap();
    let reservation = read_reservation(&path).unwrap();
    assert_eq!(reservation.phase, ReservationPhase::ReservedUnknown);
    assert_eq!(reservation.inspected_turns.len(), 1);
    guard_internal_exit(/*durable*/ true).await;
    drop(owner);
    assert!(RecoveryStartupLease::acquire(&path).is_err());
}

#[tokio::test]
async fn restart_interlock_error_exit_retains_owner_and_partial_state() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("handoff.json");
    let owner = Arc::new(RecoveryStartupLease::acquire(&path).unwrap());
    std::fs::write(&path, b"{").unwrap();
    let durable = snapshot(Arc::clone(&owner), Default::default())
        .await
        .is_ok();
    assert!(!durable);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            guard_internal_exit(durable),
        )
        .await
        .is_err()
    );
    assert!(RecoveryStartupLease::acquire(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"{");
}
