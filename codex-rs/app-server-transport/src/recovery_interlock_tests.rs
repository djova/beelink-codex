use super::*;
use crate::daemon_recovery::InterruptedTurn;
use pretty_assertions::assert_eq;
use std::process::Command;
use tempfile::TempDir;

fn snapshot() -> RecoverySnapshot {
    RecoverySnapshot {
        loaded: ["synthetic-thread".to_owned()].into_iter().collect(),
        interrupted: [(
            "synthetic-thread".to_owned(),
            InterruptedTurn {
                turn_id: "exact-turn".to_owned(),
                output_schema: None,
                service_tier: None,
                cyber_access_program: None,
                local_environment: None,
            },
        )]
        .into_iter()
        .collect(),
    }
}

fn child(path: &Path, phase: &str) -> io::Result<std::process::ExitStatus> {
    Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "recovery_interlock::tests::restart_interlock_process_fixture",
            "--nocapture",
        ])
        .env("CODEX_RESTART_FIXTURE_PATH", path)
        .env("CODEX_RESTART_FIXTURE_PHASE", phase)
        .status()
}

#[test]
fn restart_interlock_process_fixture() {
    let Some(path) = std::env::var_os("CODEX_RESTART_FIXTURE_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let phase = std::env::var("CODEX_RESTART_FIXTURE_PHASE").unwrap();
    if phase == "competing" {
        assert!(RecoveryStartupLease::acquire(&path).is_err());
        std::process::exit(24);
    }
    let owner = RecoveryStartupLease::acquire(&path).unwrap();
    match phase.as_str() {
        "before-create" => {}
        "partial-create" => {
            let mut file = owner
                .open_reserved_file(libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL)
                .unwrap();
            file.write_all(b"{").unwrap();
            file.sync_all().unwrap();
            owner.directory.sync_all().unwrap();
        }
        "committed" => {
            owner
                .reserve_interruption(Uuid::now_v7(), &snapshot())
                .unwrap();
        }
        _ => panic!("unknown synthetic phase"),
    }
    // Simulated process loss: neither Rust Drop nor a recovery acknowledgement runs.
    std::process::exit(24);
}

#[test]
fn restart_interlock_single_owner_blocks_competing_startup_process() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("handoff.json");
    let owner = RecoveryStartupLease::acquire(&path).unwrap();
    assert!(RecoveryStartupLease::acquire(&path).is_err());
    assert_eq!(child(&path, "competing").expect("fixture child").code(), Some(24));
    drop(owner);
    assert!(RecoveryStartupLease::acquire(&path).is_ok());
}

#[test]
fn restart_interlock_crash_boundaries_retain_unknown_or_committed_work() {
    for phase in ["before-create", "partial-create", "committed"] {
        let root = TempDir::new().unwrap();
        let path = root.path().join("handoff.json");
        assert_eq!(child(&path, phase).expect("fixture child").code(), Some(24));
        if phase == "before-create" {
            assert!(!path.exists());
            assert!(RecoveryStartupLease::acquire(&path).is_ok());
        } else {
            let bytes = std::fs::read(&path).unwrap();
            assert!(RecoveryStartupLease::acquire(&path).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            if phase == "committed" {
                assert_eq!(
                    read_reservation(&path).unwrap().phase,
                    ReservationPhase::ReservedUnknown
                );
            } else {
                assert!(read_reservation(&path).is_err());
            }
        }
    }
}

#[test]
fn restart_interlock_exact_owner_operation_reconciles_without_replacement() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("handoff.json");
    let owner = RecoveryStartupLease::acquire(&path).unwrap();
    let operation = Uuid::now_v7();
    let saved = owner.reserve_interruption(operation, &snapshot()).unwrap();
    assert_eq!(saved.owner_id, owner.owner_id);
    assert_eq!(saved.operation_id, operation);
    assert_eq!(
        saved.inspected_turns,
        [("synthetic-thread".to_owned(), Some("exact-turn".to_owned()))]
            .into_iter()
            .collect()
    );
    assert_eq!(read_reservation(&path).unwrap(), saved);
    assert_eq!(
        owner.reserve_interruption(operation, &snapshot()).unwrap(),
        saved
    );
    let bytes = std::fs::read(&path).unwrap();
    assert!(
        owner
            .reserve_interruption(Uuid::now_v7(), &snapshot())
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    drop(owner);
    assert!(RecoveryStartupLease::acquire(&path).is_err());
}

#[test]
fn restart_interlock_legacy_corrupt_and_future_state_is_not_consumed() {
    for bytes in [
        b"[\"old-thread\"]".as_slice(),
        b"",
        b"{\"schema_version\":999}",
        b"null",
    ] {
        let root = TempDir::new().unwrap();
        let path = root.path().join("handoff.json");
        std::fs::write(&path, bytes).unwrap();
        assert!(require_no_pending(&path).is_err());
        assert!(RecoveryStartupLease::acquire(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn restart_interlock_changed_directory_or_invalid_identity_cannot_reserve() {
    let root = TempDir::new().unwrap();
    let directory = root.path().join("state");
    let path = directory.join("handoff.json");
    let owner = RecoveryStartupLease::acquire(&path).unwrap();
    assert!(
        owner
            .reserve_interruption(Uuid::nil(), &snapshot())
            .is_err()
    );
    let mut mismatched = snapshot();
    mismatched.loaded.clear();
    assert!(
        owner
            .reserve_interruption(Uuid::now_v7(), &mismatched)
            .is_err()
    );
    assert!(!path.exists());
    std::fs::rename(&directory, root.path().join("old-state")).unwrap();
    std::fs::create_dir(&directory).unwrap();
    assert!(
        owner
            .reserve_interruption(Uuid::now_v7(), &snapshot())
            .is_err()
    );
    assert!(!path.exists());
}
