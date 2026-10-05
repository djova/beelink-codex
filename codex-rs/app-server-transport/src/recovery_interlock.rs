//! Fail-closed managed restart admission. Reservations never authorize execution.
//! A retained legacy, corrupt or unverified handoff must be reconciled, not consumed.

use crate::daemon_recovery::RecoverySnapshot;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::fd::FromRawFd;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

fn hold() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "retained recovery work requires reconciliation",
    )
}

/// Presence, including unreadable or malformed state, is a hold for every package.
/// This check is not a lifetime lease; managed server admission also acquires one.
pub fn require_no_pending(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(hold()),
        Err(error) => Err(error),
    }
}

/// One process-lifetime owner for the stable recovery directory inode.
/// Directory replacement is detected before persistence; privileged namespace
/// replacement and unsupported external writers are not sealed by this lease.
pub struct RecoveryStartupLease {
    directory: File,
    path: PathBuf,
    owner_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InterruptionReservation {
    pub schema_version: u32,
    pub owner_id: Uuid,
    pub operation_id: Uuid,
    pub process_id: u32,
    pub source_device: u64,
    pub source_inode: u64,
    /// These are inspected identities, not an assertion of atomic interruption.
    pub inspected_turns: BTreeMap<String, Option<String>>,
    pub phase: ReservationPhase,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReservationPhase {
    /// Input/settings/approval/job/outcome fences are unproven. Never resume it.
    ReservedUnknown,
}

impl RecoveryStartupLease {
    pub fn acquire(path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let parent = path.parent().ok_or_else(hold)?;
            std::fs::create_dir_all(parent)?;
            let directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(parent)?;
            if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let lease = Self {
                directory,
                path: path.to_path_buf(),
                owner_id: Uuid::now_v7(),
            };
            lease.check_directory()?;
            require_no_pending(path)?;
            Ok(lease)
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recovery lease unavailable",
            ))
        }
    }

    fn check_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let held = self.directory.metadata()?;
            let fresh = std::fs::symlink_metadata(self.path.parent().ok_or_else(hold)?)?;
            if !fresh.is_dir() || (held.dev(), held.ino()) != (fresh.dev(), fresh.ino()) {
                return Err(hold());
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(hold())
        }
    }

    #[cfg(unix)]
    fn open_reserved_file(&self, flags: libc::c_int) -> io::Result<File> {
        let name = std::ffi::CString::new(self.path.file_name().ok_or_else(hold)?.as_bytes())
            .map_err(|_| hold())?;
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    /// Reserve exact inspected IDs before shutdown effects. Partial writes and
    /// fsync failures retain a hold. No overwrite, delete, or continuation exists.
    pub fn reserve_interruption(
        &self,
        operation_id: Uuid,
        snapshot: &RecoverySnapshot,
    ) -> io::Result<InterruptionReservation> {
        self.check_directory()?;
        if operation_id.is_nil()
            || snapshot
                .interrupted
                .keys()
                .any(|id| !snapshot.loaded.contains(id))
            || snapshot
                .loaded
                .iter()
                .any(|id| id.is_empty() || id.len() > 128)
            || snapshot
                .interrupted
                .values()
                .any(|turn| turn.turn_id.is_empty() || turn.turn_id.len() > 128)
        {
            return Err(hold());
        }
        #[cfg(unix)]
        let source = File::open(std::env::current_exe()?)?.metadata()?;
        let reservation = InterruptionReservation {
            schema_version: 1,
            owner_id: self.owner_id,
            operation_id,
            process_id: std::process::id(),
            #[cfg(unix)]
            source_device: source.dev(),
            #[cfg(unix)]
            source_inode: source.ino(),
            #[cfg(not(unix))]
            source_device: 0,
            #[cfg(not(unix))]
            source_inode: 0,
            inspected_turns: snapshot
                .loaded
                .iter()
                .map(|id| {
                    (
                        id.clone(),
                        snapshot
                            .interrupted
                            .get(id)
                            .map(|turn| turn.turn_id.clone()),
                    )
                })
                .collect(),
            phase: ReservationPhase::ReservedUnknown,
        };
        let bytes = serde_json::to_vec(&reservation).map_err(io::Error::other)?;
        if bytes.len() > 64 * 1024 {
            return Err(hold());
        }
        #[cfg(unix)]
        let created = self.open_reserved_file(libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL);
        #[cfg(not(unix))]
        let created: io::Result<File> = Err(hold());
        match created {
            Ok(mut file) => {
                file.write_all(&bytes)?;
                file.sync_all()?;
                self.directory.sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if read_reservation(&self.path)? != reservation {
                    return Err(hold());
                }
                // A previous response may have been lost after creation. Re-sync
                // before reporting the same operation, never start a replacement ID.
                #[cfg(unix)]
                self.open_reserved_file(libc::O_RDONLY)?.sync_all()?;
                self.directory.sync_all()?;
            }
            Err(error) => return Err(error),
        }
        self.check_directory()?;
        Ok(reservation)
    }
}

/// Strict bounded reconciliation only. Parsing never authorizes startup or replay.
pub fn read_reservation(path: &Path) -> io::Result<InterruptionReservation> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(hold());
    }
    let mut bytes = Vec::new();
    file.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err(hold());
    }
    let value: InterruptionReservation = serde_json::from_slice(&bytes).map_err(|_| hold())?;
    if value.schema_version != 1
        || value.owner_id.is_nil()
        || value.operation_id.is_nil()
        || value.process_id == 0
        || value.source_inode == 0
    {
        return Err(hold());
    }
    Ok(value)
}

#[cfg(all(test, unix))]
#[path = "recovery_interlock_tests.rs"]
mod tests;
