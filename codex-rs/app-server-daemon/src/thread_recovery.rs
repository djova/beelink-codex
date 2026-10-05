//! Common managed start/replacement/rollback hold. Never discard retained work.

use anyhow::Context;
use anyhow::Result;

use crate::Daemon;

pub(crate) fn require_clear(daemon: &Daemon) -> Result<()> {
    codex_app_server_transport::recovery_interlock::require_no_pending(&daemon.recovery_file()?)
        .context("managed launch held: retained recovery work requires reconciliation")
}
