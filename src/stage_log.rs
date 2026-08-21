//! Shared stage logging — one canonical diagnostic format for every
//! pipeline stage, replacing the per-stage `slog!` / `vlog!` / bare
//! `eprintln!` divergence.
//!
//! Stages write on stderr; the actuator's stage stderr reader
//! (`pipeline::stage`) parses the `BKLOG1` sentinel format into correlated
//! `stage.diagnostic` bus events. Lines without the sentinel — native-library output, panics, a
//! not-yet-migrated stage — still reach the bus via the reader's generic
//! fallback, just uncorrelated and at `info`.
//!
//! Levels mirror the platform's five-level model (`trace`/`debug`/`info`/
//! `warn`/`error`). The session id is **ambient**: a stage calls
//! [`set_session`] on `audio_start` and [`clear_session`] at session end, and
//! every log call stamps the live session automatically — so a line
//! correlates to the command's `tr_` (derived from the session id) without
//! threading the id through every call site. Pooled stages just re-`set_session`
//! on each `audio_start`.

use std::sync::Mutex;

/// Sentinel + version prefix every structured line begins with. The actuator
/// reader (`pipeline::stage`) splits on this; it is the single source of truth
/// for the wire format `BKLOG1\t<level>\t<session_id>\t<message>`.
pub const LINE_PREFIX: &str = "BKLOG1\t";

static CURRENT_SESSION: Mutex<Option<String>> = Mutex::new(None);

/// Set the ambient session id (call on `audio_start`). Subsequent log lines
/// carry it so they correlate to this command.
pub fn set_session(session_id: &str) {
    *CURRENT_SESSION.lock().unwrap() = Some(session_id.to_string());
}

/// Clear the ambient session id (call at session end). Lines emitted after
/// this carry no session — correct for between-command output.
pub fn clear_session() {
    *CURRENT_SESSION.lock().unwrap() = None;
}

/// Emit one structured diagnostic line on stderr. Prefer the level helpers
/// ([`info`], [`warn`], …) at call sites. `message` may contain tabs (it is
/// the final, unbounded field); embedded newlines are flattened to spaces so
/// one logical diagnostic stays one line.
pub fn log(level: &str, message: &str) {
    let session = CURRENT_SESSION.lock().unwrap().clone().unwrap_or_default();
    let message = if message.contains(['\n', '\r']) {
        message.replace(['\n', '\r'], " ")
    } else {
        message.to_string()
    };
    eprintln!("{LINE_PREFIX}{level}\t{session}\t{message}");
}

pub fn trace(message: &str) {
    log("trace", message);
}
pub fn debug(message: &str) {
    log("debug", message);
}
pub fn info(message: &str) {
    log("info", message);
}
pub fn warn(message: &str) {
    log("warn", message);
}
pub fn error(message: &str) {
    log("error", message);
}
