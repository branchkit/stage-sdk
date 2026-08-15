//! The stage-facing surface of the BranchKit pipeline runtime, MIT-licensed so
//! pipeline stages can be authored out-of-tree without depending on the
//! proprietary actuator crate.
//!
//! Three modules, extracted verbatim from `branch_actuator::pipeline` (which
//! re-exports them at the old paths for its internal callers):
//!
//! - [`wire`] — the framing: one JSON header line, optional binary payload.
//! - [`events`] — the typed event vocabulary that serializes into
//!   `Event::data` (audio, transcript, flow credit, device/location/display/
//!   power families).
//! - [`stage_log`] — the `BKLOG1` stderr line protocol the platform parses
//!   into per-stage, per-session log records.
//!
//! Boundary and roadmap: `notes/DESIGN_STAGE_SDK.md` in the app repo.

pub mod events;
pub mod stage_log;
pub mod wire;
