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
//! - [`grammar_dag`] — the wire projection of the command-grammar DAG a
//!   recognition stage decodes (the builder stays in the actuator).
//! - [`credit`] — receiver-side flow-credit granting (the counter + emission
//!   mechanism; window sizes stay per-stage policy).
//!
//! Boundary and roadmap: `notes/DESIGN_STAGE_SDK.md` in the app repo.

pub mod credit;
pub mod events;
pub mod grammar_dag;
#[cfg(feature = "schema")]
pub mod schema;
pub mod stage_log;
pub mod wire;
