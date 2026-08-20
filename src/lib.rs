//! The stage-facing surface of the BranchKit pipeline runtime, MIT-licensed so
//! pipeline stages can be authored out-of-tree without depending on the
//! proprietary actuator crate.
//!
//! [`stage`] is the entry point most stages want — it owns the handshake,
//! credit, and leniency obligations that sit above framing. The rest is the
//! surface it is built from, extracted verbatim from `branch_actuator::pipeline`
//! (which re-exports them at the old paths for its internal callers):
//!
//! - [`stage`] — the runtime: [`stage::serve_audio_consumer`] for read-driven
//!   stages, [`stage::serve_source`] for notifier-driven ones. The naming
//!   asymmetry is the wire's, not the module's — see that module's docs.
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

#[cfg(feature = "schema")]
pub mod codegen;
pub mod credit;
pub mod events;
pub mod grammar_dag;
#[cfg(feature = "schema")]
pub mod schema;
pub mod stage;
pub mod stage_log;
pub mod wire;

/// Environment variable carrying the platform's model-root directory, set by
/// the platform when it spawns a stage subprocess. A stage that loads models
/// resolves relative model names against this root (`<root>/<name>`) and must
/// NEVER re-derive the path itself: the sandbox profile grants read access to
/// the model dir by the path the PLATFORM computes, so a second spelling does
/// not merely disagree — it points the stage at a directory the profile
/// denies. Absent (e.g. a stage run by hand) → no platform model root; stages
/// fall back to explicit paths or their own overrides.
pub const MODELS_DIR_ENV: &str = "BRANCHKIT_MODELS_DIR";

/// Environment variable carrying the data directory a stage may write to, set
/// by the platform when it spawns a stage subprocess. This is the **owning
/// plugin's** namespace, shared with the plugin process itself — a stage
/// writes here and its plugin reads it back with ordinary file calls, which is
/// the whole point: the two are one plugin's worth of data, and the platform
/// should not have to carry it between them.
///
/// A built-in stage, which no plugin owns, gets a namespace of its own keyed by
/// its qualified name.
///
/// Same NEVER-re-derive rule as [`MODELS_DIR_ENV`], and it bites harder here
/// because the grant is a write: a stage that computes its own spelling of this
/// path gets a denial that reads exactly like a missing directory. The platform
/// creates the directory before the spawn, so it exists by the time a stage
/// looks. Absent (a stage run by hand) → no platform data root; a stage should
/// treat writing as unavailable rather than guessing a location.
pub const DATA_DIR_ENV: &str = "BRANCHKIT_STAGE_DATA";
