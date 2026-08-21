//! Typed event constructors and constants for the pipeline wire vocabulary.
//!
//! These types ARE the contract: `contracts/pipeline.json` is generated from
//! them (`just gen-stage-proto`, drift-gated by `just check-stage-gen`) —
//! never the other way around. Conventions: snake-case event-type tags,
//! snake-case field names, `final` (Rust keyword) serialized via serde
//! rename. Every per-session event carries a `session_id`.
//!
//! These structs serialize *into* the `Event::data` slot, not the wire
//! envelope itself — the framing is in [`crate::wire`].

use serde::{Deserialize, Serialize};

/// Event-type tags — the closed wire vocabulary (projected into the
/// generated `contracts/pipeline.json` event catalog).
pub mod event_type {
    pub const CAPABILITY: &str = "capability";
    pub const AUDIO_START: &str = "audio_start";
    pub const AUDIO_CHUNK: &str = "audio_chunk";
    pub const AUDIO_STOP: &str = "audio_stop";
    pub const TRANSCRIPT: &str = "transcript";
    pub const FLOW_CREDIT: &str = "flow_credit";
    pub const ERROR: &str = "error";
    pub const VOCABULARY_UPDATE: &str = "vocabulary_update";

    pub const AUDIO_DEVICE_SNAPSHOT: &str = "audio_device_snapshot";
    pub const AUDIO_DEVICE_ADDED: &str = "audio_device_added";
    pub const AUDIO_DEVICE_REMOVED: &str = "audio_device_removed";
    pub const AUDIO_DEVICE_DEFAULT_CHANGED: &str = "audio_device_default_changed";

    pub const LOCATION_UPDATE: &str = "location_update";
    pub const LOCATION_ERROR: &str = "location_error";
    pub const HEADING_UPDATE: &str = "heading_update";

    pub const DISPLAY_SNAPSHOT: &str = "display_snapshot";
    pub const DISPLAY_ADDED: &str = "display_added";
    pub const DISPLAY_REMOVED: &str = "display_removed";
    pub const DISPLAY_CHANGED: &str = "display_changed";

    pub const POWER_SNAPSHOT: &str = "power_snapshot";
    pub const POWER_SOURCE_CHANGED: &str = "power_source_changed";

    /// Every tag above, in declaration order — the single list the schema
    /// projection and the conformance harness both check themselves against,
    /// so a new tag cannot reach one and miss the other. `schema.rs` asserts
    /// every member has a row (a missing row would otherwise regenerate
    /// `contracts/pipeline.json` byte-identical and sail through
    /// `just check-stage-gen`).
    pub const ALL: &[&str] = &[
        CAPABILITY,
        AUDIO_START,
        AUDIO_CHUNK,
        AUDIO_STOP,
        TRANSCRIPT,
        FLOW_CREDIT,
        ERROR,
        VOCABULARY_UPDATE,
        AUDIO_DEVICE_SNAPSHOT,
        AUDIO_DEVICE_ADDED,
        AUDIO_DEVICE_REMOVED,
        AUDIO_DEVICE_DEFAULT_CHANGED,
        LOCATION_UPDATE,
        LOCATION_ERROR,
        HEADING_UPDATE,
        DISPLAY_SNAPSHOT,
        DISPLAY_ADDED,
        DISPLAY_REMOVED,
        DISPLAY_CHANGED,
        POWER_SNAPSHOT,
        POWER_SOURCE_CHANGED,
    ];
}

/// Prefix of the custom-event namespace: `ext.<vendor>.<name>`.
///
/// The typed vocabulary above is closed — but a stage in a domain the
/// platform hasn't typed yet (gaze, HID, EMG, OCR, …) may emit events under
/// this namespace and the platform routes them opaquely onto its event bus,
/// stamped with the originating stage as the source, where plugins subscribe
/// to them like any other event (`consumes.events` pattern `ext.<vendor>.*`).
/// Rules:
/// - at least three non-empty dot segments (`ext.` alone or `ext.foo` is
///   invalid) — the vendor segment is what keeps two stages' vocabularies
///   from colliding; use a name you control
/// - `data` crosses the bus; a binary `payload` does NOT (the bus is JSON) —
///   the platform forwards `payload_length` in its place so the drop is
///   visible, and payloads remain valid wire-level for stage-to-stage use
/// - the namespace is reserved to stages: plugin emits of `ext.*` are
///   rejected, so a subscriber can trust the source attribution
/// - declare emitted types (or an `ext.<vendor>.*` glob) in
///   [`Capability::emits`]; the conformance harness enforces the declaration
///   and the platform logs undeclared emissions
pub const EXT_EVENT_PREFIX: &str = "ext.";

/// True when `event_type` is a well-formed custom event:
/// `ext.<vendor>.<name>` with non-empty segments (more segments allowed).
pub fn is_valid_ext_event_type(event_type: &str) -> bool {
    let Some(rest) = event_type.strip_prefix(EXT_EVENT_PREFIX) else {
        return false;
    };
    let mut segments = rest.split('.');
    let has_two = segments.next().is_some_and(|s| !s.is_empty())
        && segments.next().is_some_and(|s| !s.is_empty());
    has_two && rest.split('.').all(|s| !s.is_empty())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioFormat {
    pub rate: u32,
    pub width: u32,
    pub channels: u32,
}

impl AudioFormat {
    /// 16 kHz mono int16 PCM — the v1 baseline used by every shipping
    /// audio stage today (WhisperKit, the echo-stt fixture).
    pub const PCM_16K_MONO: AudioFormat = AudioFormat {
        rate: 16000,
        width: 2,
        channels: 1,
    };
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Capability {
    pub stage_type: String,
    pub stage_name: String,
    #[serde(default)]
    pub audio_formats: Vec<AudioFormat>,
    pub lifecycle_modes: Vec<String>,
    #[serde(default)]
    pub feature_flags: serde_json::Map<String, serde_json::Value>,
    /// Event types this stage emits — built-in tags (`transcript`,
    /// `power_snapshot`, …) and/or custom types under [`EXT_EVENT_PREFIX`]
    /// (an `ext.<vendor>.*` glob covers a vendor namespace). Advisory at
    /// runtime (the platform logs undeclared `ext.*` emissions rather than
    /// dropping them); enforced by the conformance harness. Empty = the
    /// stage declares nothing (pre-existing stages).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emits: Vec<String>,
    /// Custom event types this stage accepts from the stage ABOVE it in a
    /// pipeline — exact `ext.<vendor>.<name>` types, or `ext.<vendor>.*`
    /// globs.
    ///
    /// This is the stage-to-stage valve. The typed families (`audio_*`,
    /// `transcript`) always flow to the next stage and need no declaration;
    /// `ext.*` events do not, because the platform's default for a vocabulary
    /// it cannot decode is to hop it onto the event bus. Declaring a type here
    /// says "send it to me instead", which is what lets two stages agree on a
    /// vocabulary the platform knows nothing about — frames, MIDI, sensor
    /// readings, anything.
    ///
    /// Checked when the pipeline is wired: a declared type its upstream
    /// neighbour does not emit is a mis-wire and fails loudly, rather than
    /// leaving the stage waiting for something that can never arrive.
    ///
    /// Forwarded events are NOT also published to the bus — downstream
    /// delivery replaces it. Backpressure on them is the pipe's, not the
    /// credit window's: a flooding producer blocks on the OS buffer. The
    /// windowed credit that `audio_chunk` gets buys abort and visibility on
    /// top of that, and no custom vocabulary has needed it yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioStart {
    pub session_id: String,
    pub format: AudioFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioChunk {
    pub session_id: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioStop {
    pub session_id: String,
    /// Shared-clock position (the AudioChunk `timestamp_ms` timebase) after
    /// which buffered audio must NOT be processed. Set when the stop was
    /// triggered by something whose audio position is known — a recognized
    /// stop phrase, say — so that a consumer buffering ahead of the trigger
    /// does not process audio the user never meant to send.
    ///
    /// A source stage forwards it verbatim on its downstream AudioStop; a
    /// batch consumer truncates its buffer at it; absent = process
    /// everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutoff_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Transcript {
    pub session_id: String,
    pub text: String,
    pub partial: bool,
    #[serde(rename = "final")]
    pub is_final: bool,
    /// Coarse scalar confidence for the whole transcript: the MEAN of
    /// `word_scores` when the engine produces them, or its own scalar when it
    /// has no per-word signal.
    ///
    /// Deliberately coarse, and the reason matters — **a confidence GATE must
    /// read `word_scores` (the minimum), never this.** A mean hides a single
    /// badly-supported word: scores of [+8, -3] average to +2.5 and sail past
    /// a threshold the per-word data says should fail. For display and
    /// logging only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    /// Shared-clock onset (ms, the AudioChunk `timestamp_ms` timebase) of each
    /// word of `text`, aligned 1:1 with its whitespace-split words. Emitted by
    /// engines that produce alignment; omitted by those that do not.
    ///
    /// Because the timebase is the shared clock rather than a per-session
    /// counter, a consumer can convert a word position into an audio position
    /// that is meaningful to OTHER concurrently-running pipelines — which is
    /// what makes `AudioStop::cutoff_ms` possible across pipelines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_onsets_ms: Option<Vec<u64>>,
    /// Per-word acoustic score, aligned 1:1 with `text`'s whitespace-split
    /// words (same alignment contract as `word_onsets_ms`).
    ///
    /// Sign is the contract; magnitude is the engine's own scale. Positive
    /// means the audio supported the word. Negative means the decoder's
    /// language constraint outweighed weak acoustic evidence — the word was
    /// produced because the grammar allowed it, not because it was heard. A
    /// stage that computes these should document its own scale; a consumer
    /// that gates on them should read the MINIMUM, per `confidence` above.
    ///
    /// When present, `confidence` is the mean of these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_scores: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowCredit {
    pub session_id: String,
    pub frames: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ErrorEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub code: String,
    pub message: String,
    pub fatal: bool,
}

/// The `vocabulary_update` payload: the recognition word union plus the
/// optional grammar stamps the platform attaches at delivery time.
///
/// This struct is the typed contract; the actuator's producer side still
/// assembles the payload field-by-field (the three delivery-time grammar
/// stamps mutate the JSON per event), and an actuator-side test pins that
/// assembly to this shape so the two cannot drift silently. A consumer
/// stage should treat every field but `words` as optional and fall back
/// to the flat word list when a stamp is absent or malformed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct VocabularyUpdate {
    /// The full recognition word union (uppercased downstream to the model
    /// lexicon's casing by the consumer).
    pub words: Vec<String>,
    /// Exclusive-mode narrowing: when present, the engine grammar is built
    /// from THIS list instead of `words`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrow_to: Option<Vec<String>>,
    /// Sparse per-word decoding bias (Lever E): only biased words appear;
    /// everything else is neutral. Keyed by word identity so it cannot drift
    /// out of alignment with the separately-built union.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_weights: Option<std::collections::HashMap<String, f32>>,
    /// The structured word-level grammar DAG (D2), stamped when the
    /// structured-grammar toggle is on. Absent/null → the consumer falls back
    /// to the flat word-list grammar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grammar_dag: Option<crate::grammar_dag::GrammarDagWire>,
}

// ---- Device monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioDeviceInfo {
    pub device_id: u32,
    pub uid: String,
    pub name: String,
    pub is_input: bool,
    pub is_output: bool,
    pub is_default_input: bool,
    pub is_default_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioDeviceSnapshot {
    pub devices: Vec<AudioDeviceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioDeviceAdded {
    pub device: AudioDeviceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioDeviceRemoved {
    pub device_id: u32,
    pub uid: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AudioDeviceDefaultChanged {
    pub direction: String,
    pub device_id: u32,
    pub uid: String,
    pub name: String,
}

// ---- Location events ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LocationUpdate {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub horizontal_accuracy: f64,
    pub vertical_accuracy: f64,
    pub timestamp: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LocationError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeadingUpdate {
    pub magnetic_heading: f64,
    pub true_heading: f64,
    pub heading_accuracy: f64,
    pub timestamp: f64,
}

// ---- Display monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DisplayInfo {
    pub display_id: u32,
    pub width: u32,
    pub height: u32,
    pub refresh_rate: f64,
    pub scale_factor: f64,
    pub is_main: bool,
    pub is_builtin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DisplaySnapshot {
    pub displays: Vec<DisplayInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DisplayAdded {
    pub display: DisplayInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DisplayRemoved {
    pub display_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DisplayChanged {
    pub display: DisplayInfo,
}

// ---- Power monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PowerState {
    pub source: String,
    pub battery_level: Option<f64>,
    pub is_charging: bool,
    pub time_to_empty: Option<i64>,
    pub time_to_full: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PowerSnapshot {
    pub state: PowerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PowerSourceChanged {
    pub state: PowerState,
}

/// Mint a fresh session ID. RFC 4122 v4 UUID derived from `getrandom`.
pub fn new_session_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("getrandom failed minting session_id");
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transcript_serializes_final_field_as_final_keyword() {
        let t = Transcript {
            session_id: "s".into(),
            text: "hi".into(),
            partial: false,
            is_final: true,
            confidence: None,
            word_onsets_ms: None,
            word_scores: None,
        };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v["final"], serde_json::json!(true));
        assert!(v.get("is_final").is_none());
    }

    #[test]
    fn session_id_is_uuid_v4_shape() {
        let id = new_session_id();
        // Length: 36 chars (32 hex + 4 dashes).
        assert_eq!(id.len(), 36);
        // Version nibble is '4'.
        assert_eq!(id.as_bytes()[14], b'4');
        // Variant nibble is one of 8/9/a/b.
        let variant = id.as_bytes()[19];
        assert!(
            matches!(variant, b'8' | b'9' | b'a' | b'b'),
            "variant nibble: {}",
            variant as char
        );
        // Two distinct calls must differ.
        assert_ne!(id, new_session_id());
    }

    #[test]
    fn error_event_omits_session_id_when_none() {
        let e = ErrorEvent {
            session_id: None,
            code: "unsupported_format".into(),
            message: "bad".into(),
            fatal: true,
        };
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("session_id").is_none());
    }

    #[test]
    fn ext_event_type_requires_vendor_and_name_segments() {
        assert!(is_valid_ext_event_type("ext.acme.gaze_point"));
        assert!(is_valid_ext_event_type("ext.acme.gaze.left_eye"));
        assert!(!is_valid_ext_event_type("ext."));
        assert!(!is_valid_ext_event_type("ext.acme"));
        assert!(!is_valid_ext_event_type("ext.acme."));
        assert!(!is_valid_ext_event_type("ext..gaze"));
        assert!(!is_valid_ext_event_type("extacme.gaze"));
        assert!(!is_valid_ext_event_type("transcript"));
    }

    #[test]
    fn capability_omits_empty_emits_and_roundtrips_declared() {
        let cap = Capability {
            stage_type: "source".into(),
            stage_name: "t".into(),
            audio_formats: vec![],
            lifecycle_modes: vec!["persistent".into()],
            feature_flags: serde_json::Map::new(),
            emits: vec![],
            consumes: vec![],
        };
        let v = serde_json::to_value(&cap).unwrap();
        assert!(v.get("emits").is_none(), "empty emits must be omitted");
        assert!(
            v.get("consumes").is_none(),
            "empty consumes must be omitted — a stage that declares nothing must \
             serialize exactly as it did before the valve existed"
        );
        // Old capability payloads (no emits key) still decode.
        let old: Capability = serde_json::from_value(v).unwrap();
        assert!(old.emits.is_empty());

        let declared = Capability {
            emits: vec!["power_snapshot".into(), "ext.acme.*".into()],
            ..cap
        };
        let v = serde_json::to_value(&declared).unwrap();
        assert_eq!(
            v["emits"],
            serde_json::json!(["power_snapshot", "ext.acme.*"])
        );
    }

    #[test]
    fn pcm_16k_mono_baseline_constants() {
        let f = AudioFormat::PCM_16K_MONO;
        assert_eq!(f.rate, 16000);
        assert_eq!(f.width, 2);
        assert_eq!(f.channels, 1);
    }
}
