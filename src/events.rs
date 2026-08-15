//! Typed event constructors and constants for the pipeline wire vocabulary.
//!
//! Mirrors `contracts/pipeline.json` exactly: snake-case event-type tags,
//! snake-case field names, `final` (Rust keyword) is serialized as `final`
//! via serde rename. Every per-session event carries a `session_id`
//! (section 1's wire-types update).
//!
//! These structs serialize *into* the `Event::data` slot, not the wire
//! envelope itself — the framing is in [`super::wire`].

use serde::{Deserialize, Serialize};

/// Event-type tags. Matches the closed vocabulary in
/// `contracts/pipeline.json`.
pub mod event_type {
    pub const CAPABILITY: &str = "capability";
    pub const AUDIO_START: &str = "audio_start";
    pub const AUDIO_CHUNK: &str = "audio_chunk";
    pub const AUDIO_STOP: &str = "audio_stop";
    pub const TRANSCRIPT: &str = "transcript";
    pub const FLOW_CREDIT: &str = "flow_credit";
    pub const ERROR: &str = "error";
    pub const VOCABULARY_UPDATE: &str = "vocabulary_update";

    pub const DEVICE_SNAPSHOT: &str = "device_snapshot";
    pub const DEVICE_ADDED: &str = "device_added";
    pub const DEVICE_REMOVED: &str = "device_removed";
    pub const DEFAULT_DEVICE_CHANGED: &str = "default_device_changed";

    pub const LOCATION_UPDATE: &str = "location_update";
    pub const LOCATION_ERROR: &str = "location_error";
    pub const HEADING_UPDATE: &str = "heading_update";

    pub const DISPLAY_SNAPSHOT: &str = "display_snapshot";
    pub const DISPLAY_ADDED: &str = "display_added";
    pub const DISPLAY_REMOVED: &str = "display_removed";
    pub const DISPLAY_CHANGED: &str = "display_changed";

    pub const POWER_SNAPSHOT: &str = "power_snapshot";
    pub const POWER_SOURCE_CHANGED: &str = "power_source_changed";
    pub const SYSTEM_WILL_SLEEP: &str = "system_will_sleep";
    pub const SYSTEM_DID_WAKE: &str = "system_did_wake";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub stage_type: String,
    pub stage_name: String,
    #[serde(default)]
    pub audio_formats: Vec<AudioFormat>,
    pub lifecycle_modes: Vec<String>,
    #[serde(default)]
    pub feature_flags: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioStart {
    pub session_id: String,
    pub format: AudioFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioChunk {
    pub session_id: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioStop {
    pub session_id: String,
    /// Shared-clock position (the AudioChunk `timestamp_ms` timebase) after
    /// which buffered audio must NOT be processed. Set when the stop was
    /// triggered by an event whose audio position is known — the dictation
    /// stop phrase (notes/DESIGN_DICTATION_AUDIO_CUTOFF.md). A source stage
    /// forwards it verbatim on its downstream AudioStop; batch consumers
    /// (whisperkit) truncate their buffer at it; absent = process everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutoff_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transcript {
    pub session_id: String,
    pub text: String,
    pub partial: bool,
    #[serde(rename = "final")]
    pub is_final: bool,
    /// Coarse scalar confidence for the whole transcript — the MEAN of
    /// `word_scores` on the sherpa CTC path, or an engine's own scalar where it
    /// has no per-word signal. Deliberately coarse: a confidence GATE must read
    /// `word_scores` (the min margin), never this. The mean hides a single
    /// deeply-coerced word — a decode scoring [+8, -3] means +2.5 and sails past
    /// a mean threshold the min-based data says should fail
    /// (notes/DESIGN_ENGINE_POSTERIORS.md). For display/logging only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    /// Shared-clock onset (ms, the AudioChunk timestamp_ms timebase) of each
    /// word of `text`, aligned 1:1 with its whitespace-split words. Emitted by
    /// engines with alignment (sherpa's CTC path); lets a consumer convert a
    /// word position into an audio position that is meaningful to OTHER
    /// concurrently-running pipelines — the dictation stop-phrase audio
    /// cutoff (notes/DESIGN_DICTATION_AUDIO_CUTOFF.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_onsets_ms: Option<Vec<u64>>,
    /// Per-word acoustic score, aligned 1:1 with `text`'s whitespace-split
    /// words (same alignment contract as `word_onsets_ms`). On the sherpa CTC
    /// path this is the word's min token argmax-margin: positive = the audio
    /// supported the word, negative = the closed grammar coerced it
    /// (notes/DESIGN_ENGINE_POSTERIORS.md). When present, `confidence` is the
    /// mean of these. Log-only today — enforcement gates land per-consumer
    /// once field distributions are measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_scores: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowCredit {
    pub session_id: String,
    pub frames: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub code: String,
    pub message: String,
    pub fatal: bool,
}

// ---- Device monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceInfo {
    pub device_id: u32,
    pub uid: String,
    pub name: String,
    pub is_input: bool,
    pub is_output: bool,
    pub is_default_input: bool,
    pub is_default_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceSnapshot {
    pub devices: Vec<DeviceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAdded {
    pub device: DeviceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceRemoved {
    pub device_id: u32,
    pub uid: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultDeviceChanged {
    pub direction: String,
    pub device_id: u32,
    pub uid: String,
    pub name: String,
}

// ---- Location events ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub horizontal_accuracy: f64,
    pub vertical_accuracy: f64,
    pub timestamp: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadingUpdate {
    pub magnetic_heading: f64,
    pub true_heading: f64,
    pub heading_accuracy: f64,
    pub timestamp: f64,
}

// ---- Display monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
pub struct DisplaySnapshot {
    pub displays: Vec<DisplayInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayAdded {
    pub display: DisplayInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayRemoved {
    pub display_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayChanged {
    pub display: DisplayInfo,
}

// ---- Power monitoring events ----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PowerState {
    pub source: String,
    pub battery_level: Option<f64>,
    pub is_charging: bool,
    pub time_to_empty: Option<i64>,
    pub time_to_full: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerSnapshot {
    pub state: PowerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerSourceChanged {
    pub state: PowerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSleepWake {
    pub timestamp: f64,
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
    fn pcm_16k_mono_baseline_constants() {
        let f = AudioFormat::PCM_16K_MONO;
        assert_eq!(f.rate, 16000);
        assert_eq!(f.width, 2);
        assert_eq!(f.channels, 1);
    }
}
