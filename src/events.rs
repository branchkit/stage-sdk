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
    // Admission: a tag names its payload's actual scope, not a broader one
    // (heading_update is not a location; capability/flow_credit/error are
    // unqualified because their scope is every stage). Keep tags flat
    // snake_case: this vocabulary has one author, and dotted segments are
    // collision machinery for many. Blank-line grouping below is a reading
    // aid, not a contract.
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
/// - prefer exactly three, `ext.<vendor>.<name>`. A subscription's `*` is
///   exactly one segment, so a plugin subscribed to `ext.<vendor>.*` receives
///   `ext.acme.gaze_point` but not `ext.acme.gaze.left_eye`; that one needs
///   `ext.acme.**` (zero or more segments, so every depth under the vendor),
///   `ext.acme.*.*`, or the exact name. Your own `ext.<vendor>.*`
///   declaration in [`Capability::emits`] covers any depth (see
///   [`declaration_covers`]), so a deeper name is legal; the platform warns
///   when a stage emits one that the same glob, written as a subscription,
///   would miss
/// - `data` crosses the bus; a binary `payload` does NOT (the bus is JSON) —
///   the platform stamps its length into `data` as `_payload_length` so the
///   drop is visible, and payloads remain valid wire-level for stage-to-stage
///   use.
///   `data` may be up to 64 KB of serialized JSON
/// - the bus admits up to 1000 events per second from one stage, across all
///   of its `ext.*` types ([`EXT_RATE_LIMIT_PER_SEC`]). Past that, events are
///   dropped for the rest of the second, and the platform logs the crossing
///   once rather than each drop. The limit protects the platform from a
///   runaway stage; it is not flow control. It clears per-sample streams up
///   to 1 kHz (eye trackers, HID); a faster source should batch samples into
///   fewer events. Events forwarded to a downstream stage through
///   [`Capability::consumes`] never reach the bus and are not counted
/// - declare the rate of each steady stream in [`Capability::streams`]. The
///   platform checks the declarations when the stage starts
///   ([`Capability::check_streams`]) and refuses the stage, naming the stream
///   and the limit, when they cannot be carried — so a rate problem is a
///   start-up error you see once rather than drops you discover later. A
///   stage whose streams stay within their declared rates never reaches the
///   limit above
/// - a stream that is STATE rather than a log of happenings (a gaze
///   position, a pointer, a pedal's travel) can be declared
///   [`Delivery::Latest`]: each subscriber then receives the newest value at
///   the pace it can take, instead of every sample or a backlog of stale ones
/// - the namespace is reserved to stages: plugin emits of `ext.*` are
///   rejected, so a subscriber can trust the source attribution
/// - declare emitted types (or an `ext.<vendor>.*` glob) in
///   [`Capability::emits`]; the conformance harness enforces the declaration
///   and the platform logs undeclared emissions
pub const EXT_EVENT_PREFIX: &str = "ext.";

/// The most `ext.*` events per second the platform's bus admits from one
/// stage, across all of its custom types.
///
/// It is also the budget [`Capability::streams`] is checked against: the
/// declared rates of one stage's streams may add up to at most this.
pub const EXT_RATE_LIMIT_PER_SEC: u32 = 1000;

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

/// Does one declared entry of a stage's [`Capability::emits`] or
/// [`Capability::consumes`] cover `event_type`?
///
/// An entry is an exact type, or a pattern over dot-separated segments in
/// which `*` stands for exactly one segment, whatever its text. One form is
/// wider: a TRAILING `.*` covers one or more segments, so `ext.acme.*`
/// declares every type under the vendor at any depth — `ext.acme.gaze` and
/// `ext.acme.gaze.left_eye`, but not `ext.acme` itself and not
/// `ext.acmeister.x`. Nothing else is a wildcard: `**`, or a `*` inside a
/// segment (`gaze_*`), is literal text in a declaration.
///
/// This is the rule the platform applies to a running stage's declarations
/// and the rule the conformance harness enforces, from this one function.
///
/// A subscriber's patterns are a different surface. There `*` is always
/// exactly one segment, even at the end, and `**` is zero or more segments —
/// so the subscription that receives everything `ext.acme.*` declares is
/// `ext.acme.**`.
pub fn declaration_covers(declared: &str, event_type: &str) -> bool {
    if declared == event_type {
        return true;
    }
    let (pattern, open_ended) = match declared.strip_suffix(".*") {
        Some(prefix) => (prefix, true),
        None => (declared, false),
    };
    let mut segments = event_type.split('.');
    for want in pattern.split('.') {
        match segments.next() {
            Some(seg) if want == "*" || want == seg => {}
            _ => return false,
        }
    }
    // Open-ended: at least one segment below the prefix. Otherwise every
    // segment was consumed, so the counts agree.
    segments.next().is_some() == open_ended
}

#[cfg(test)]
mod declaration_covers_tests {
    use super::declaration_covers;

    #[test]
    fn a_trailing_glob_covers_every_depth_below_its_prefix() {
        assert!(declaration_covers("ext.acme.*", "ext.acme.gaze_point"));
        assert!(declaration_covers("ext.acme.*", "ext.acme.gaze.left_eye"));
        assert!(declaration_covers("ext.acme.*", "ext.acme.a.b.c"));
        // Not the prefix itself, and not a longer vendor name.
        assert!(!declaration_covers("ext.acme.*", "ext.acme"));
        assert!(!declaration_covers("ext.acme.*", "ext.acmeister.x"));
    }

    #[test]
    fn a_star_anywhere_else_is_exactly_one_segment() {
        assert!(declaration_covers("ext.*.gaze", "ext.acme.gaze"));
        assert!(!declaration_covers("ext.*.gaze", "ext.acme.eye.gaze"));
        assert!(declaration_covers("*", "transcript"));
        assert!(!declaration_covers("*", "power.snapshot"));
        // A trailing glob after a mid-pattern one still opens the end.
        assert!(declaration_covers("ext.*.*", "ext.acme.gaze.left_eye"));
        assert!(!declaration_covers("ext.*.*", "ext.acme"));
    }

    #[test]
    fn exact_entries_and_literal_text() {
        assert!(declaration_covers("transcript", "transcript"));
        assert!(declaration_covers("power_snapshot", "power_snapshot"));
        assert!(!declaration_covers("transcript", "transcript.final"));
        // `**` and a within-segment `*` are literal text here.
        assert!(!declaration_covers("ext.acme.**", "ext.acme.gaze"));
        assert!(declaration_covers("ext.acme.**", "ext.acme.**"));
        assert!(!declaration_covers(
            "ext.acme.gaze_*",
            "ext.acme.gaze_point"
        ));
    }
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

/// What a stage tells the platform about itself in its handshake: what it is,
/// how it runs, and the events it emits, consumes and accepts as config.
///
/// Build one with [`Capability::new`] and the chained setters below:
///
/// ```
/// use branchkit_stage_sdk::events::{Capability, StreamDecl};
///
/// let cap = Capability::new("sensor", "gaze")
///     .persistent()
///     .emits(["ext.acme.*"])
///     .stream(StreamDecl::latest("ext.acme.gaze_point", 250));
/// assert_eq!(cap.emits, ["ext.acme.*"]);
/// ```
///
/// The struct is `#[non_exhaustive]`, so a field added later is not a
/// breaking change for any stage: outside this crate it cannot be written as
/// a struct literal (not even with `..Default::default()`), only built
/// through `new` and the setters, or `Capability::default()` plus field
/// assignment. Every field stays public to read and to change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// The schema's description is the language-neutral half of the doc above:
// the builder and `#[non_exhaustive]` are Rust's, and it projects into the
// Go, TypeScript and Python ports.
#[cfg_attr(
    feature = "schema",
    schemars(
        description = "What a stage declares about itself in its handshake: what it is, how it runs, and the events it emits, consumes and accepts as configuration."
    )
)]
#[non_exhaustive]
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
    /// (an `ext.<vendor>.*` glob covers every type under the vendor, at any
    /// depth; a `*` elsewhere is exactly one segment). Advisory at
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
    /// Custom event types this stage accepts as CONFIGURATION from the plugin
    /// that ships it — exact `ext.<vendor>.<name>` types, or `ext.<vendor>.*`
    /// globs.
    ///
    /// Distinct from [`Capability::consumes`], and deliberately not folded
    /// into it. `consumes` is the stage-to-stage valve: its types arrive from
    /// the stage ABOVE in the pipeline, and the wiring check fails loudly when
    /// the upstream neighbour does not emit a declared type. Plugin config has
    /// no upstream neighbour, so reusing `consumes` would make every stage
    /// that wants config fail to wire. Two different relations, two fields.
    ///
    /// The direction this opens is plugin → a stage it ships. It does not
    /// loosen the bus rule: plugin emits of `ext.*` ONTO THE EVENT BUS stay
    /// rejected, because a bus subscriber trusts the source attribution. A
    /// message sent here never reaches the bus — it is written to one stage's
    /// stdin, the recipient is named by the sender, and the platform checks
    /// that the sender ships that stage. There is no attribution to forge.
    ///
    /// A stage should apply config it receives and keep working without it.
    /// Gate on config only when the stage cannot be CORRECT without it (an
    /// unseeded recognition grammar decodes to garbage, so `sherpa_commands`
    /// drops audio until seeded); config that only improves QUALITY — a
    /// decoder bias list, say — must never block the stage's real work.
    ///
    /// Empty = the stage accepts no plugin configuration, which is every
    /// pre-existing stage.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepts_config: Vec<String>,
    /// The steady `ext.*` streams this stage emits onto the bus: for each,
    /// the most events per second it will send and how subscribers receive
    /// it (see `StreamDecl`).
    ///
    /// Declare a stream when it runs at a rate — a sensor sampling, a
    /// position reporting. Occasional events need no declaration.
    ///
    /// The platform checks this list when the stage starts and refuses to
    /// run a stage whose declarations it cannot carry — naming the stream and
    /// the limit — rather than dropping its events later. Each stream must be
    /// an exact type that `emits` covers, appear once, declare at least 1
    /// event per second, and the declared rates of one stage add up to at
    /// most 1000 per second (the Rust SDK's `Capability::check_streams` is
    /// the rule, and the conformance harness runs it).
    ///
    /// Empty = no declarations, which is every pre-existing stage: its events
    /// are delivered one by one under the shared limit, exactly as before.
    /// The field is optional on the wire in both directions — a platform that
    /// predates it ignores it and delivers every event.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<StreamDecl>,
}

/// How the platform delivers one declared stream to its subscribers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Delivery {
    /// Every event reaches every subscriber, in order: a log of things that
    /// happened (a blink, a key press, a recognized gesture). The default,
    /// and how every undeclared event is delivered.
    #[default]
    Every,
    /// State: each event carries the whole current value (a gaze position, a
    /// pointer, a pedal's travel) in its `data`, and supersedes the one
    /// before it. Each subscriber receives the newest value at the pace it
    /// can take: a fast subscriber sees every sample, a slow one fewer and
    /// newer — it skips to the newest rather than working through a backlog
    /// of stale positions. Values of one stream arrive in order, never an
    /// older after a newer; they are not ordered against other events, and a
    /// newest value may arrive ahead of events emitted before it. A
    /// subscriber is not replayed the value from before it subscribed; it
    /// receives the next one.
    Latest,
}

impl Delivery {
    /// True for [`Delivery::Every`] — what an absent `delivery` means.
    pub fn is_every(&self) -> bool {
        matches!(self, Delivery::Every)
    }
}

/// One steady `ext.*` stream a stage declares in [`Capability::streams`].
///
/// Build one with [`StreamDecl::every`] or [`StreamDecl::latest`]. The struct
/// is `#[non_exhaustive]` so a later field (a burst allowance, say) does not
/// break a stage that builds one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// The schema's description is the language-neutral half of the doc above: it
// projects into the Go, TypeScript and Python ports, where the Rust
// constructors and `#[non_exhaustive]` mean nothing.
#[cfg_attr(
    feature = "schema",
    schemars(
        description = "One steady `ext.*` stream a stage declares in Capability.streams: its exact type, the most events per second it will emit, and how subscribers receive it."
    )
)]
#[non_exhaustive]
pub struct StreamDecl {
    /// The exact custom event type, `ext.<vendor>.<name>` — not a glob. It
    /// must also be covered by the stage's `emits`.
    pub event_type: String,
    /// The most events per second the stage will emit on this stream; at
    /// least 1. Declare the sensor's real ceiling (a 250 Hz tracker declares
    /// 250): the platform warns when a stream runs well past its declaration.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub rate_hz: u32,
    /// How subscribers receive it. Absent = `every`.
    #[serde(default, skip_serializing_if = "Delivery::is_every")]
    pub delivery: Delivery,
}

impl StreamDecl {
    /// A stream whose every event reaches every subscriber.
    pub fn every(event_type: impl Into<String>, rate_hz: u32) -> Self {
        StreamDecl {
            event_type: event_type.into(),
            rate_hz,
            delivery: Delivery::Every,
        }
    }

    /// A state stream: subscribers receive the newest value at their own
    /// pace (see [`Delivery::Latest`]).
    pub fn latest(event_type: impl Into<String>, rate_hz: u32) -> Self {
        StreamDecl {
            event_type: event_type.into(),
            rate_hz,
            delivery: Delivery::Latest,
        }
    }

    /// The most events of this stream in one second before the platform
    /// says it is running past its declaration: the declared rate plus a
    /// tenth (at least one event). A sensor's clock drifts and an OS hands
    /// over samples in small bursts, so a stream at exactly its declared rate
    /// still lands a little over it in some seconds; that is not a stage
    /// misdeclaring. The platform only advises past this — it drops nothing
    /// until the per-stage limit — and the conformance harness fails a
    /// stream that averages above it.
    pub fn tolerated_per_second(&self) -> u32 {
        self.rate_hz.saturating_add((self.rate_hz / 10).max(1))
    }
}

/// Why the platform refuses a stream declaration — returned by
/// [`Capability::check_streams`], and the text the platform logs when it
/// refuses to run the stage.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamRefusal {
    /// Not an exact, well-formed `ext.<vendor>.<name>` type. Globs name a
    /// namespace, not a stream.
    NotAStream { event_type: String },
    /// Not covered by [`Capability::emits`].
    NotEmitted { event_type: String },
    /// Declared more than once.
    Duplicate { event_type: String },
    /// A declared rate of zero.
    ZeroRate { event_type: String },
    /// The stream does not fit in what is left of the stage's budget of
    /// [`EXT_RATE_LIMIT_PER_SEC`] declared events per second.
    OverBudget {
        event_type: String,
        rate_hz: u32,
        /// What the streams declared before this one left over.
        available: u32,
        limit: u32,
    },
}

impl std::fmt::Display for StreamRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamRefusal::NotAStream { event_type } => write!(
                f,
                "stream {event_type:?} is not an exact ext.<vendor>.<name> type \
                 (a stream is one type; a glob belongs in emits)"
            ),
            StreamRefusal::NotEmitted { event_type } => write!(
                f,
                "stream {event_type:?} is not covered by the stage's emits — declare it there too"
            ),
            StreamRefusal::Duplicate { event_type } => {
                write!(f, "stream {event_type:?} is declared more than once")
            }
            StreamRefusal::ZeroRate { event_type } => write!(
                f,
                "stream {event_type:?} declares a rate of 0 events/s; the least is 1"
            ),
            StreamRefusal::OverBudget {
                event_type,
                rate_hz,
                available,
                limit,
            } => write!(
                f,
                "stream {event_type:?} declares {rate_hz} events/s, but one stage's streams \
                 may declare at most {limit} events/s in total and {available} is left — \
                 batch samples into fewer events, or sample a `latest` stream down to a \
                 rate its subscribers can use"
            ),
        }
    }
}

impl Capability {
    /// A capability for a stage of `stage_type` named `stage_name`, declaring
    /// nothing else yet. Add a lifecycle mode ([`Capability::persistent`] or
    /// [`Capability::per_run`]) — the platform needs at least one — and what
    /// the stage emits.
    pub fn new(stage_type: impl Into<String>, stage_name: impl Into<String>) -> Self {
        Capability {
            stage_type: stage_type.into(),
            stage_name: stage_name.into(),
            ..Default::default()
        }
    }

    /// Add a lifecycle mode by name. [`Capability::persistent`] and
    /// [`Capability::per_run`] name the two the platform runs today.
    pub fn lifecycle_mode(mut self, mode: impl Into<String>) -> Self {
        self.lifecycle_modes.push(mode.into());
        self
    }

    /// The stage runs for as long as its pipeline does.
    pub fn persistent(self) -> Self {
        self.lifecycle_mode("persistent")
    }

    /// The stage runs for one session and then exits.
    pub fn per_run(self) -> Self {
        self.lifecycle_mode("per_run")
    }

    /// Add an audio format the stage accepts or produces.
    pub fn audio_format(mut self, format: AudioFormat) -> Self {
        self.audio_formats.push(format);
        self
    }

    /// Set one feature flag.
    pub fn feature_flag(
        mut self,
        name: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.feature_flags.insert(name.into(), value.into());
        self
    }

    /// Add event types (or `ext.<vendor>.*` globs) to
    /// [`Capability::emits`].
    pub fn emits<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.emits.extend(types.into_iter().map(Into::into));
        self
    }

    /// Add custom event types to [`Capability::consumes`].
    pub fn consumes<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.consumes.extend(types.into_iter().map(Into::into));
        self
    }

    /// Add custom event types to [`Capability::accepts_config`].
    pub fn accepts_config<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.accepts_config
            .extend(types.into_iter().map(Into::into));
        self
    }

    /// Declare one steady stream in [`Capability::streams`].
    pub fn stream(mut self, stream: StreamDecl) -> Self {
        self.streams.push(stream);
        self
    }

    /// Declare several steady streams in [`Capability::streams`].
    pub fn streams(mut self, streams: impl IntoIterator<Item = StreamDecl>) -> Self {
        self.streams.extend(streams);
        self
    }
}

impl Capability {
    /// Check [`Capability::streams`] against the rule the platform applies
    /// when the stage starts. `Ok` = the platform admits every declaration;
    /// otherwise every refusal, in declaration order. The platform refuses to
    /// run a stage this returns `Err` for, and the conformance harness fails
    /// it.
    ///
    /// Rates are budgeted in declaration order: a stream is refused when it
    /// does not fit in what the streams before it left of
    /// [`EXT_RATE_LIMIT_PER_SEC`].
    pub fn check_streams(&self) -> Result<(), Vec<StreamRefusal>> {
        let mut refusals = Vec::new();
        let mut seen: Vec<&str> = Vec::new();
        let mut declared_total: u32 = 0;
        for s in &self.streams {
            let event_type = s.event_type.clone();
            if !is_valid_ext_event_type(&s.event_type) || s.event_type.split('.').any(|p| p == "*")
            {
                refusals.push(StreamRefusal::NotAStream { event_type });
                continue;
            }
            if !self
                .emits
                .iter()
                .any(|d| declaration_covers(d, &s.event_type))
            {
                refusals.push(StreamRefusal::NotEmitted { event_type });
                continue;
            }
            if seen.contains(&s.event_type.as_str()) {
                refusals.push(StreamRefusal::Duplicate { event_type });
                continue;
            }
            seen.push(&s.event_type);
            if s.rate_hz == 0 {
                refusals.push(StreamRefusal::ZeroRate { event_type });
                continue;
            }
            let available = EXT_RATE_LIMIT_PER_SEC.saturating_sub(declared_total);
            if s.rate_hz > available {
                refusals.push(StreamRefusal::OverBudget {
                    event_type,
                    rate_hz: s.rate_hz,
                    available,
                    limit: EXT_RATE_LIMIT_PER_SEC,
                });
                continue;
            }
            declared_total += s.rate_hz;
        }
        if refusals.is_empty() {
            Ok(())
        } else {
            Err(refusals)
        }
    }
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
            ..Default::default()
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

    fn gaze_cap(streams: Vec<StreamDecl>) -> Capability {
        Capability::new("sensor", "gaze")
            .persistent()
            .emits(["ext.acme.*"])
            .streams(streams)
    }

    /// The builder sets exactly what a struct literal would, and its list
    /// setters add to what is there rather than replacing it.
    #[test]
    fn the_builder_sets_every_field() {
        let cap = Capability::new("sensor", "gaze")
            .persistent()
            .per_run()
            .audio_format(AudioFormat::PCM_16K_MONO)
            .feature_flag("fast", true)
            .emits(["ext.acme.a"])
            .emits(vec![String::from("ext.acme.b")])
            .consumes(["ext.up.frame"])
            .accepts_config(["ext.acme.config"])
            .stream(StreamDecl::latest("ext.acme.a", 250))
            .streams([StreamDecl::every("ext.acme.b", 5)]);
        assert_eq!(cap.stage_type, "sensor");
        assert_eq!(cap.stage_name, "gaze");
        assert_eq!(cap.lifecycle_modes, ["persistent", "per_run"]);
        assert_eq!(cap.audio_formats, [AudioFormat::PCM_16K_MONO]);
        assert_eq!(cap.feature_flags["fast"], serde_json::json!(true));
        assert_eq!(cap.emits, ["ext.acme.a", "ext.acme.b"]);
        assert_eq!(cap.consumes, ["ext.up.frame"]);
        assert_eq!(cap.accepts_config, ["ext.acme.config"]);
        assert_eq!(
            cap.streams,
            [
                StreamDecl::latest("ext.acme.a", 250),
                StreamDecl::every("ext.acme.b", 5)
            ]
        );
        assert!(cap.check_streams().is_ok());
    }

    /// Wire compatibility, both directions: a stage that declares no streams
    /// serializes exactly as before the field existed, and a capability from
    /// such a stage (no `streams` key) decodes to an empty list.
    #[test]
    fn capability_without_streams_is_unchanged_on_the_wire() {
        let cap = gaze_cap(vec![]);
        let v = serde_json::to_value(&cap).unwrap();
        assert!(v.get("streams").is_none(), "empty streams must be omitted");
        let old: Capability = serde_json::from_value(serde_json::json!({
            "stage_type": "sensor",
            "stage_name": "gaze",
            "lifecycle_modes": ["persistent"],
            "emits": ["ext.acme.*"],
        }))
        .unwrap();
        assert!(old.streams.is_empty());
    }

    #[test]
    fn stream_decl_wire_shape() {
        let cap = gaze_cap(vec![
            StreamDecl::latest("ext.acme.gaze_point", 250),
            StreamDecl::every("ext.acme.blink", 20),
        ]);
        let v = serde_json::to_value(&cap).unwrap();
        assert_eq!(
            v["streams"],
            serde_json::json!([
                {"event_type": "ext.acme.gaze_point", "rate_hz": 250, "delivery": "latest"},
                // `every` is the default and is left off the wire.
                {"event_type": "ext.acme.blink", "rate_hz": 20},
            ])
        );
        let back: Capability = serde_json::from_value(v).unwrap();
        assert_eq!(back.streams, cap.streams);
        assert_eq!(back.streams[1].delivery, Delivery::Every);
    }

    /// A delivery mode this SDK does not know is a decode error that names
    /// the modes it does — which the platform reports when it refuses the
    /// stage — never a silent fallback to another mode.
    #[test]
    fn unknown_delivery_mode_does_not_decode() {
        let err = serde_json::from_value::<StreamDecl>(serde_json::json!({
            "event_type": "ext.acme.gaze_point", "rate_hz": 250, "delivery": "sampled"
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("sampled") && err.contains("latest"),
            "error should name the mode and the known ones: {err}"
        );
    }

    /// The acceptance shape: a 250 Hz gaze stream, as state, beside an
    /// every-event stream, fits.
    #[test]
    fn a_250hz_latest_stream_is_admitted() {
        let cap = gaze_cap(vec![
            StreamDecl::latest("ext.acme.gaze_point", 250),
            StreamDecl::every("ext.acme.blink", 20),
        ]);
        assert_eq!(cap.check_streams(), Ok(()));
        // No streams at all: trivially admitted.
        assert_eq!(gaze_cap(vec![]).check_streams(), Ok(()));
    }

    #[test]
    fn a_stream_over_the_limit_is_refused_naming_it_and_the_limit() {
        let cap = gaze_cap(vec![StreamDecl::every("ext.acme.emg", 1200)]);
        let refusals = cap.check_streams().unwrap_err();
        assert_eq!(
            refusals,
            vec![StreamRefusal::OverBudget {
                event_type: "ext.acme.emg".into(),
                rate_hz: 1200,
                available: EXT_RATE_LIMIT_PER_SEC,
                limit: EXT_RATE_LIMIT_PER_SEC,
            }]
        );
        let text = refusals[0].to_string();
        assert!(
            text.contains("ext.acme.emg") && text.contains("1200") && text.contains("1000"),
            "{text}"
        );
    }

    /// The budget is the stage's, spent in declaration order: two streams
    /// that each fit alone do not fit together, and the one that overflows
    /// is the one named.
    #[test]
    fn declared_rates_share_one_budget() {
        let cap = gaze_cap(vec![
            StreamDecl::latest("ext.acme.gaze_point", 600),
            StreamDecl::every("ext.acme.emg", 600),
            StreamDecl::every("ext.acme.blink", 400),
        ]);
        assert_eq!(
            cap.check_streams().unwrap_err(),
            vec![StreamRefusal::OverBudget {
                event_type: "ext.acme.emg".into(),
                rate_hz: 600,
                available: 400,
                limit: EXT_RATE_LIMIT_PER_SEC,
            }],
            "blink still fits in what gaze left"
        );
        let exact = gaze_cap(vec![
            StreamDecl::latest("ext.acme.gaze_point", 600),
            StreamDecl::every("ext.acme.emg", 400),
        ]);
        assert_eq!(
            exact.check_streams(),
            Ok(()),
            "the limit itself is admitted"
        );
    }

    #[test]
    fn malformed_declarations_are_refused() {
        let mut cap = gaze_cap(vec![
            StreamDecl::every("ext.acme.*", 10),
            StreamDecl::every("ext.acme", 10),
            StreamDecl::every("ext.other.tick", 10),
            StreamDecl::every("ext.acme.tick", 0),
            StreamDecl::every("ext.acme.pos", 10),
            StreamDecl::latest("ext.acme.pos", 10),
        ]);
        let got = cap.check_streams().unwrap_err();
        assert_eq!(
            got,
            vec![
                StreamRefusal::NotAStream {
                    event_type: "ext.acme.*".into()
                },
                StreamRefusal::NotAStream {
                    event_type: "ext.acme".into()
                },
                StreamRefusal::NotEmitted {
                    event_type: "ext.other.tick".into()
                },
                StreamRefusal::ZeroRate {
                    event_type: "ext.acme.tick".into()
                },
                StreamRefusal::Duplicate {
                    event_type: "ext.acme.pos".into()
                },
            ]
        );
        // A stage that declares no emits covers nothing.
        cap.emits.clear();
        cap.streams = vec![StreamDecl::every("ext.acme.pos", 10)];
        assert_eq!(
            cap.check_streams().unwrap_err(),
            vec![StreamRefusal::NotEmitted {
                event_type: "ext.acme.pos".into()
            }]
        );
    }

    /// The `streams` field doc projects into every port without Rust links,
    /// so it states the limit as a number; it must be this crate's number.
    #[test]
    fn streams_doc_states_the_limit() {
        let src = include_str!("events.rs");
        let stated = format!("most {EXT_RATE_LIMIT_PER_SEC} per second");
        assert!(
            src.contains(&stated),
            "Capability::streams must say {stated:?}"
        );
    }

    #[test]
    fn tolerance_is_a_tenth_and_at_least_one() {
        assert_eq!(
            StreamDecl::latest("ext.acme.gaze_point", 250).tolerated_per_second(),
            275
        );
        assert_eq!(
            StreamDecl::every("ext.acme.tick", 100).tolerated_per_second(),
            110
        );
        assert_eq!(
            StreamDecl::every("ext.acme.slow", 5).tolerated_per_second(),
            6
        );
        assert_eq!(
            StreamDecl::every("ext.acme.x", u32::MAX).tolerated_per_second(),
            u32::MAX
        );
    }

    #[test]
    fn emits_coverage_follows_the_stage_declaration_rule() {
        assert!(declaration_covers("ext.acme.pos", "ext.acme.pos"));
        assert!(declaration_covers("ext.acme.*", "ext.acme.pos"));
        assert!(declaration_covers("ext.acme.*", "ext.acme.gaze.left_eye"));
        assert!(declaration_covers("ext.*.pos", "ext.acme.pos"));
        assert!(!declaration_covers("ext.*.pos", "ext.acme.gaze.pos"));
        assert!(!declaration_covers("ext.acme.*", "ext.acme"));
        assert!(!declaration_covers("ext.acme.*", "ext.acmeister.pos"));
        assert!(!declaration_covers("ext.acme.pos", "ext.acme.pose"));
    }

    #[test]
    fn pcm_16k_mono_baseline_constants() {
        let f = AudioFormat::PCM_16K_MONO;
        assert_eq!(f.rate, 16000);
        assert_eq!(f.width, 2);
        assert_eq!(f.channels, 1);
    }
}
