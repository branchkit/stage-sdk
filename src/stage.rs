//! The stage runtime — the layer between wire framing and a working stage.
//!
//! [`wire`](crate::wire) gets bytes on and off the pipe. This module owns the
//! obligations above it that every stage otherwise hand-rolls, and that fail
//! *silently* when hand-rolled wrong:
//!
//! - the capability handshake goes out first and unprompted,
//! - receiver-side flow credit is granted on a declared policy rather than a
//!   counter each stage maintains itself,
//! - unknown events are tolerated (wire leniency is contract, not courtesy),
//! - a fatal error becomes one `BKLOG1` line and exit 1, the same way in
//!   every stage.
//!
//! # Which entry point
//!
//! There are three, because there are three loop shapes:
//!
//! - [`serve_audio_consumer`] — **read-driven**. The stage's work is a reaction
//!   to an inbound audio session. VAD gates, STT engines, command recognizers.
//!   (An audio sink, which plays what it consumes, also reports when it was
//!   heard — `playback_started` / `playback_ended`, stamped with
//!   [`shared_clock_ms`] — at moments no inbound event marks, so it drives
//!   [`crate::wire`] and [`crate::credit`] itself rather than this loop.)
//! - [`serve_source`] — **notifier-driven**. The stage produces spontaneously
//!   from a device, OS notification, or timer, and may never read stdin at
//!   all. Power/display/location monitors, microphones.
//! - [`serve_speech_engine`] — **request-driven**. The stage turns each
//!   `speak` request into an audio session it produces, streaming, and stops
//!   one the moment it is cancelled. Text-to-speech engines: the pipeline run
//!   the other way, from words to the speakers.
//!
//! An audio source is the second shape plus a stdin listener for the stop
//! request; see [`SourceOptions::listen_for_stop`].
//!
//! # Why one is media-neutral and the other is not
//!
//! The asymmetry in those names is deliberate, and it is a property of the
//! wire rather than of this module.
//!
//! [`serve_source`] is domain-free: it assumes nothing about what you emit, and
//! four non-audio stages ship on it today (power, display, location, audio
//! device). A gaze tracker, a foot pedal, an EMG sensor, a presence detector
//! are all the same shape — something external notifies you, you emit — and the
//! `feeds` declaration plus the `ext.<vendor>.*` namespace is already their
//! route onto the bus.
//!
//! [`serve_audio_consumer`] is audio-bound because **audio is the only stream
//! the wire has**. Of the pipeline contract's event types, the streaming ones
//! are `audio_start` / `audio_chunk` / `audio_stop`; everything else is a
//! discrete event. Flow credit counts audio frames. A frame-consuming stage —
//! screen OCR, gesture or lip-VAD vision — has no typed stream to consume, so
//! it would fall through to [`AudioConsumer::on_other`] and hand-roll exactly
//! what this module exists to remove.
//!
//! That is a contract question, not an SDK one: this runtime cannot be more
//! general than the protocol it speaks, and generalizing the wire to a
//! media-typed stream is a much larger decision than naming things honestly
//! here.
//!
//! # Flow credit: which side are you on
//!
//! The pipeline's backpressure is asymmetric, and getting this backwards is
//! the most expensive mistake available to a stage author:
//!
//! - **Consuming audio → you must grant credit.** That is what
//!   [`CreditPolicy`] and [`crate::credit::CreditGranter`] are for, and
//!   [`serve_audio_consumer`] drives it for you.
//! - **Producing audio → you must not implement credit at all.** That covers
//!   speech engines as much as microphones. The platform
//!   sits between every pair of stages and holds the sender-side window; a
//!   producer that outruns it blocks on the pipe. There is deliberately no
//!   sender-side helper in this crate.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;

use crate::credit::CreditGranter;
use crate::events::{
    AudioChunk, AudioFormat, AudioStart, AudioStop, Capability, Reply, Request, Speak, event_type,
};
use crate::stage_log;
use crate::wire::{Event, Reader, Writer};

/// Boxed error, matching what stage `main`s already return.
pub type BoxError = Box<dyn std::error::Error>;

/// Result alias used throughout the runtime.
pub type Result<T = ()> = std::result::Result<T, BoxError>;

type BoxRead = Box<dyn AsyncRead + Unpin>;
type BoxWrite = Box<dyn AsyncWrite + Unpin>;
/// A reader a spawned task may own: the speech engine reads stdin on its own
/// task so a cancel is seen while the engine is busy speaking.
type BoxReadSend = Box<dyn AsyncRead + Unpin + Send>;

/// Run a stage body as `main`, mapping a fatal error to one structured log
/// line and exit code 1.
///
/// Replaces the identical wrapper every stage carries:
///
/// ```ignore
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() {
///     branchkit_stage_sdk::stage::run(run()).await
/// }
/// ```
pub async fn run<F: Future<Output = Result>>(body: F) {
    if let Err(e) = body.await {
        stage_log::error(&format!("fatal: {e}"));
        std::process::exit(1);
    }
}

/// When the runtime emits the initial credit window.
///
/// The *numbers* are receiver-chosen buffering policy and stay at the call
/// site; only the mechanism is
/// shared. This enum captures the one structural difference observed between
/// stages: whether the window opens before any session exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialGrant {
    /// Emit `initial` immediately after the handshake, stamped with an empty
    /// session id — the stage buffers freely and wants upstream moving before
    /// any session exists.
    OnStart,
    /// Emit `initial` on every `audio_start`, stamped with that session (which
    /// also resets the cadence counter). Shallow-queue stages.
    OnSessionStart,
    /// No automatic grant. For stages whose window depends on runtime state,
    /// or that consume no audio and must never grant.
    Manual,
}

/// Receiver-side credit policy. `every`/`grant` feed
/// [`CreditGranter`]; `initial` and `when` control the unconditional window.
#[derive(Debug, Clone, Copy)]
pub struct CreditPolicy {
    /// Frames in the unconditional initial window.
    pub initial: u32,
    /// Grant after every N processed chunks.
    pub every: u32,
    /// Frames per cadence grant.
    pub grant: u32,
    /// When the initial window is emitted.
    pub when: InitialGrant,
}

impl CreditPolicy {
    /// A stage that consumes no audio and must never grant credit.
    pub const NONE: CreditPolicy = CreditPolicy {
        initial: 0,
        every: 0,
        grant: 0,
        when: InitialGrant::Manual,
    };
}

/// Whether a delivered `audio_chunk` counts toward the credit cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chunk {
    /// Processed — count it. The normal answer, and the default.
    Counted,
    /// Dropped without processing (a stale session id, an unsupported
    /// format). Not counted.
    ///
    /// Note the open question this preserves rather than settles: a dropped
    /// chunk still spent a frame of the sender's window, so never counting it
    /// shrinks that window for the rest of the run. Today's behavior is
    /// preserved verbatim; if it turns out to matter, the fix belongs here in
    /// the runtime, not in each stage.
    Dropped,
}

/// Whether the consumer loop continues after a callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep reading. The default.
    Continue,
    /// Stop the loop and return — a `per_run` stage finishing its session.
    Stop,
}

/// What a callback is handed: the outbound writer, plus the credit granter
/// wired to this stage's policy.
pub struct AudioCtx {
    writer: Writer<BoxWrite>,
    credit: CreditGranter,
    policy: CreditPolicy,
}

impl AudioCtx {
    /// Serialize `data` and write it as one framed event.
    pub async fn emit<T: Serialize>(&mut self, event_type: &str, data: &T) -> Result {
        let value = serde_json::to_value(data)?;
        self.writer
            .write_event(&Event::new(event_type, value))
            .await?;
        Ok(())
    }

    /// Serialize `data` and write it with a borrowed binary payload, then
    /// flush. Avoids the per-frame payload copy an [`Event`] would take.
    pub async fn emit_raw<T: Serialize>(
        &mut self,
        event_type: &str,
        data: &T,
        payload: &[u8],
    ) -> Result {
        let value = serde_json::to_value(data)?;
        self.writer.write_raw(event_type, value, payload).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Grant `frames` of credit unconditionally and reset the cadence counter.
    /// For windows the policy cannot express — a re-grant at an utterance
    /// boundary, say.
    pub async fn grant_now(&mut self, session_id: &str, frames: u32) -> Result {
        self.credit
            .grant_now(&mut self.writer, session_id, frames)
            .await?;
        Ok(())
    }

    /// The raw writer, for anything the helpers above do not cover.
    pub fn writer(&mut self) -> &mut Writer<BoxWrite> {
        &mut self.writer
    }
}

/// A read-driven stage.
///
/// Every method has a default, so a stage implements only the events it cares
/// about. The runtime decodes `data` into the typed event before dispatch and
/// routes anything it does not decode to [`AudioConsumer::on_other`].
//
// `async fn` in a public trait: stages run on a single-threaded runtime
// (`#[tokio::main(flavor = "current_thread")]`, all of them), so the absent
// `Send` bound the lint warns about is not a constraint any stage can hit.
#[allow(async_fn_in_trait)]
pub trait AudioConsumer {
    /// A session began upstream.
    async fn on_audio_start(&mut self, _ev: AudioStart, _ctx: &mut AudioCtx) -> Result {
        Ok(())
    }

    /// One frame of audio. Return [`Chunk::Dropped`] to keep it out of the
    /// credit cadence.
    async fn on_audio_chunk(
        &mut self,
        _ev: AudioChunk,
        _payload: &[u8],
        _ctx: &mut AudioCtx,
    ) -> Result<Chunk> {
        Ok(Chunk::Counted)
    }

    /// The session ended. A `per_run` stage emits its final result here and
    /// returns [`Flow::Stop`].
    async fn on_audio_stop(&mut self, _ev: AudioStop, _ctx: &mut AudioCtx) -> Result<Flow> {
        Ok(Flow::Continue)
    }

    /// Any event the runtime did not decode, including unknown types.
    /// Ignoring them is the default because wire leniency is contract.
    async fn on_other(&mut self, _ev: Event, _ctx: &mut AudioCtx) -> Result<Flow> {
        Ok(Flow::Continue)
    }

    /// Clean EOF on stdin — upstream closed.
    async fn on_eof(&mut self, _ctx: &mut AudioCtx) -> Result {
        Ok(())
    }
}

/// Serve a read-driven stage on stdin/stdout.
pub async fn serve_audio_consumer<C: AudioConsumer>(
    cap: Capability,
    policy: CreditPolicy,
    handler: &mut C,
) -> Result {
    serve_audio_consumer_on(
        Box::new(tokio::io::stdin()),
        Box::new(tokio::io::stdout()),
        cap,
        policy,
        handler,
    )
    .await
}

/// [`serve_audio_consumer`] over explicit transports. The stdio wrapper is the one
/// stages use; this exists so the runtime itself is testable over a duplex.
pub async fn serve_audio_consumer_on<C: AudioConsumer>(
    reader: BoxRead,
    writer: BoxWrite,
    cap: Capability,
    policy: CreditPolicy,
    handler: &mut C,
) -> Result {
    let mut reader = Reader::new(reader);
    let mut ctx = AudioCtx {
        writer: Writer::new(writer),
        credit: CreditGranter::new(policy.every.max(1), policy.grant),
        policy,
    };

    ctx.emit(event_type::CAPABILITY, &cap).await?;

    if policy.when == InitialGrant::OnStart && policy.initial > 0 {
        ctx.grant_now("", policy.initial).await?;
    }

    loop {
        let Some(ev) = reader.read_event().await? else {
            handler.on_eof(&mut ctx).await?;
            return Ok(());
        };

        match ev.event_type.as_str() {
            event_type::AUDIO_START => {
                let start: AudioStart = serde_json::from_value(ev.data)?;
                if ctx.policy.when == InitialGrant::OnSessionStart && ctx.policy.initial > 0 {
                    let (sid, frames) = (start.session_id.clone(), ctx.policy.initial);
                    ctx.grant_now(&sid, frames).await?;
                }
                handler.on_audio_start(start, &mut ctx).await?;
            }
            event_type::AUDIO_CHUNK => {
                let Event { data, payload, .. } = ev;
                let chunk: AudioChunk = serde_json::from_value(data)?;
                let session_id = chunk.session_id.clone();
                if handler.on_audio_chunk(chunk, &payload, &mut ctx).await? == Chunk::Counted
                    && ctx.policy.every > 0
                {
                    ctx.credit.on_chunk(&mut ctx.writer, &session_id).await?;
                }
            }
            event_type::AUDIO_STOP => {
                let stop: AudioStop = serde_json::from_value(ev.data)?;
                if handler.on_audio_stop(stop, &mut ctx).await? == Flow::Stop {
                    return Ok(());
                }
            }
            _ => {
                if handler.on_other(ev, &mut ctx).await? == Flow::Stop {
                    return Ok(());
                }
            }
        }
    }
}

/// Options for [`serve_source`]. `SourceOptions::default()` is every option
/// off; turn one on with its setter:
///
/// ```
/// use branchkit_stage_sdk::stage::SourceOptions;
///
/// let opts = SourceOptions::new().listen_for_stop(true);
/// assert!(opts.listen_for_stop);
/// ```
///
/// `#[non_exhaustive]`, like [`crate::events::Capability`], so the next
/// option is not a breaking change for a stage that sets this one.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct SourceOptions {
    /// Listen on stdin for the platform's stop request and stop when it
    /// arrives (or on EOF, which means the platform is gone).
    ///
    /// This is what makes an audio source out of an event source: the runner
    /// ends a session by writing `audio_stop` to the source's stdin, and the
    /// stop may carry a `cutoff_ms` the source must forward verbatim on its
    /// own downstream `audio_stop`, so a consumer buffering ahead of the stop
    /// does not process audio past it. Read it back with
    /// [`SourceCtx::stop_request`].
    ///
    /// Off by default, deliberately: the monitor stages never open stdin
    /// today, and turning that on for them would be a behavior change rather
    /// than an extraction.
    pub listen_for_stop: bool,
}

impl SourceOptions {
    /// Every option off — the same as `SourceOptions::default()`, usable in
    /// a `const`.
    pub const fn new() -> Self {
        SourceOptions {
            listen_for_stop: false,
        }
    }

    /// Set [`SourceOptions::listen_for_stop`](#structfield.listen_for_stop).
    pub const fn listen_for_stop(mut self, on: bool) -> Self {
        self.listen_for_stop = on;
        self
    }
}

/// A running source stage's handle: the outbound writer plus the stop signal.
pub struct SourceCtx {
    writer: Writer<BoxWrite>,
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    stop_request: Arc<Mutex<Option<AudioStop>>>,
}

impl SourceCtx {
    /// Serialize `data` and write it as one framed event.
    pub async fn emit<T: Serialize>(&mut self, event_type: &str, data: &T) -> Result {
        let value = serde_json::to_value(data)?;
        self.writer
            .write_event(&Event::new(event_type, value))
            .await?;
        Ok(())
    }

    /// Serialize `data` and write it with a borrowed binary payload, then flush.
    pub async fn emit_raw<T: Serialize>(
        &mut self,
        event_type: &str,
        data: &T,
        payload: &[u8],
    ) -> Result {
        let value = serde_json::to_value(data)?;
        self.writer.write_raw(event_type, value, payload).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Has a stop been requested (signal, or stdin EOF when enabled)?
    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Wait until a stop is requested. Pairs with `tokio::select!` against the
    /// stage's own event source, and is also correct as a body's only await.
    ///
    /// Register-then-check: `Notify::notify_waiters` keeps no permit, so a
    /// stop published between the flag check and the await would otherwise be
    /// lost — and a body whose only await is this one would then hang until
    /// the platform's kill.
    pub async fn stopped_signal(&self) {
        while !self.stopped() {
            let notified = self.notify.notified();
            if self.stopped() {
                break;
            }
            notified.await;
        }
    }

    /// The stop flag, for a producer loop that polls rather than awaits.
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// The `audio_stop` that requested this stop, when the platform sent one
    /// and [`SourceOptions::listen_for_stop`] is on. `None` if the stop came
    /// from a signal, from stdin EOF, or from [`SourceCtx::request_stop`].
    ///
    /// An audio source forwards this event's `cutoff_ms` verbatim on its own
    /// downstream `audio_stop`.
    pub fn stop_request(&self) -> Option<AudioStop> {
        self.stop_request.lock().ok().and_then(|g| g.clone())
    }

    /// Request a stop from inside the stage.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// The raw writer, for anything the helpers above do not cover.
    pub fn writer(&mut self) -> &mut Writer<BoxWrite> {
        &mut self.writer
    }
}

/// Serve a notifier-driven stage on stdout.
///
/// Emits the capability handshake, installs a SIGTERM/SIGINT stop watcher (and
/// the stdin drain, per [`SourceOptions`]), then hands control to `body`. The
/// stage owns its own loop — that is the point of this shape.
pub async fn serve_source<F, Fut>(cap: Capability, opts: SourceOptions, body: F) -> Result
where
    F: FnOnce(SourceCtx) -> Fut,
    Fut: Future<Output = Result>,
{
    serve_source_on(Box::new(tokio::io::stdout()), cap, opts, body).await
}

/// [`serve_source`] over an explicit transport, for tests.
pub async fn serve_source_on<F, Fut>(
    writer: BoxWrite,
    cap: Capability,
    opts: SourceOptions,
    body: F,
) -> Result
where
    F: FnOnce(SourceCtx) -> Fut,
    Fut: Future<Output = Result>,
{
    let mut ctx = SourceCtx {
        writer: Writer::new(writer),
        stop: Arc::new(AtomicBool::new(false)),
        notify: Arc::new(Notify::new()),
        stop_request: Arc::new(Mutex::new(None)),
    };

    // The trap goes in BEFORE the capability: a platform that has read the
    // handshake may send SIGTERM at once, and must get a clean stop.
    spawn_signal_watcher(ctx.stop.clone(), ctx.notify.clone());

    ctx.emit(event_type::CAPABILITY, &cap).await?;
    if opts.listen_for_stop {
        spawn_stop_listener(
            ctx.stop.clone(),
            ctx.notify.clone(),
            ctx.stop_request.clone(),
        );
    }

    body(ctx).await
}

/// Set the stop flag on SIGTERM/SIGINT (Ctrl-C on non-unix).
///
/// The handlers are registered here, synchronously, not inside the spawned
/// task: tokio installs the process handler at `signal()`, and on a
/// current_thread runtime a spawned task is not polled until the caller first
/// yields. Registered lazily, a SIGTERM in that window took the default action
/// and killed the stage mid-handshake.
fn spawn_signal_watcher(stop: Arc<AtomicBool>, notify: Arc<Notify>) {
    #[cfg(unix)]
    let signals = {
        use tokio::signal::unix::{SignalKind, signal};
        match (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) {
            (Ok(t), Ok(i)) => Some((t, i)),
            _ => None,
        }
    };
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let Some((mut term, mut int)) = signals else {
                return;
            };
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        stop.store(true, Ordering::Relaxed);
        notify.notify_waiters();
    });
}

/// Watch stdin for the platform's `audio_stop` stop request, stopping on it —
/// or on EOF/error, which mean the platform is gone.
///
/// Every other inbound event is ignored rather than fatal: a source stage must
/// tolerate inbound bytes without dying, which the conformance `source` suite
/// tests directly.
fn spawn_stop_listener(
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    slot: Arc<Mutex<Option<AudioStop>>>,
) {
    tokio::spawn(async move {
        let mut reader = Reader::new(tokio::io::stdin());
        loop {
            match reader.read_event().await {
                Ok(Some(ev)) if ev.event_type == event_type::AUDIO_STOP => {
                    if let Ok(parsed) = serde_json::from_value::<AudioStop>(ev.data) {
                        if let Ok(mut g) = slot.lock() {
                            *g = Some(parsed);
                        }
                    }
                    break;
                }
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break,
            }
        }
        stop.store(true, Ordering::Relaxed);
        notify.notify_waiters();
    });
}

/// Milliseconds on the shared clock: the clock the platform stamps microphone
/// audio with (`AudioChunk::timestamp_ms`), and so the clock a recognizer's
/// word onsets are on.
///
/// It is the OS's monotonic clock as the platform reads it on each OS —
/// `CLOCK_UPTIME_RAW` on macOS (the uptime clock the macOS app stamps its
/// microphone with), `CLOCK_MONOTONIC` on other unixes, and wall time on
/// Windows, where every producer on the machine uses it. Two stamps from
/// different processes on one machine are comparable; a stamp is meaningless
/// on another machine or across a reboot.
///
/// A stage stamps anything the platform will compare with what the
/// microphone heard: an audio sink stamps `playback_started` /
/// `playback_ended` with it, which is how the platform drops BranchKit's own
/// voice coming back through the microphone.
pub fn shared_clock_ms() -> u64 {
    #[cfg(unix)]
    {
        #[cfg(target_os = "macos")]
        let clock = libc::CLOCK_UPTIME_RAW;
        #[cfg(not(target_os = "macos"))]
        let clock = libc::CLOCK_MONOTONIC;
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid, writable timespec; clock_gettime only
        // writes through the pointer.
        unsafe { libc::clock_gettime(clock, &mut ts) };
        ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000
    }
    #[cfg(not(unix))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// What a speech engine's [`SpeechEngine::speak`] is handed: where the
/// utterance's audio goes, and whether it has been cancelled.
pub struct SpeakCtx<'a> {
    writer: &'a mut Writer<BoxWrite>,
    session_id: String,
    cancel: Arc<AtomicBool>,
    started: bool,
}

impl SpeakCtx<'_> {
    /// The utterance this context belongs to.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Has the platform cancelled this utterance? Stop synthesizing when it
    /// has: nothing more of it will be sent.
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    /// The cancel flag itself, for synthesis running on another thread (a
    /// blocking engine under `tokio::task::spawn_blocking`, or a C callback
    /// that can return "stop"). It turns true when the platform cancels.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        self.cancel.clone()
    }

    /// Open the utterance's audio in `format`. Call it once, before the first
    /// [`SpeakCtx::audio`]. Engines usually know their format only once the
    /// model is loaded, which is why it is given here and not in the
    /// capability.
    pub async fn start(&mut self, format: AudioFormat) -> Result {
        if self.started {
            return Err("speech engine: start called twice for one utterance".into());
        }
        self.started = true;
        if self.cancelled() {
            return Ok(());
        }
        let start = AudioStart {
            session_id: self.session_id.clone(),
            format,
        };
        self.writer
            .write_event(&Event::new(
                event_type::AUDIO_START,
                serde_json::to_value(&start)?,
            ))
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Send one chunk of audio, in the format given to [`SpeakCtx::start`],
    /// as soon as it is synthesized; send it in pieces as the engine makes
    /// them, never the whole utterance at the end, so the first words play
    /// while the rest are being made.
    ///
    /// Returns [`Flow::Stop`] once the utterance is cancelled, without sending
    /// anything: stop synthesizing and return. The runtime closes the
    /// utterance either way.
    pub async fn audio(&mut self, pcm: &[u8]) -> Result<Flow> {
        if !self.started {
            return Err("speech engine: audio before start".into());
        }
        if self.cancelled() {
            return Ok(Flow::Stop);
        }
        let chunk = AudioChunk {
            session_id: self.session_id.clone(),
            timestamp_ms: shared_clock_ms(),
        };
        self.writer
            .write_raw(event_type::AUDIO_CHUNK, serde_json::to_value(&chunk)?, pcm)
            .await?;
        self.writer.flush().await?;
        Ok(Flow::Continue)
    }
}

/// A speech engine: text in, audio out (`stage_type` `tts`).
///
/// The engine implements one thing, how to say one [`Speak`] request; the
/// runtime owns the rest of the contract — the handshake, the order requests
/// are spoken in, cancellation, and closing every utterance with exactly one
/// `audio_stop`, including one that failed or was cancelled before it began.
//
// `async fn` in a public trait: see `AudioConsumer`.
#[allow(async_fn_in_trait)]
pub trait SpeechEngine {
    /// Say `req`: call [`SpeakCtx::start`] once with the audio format, then
    /// [`SpeakCtx::audio`] for each piece as it is synthesized, and return
    /// when the utterance is done or [`SpeakCtx::audio`] says
    /// [`Flow::Stop`].
    ///
    /// An `Err` fails this utterance only: the runtime sends an `error` for
    /// it, closes it, and goes on to the next. A failure that makes the
    /// engine unusable belongs before [`serve_speech_engine`] (a model that
    /// does not load), where [`run`] turns it into exit 1.
    ///
    /// Never block the runtime here: synthesis that blocks a thread runs
    /// under `tokio::task::spawn_blocking` and checks
    /// [`SpeakCtx::cancel_flag`], or the platform's cancel cannot be read
    /// until it returns.
    async fn speak(&mut self, req: Speak, ctx: &mut SpeakCtx<'_>) -> Result;
}

/// The utterance being spoken and its cancel flag, shared with the reader.
type Current = Arc<Mutex<Option<(String, Arc<AtomicBool>)>>>;

/// What the stdin reader hands the speaking loop.
enum Inbound {
    Speak(Speak),
    Cancel(String),
}

/// Serve a speech engine on stdin/stdout.
///
/// Speech engines produce audio, so they implement no flow credit (see the
/// module docs): the platform holds the window, and an engine that outruns
/// playback blocks on the pipe.
pub async fn serve_speech_engine<E: SpeechEngine>(cap: Capability, engine: &mut E) -> Result {
    serve_speech_engine_on(
        Box::new(tokio::io::stdin()),
        Box::new(tokio::io::stdout()),
        cap,
        engine,
    )
    .await
}

/// [`serve_speech_engine`] over explicit transports, for tests.
pub async fn serve_speech_engine_on<E: SpeechEngine>(
    reader: BoxReadSend,
    writer: BoxWrite,
    cap: Capability,
    engine: &mut E,
) -> Result {
    let mut writer = Writer::new(writer);
    writer
        .write_event(&Event::new(
            event_type::CAPABILITY,
            serde_json::to_value(&cap)?,
        ))
        .await?;
    writer.flush().await?;

    // The utterance being spoken, so a cancel for it reaches the engine
    // while `speak` is still running rather than after it returns.
    let current: Current = Arc::new(Mutex::new(None));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
    spawn_speak_reader(reader, current.clone(), tx);

    let mut queue: std::collections::VecDeque<Speak> = std::collections::VecDeque::new();
    loop {
        // Take everything that has arrived before choosing what to say next,
        // so a cancel already sent for a queued utterance is honored before
        // it starts.
        loop {
            match rx.try_recv() {
                Ok(msg) => handle_inbound(msg, &mut queue, &mut writer).await?,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                // The platform is gone: nothing queued will be heard.
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(()),
            }
        }
        let Some(req) = queue.pop_front() else {
            match rx.recv().await {
                Some(msg) => handle_inbound(msg, &mut queue, &mut writer).await?,
                None => return Ok(()),
            }
            continue;
        };

        let session_id = req.session_id.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        if let Ok(mut g) = current.lock() {
            *g = Some((session_id.clone(), cancel.clone()));
        }
        let outcome = {
            let mut ctx = SpeakCtx {
                writer: &mut writer,
                session_id: session_id.clone(),
                cancel,
                started: false,
            };
            engine.speak(req, &mut ctx).await
        };
        if let Ok(mut g) = current.lock() {
            *g = None;
        }
        if let Err(e) = outcome {
            let err = crate::events::ErrorEvent {
                session_id: Some(session_id.clone()),
                code: "speak_failed".into(),
                message: e.to_string(),
                fatal: false,
            };
            writer
                .write_event(&Event::new(event_type::ERROR, serde_json::to_value(&err)?))
                .await?;
        }
        write_audio_stop(&mut writer, &session_id).await?;
    }
}

async fn handle_inbound(
    msg: Inbound,
    queue: &mut std::collections::VecDeque<Speak>,
    writer: &mut Writer<BoxWrite>,
) -> Result {
    match msg {
        Inbound::Speak(req) => queue.push_back(req),
        Inbound::Cancel(session_id) => {
            // Queued and not yet begun: close it now. A cancel for the
            // utterance being spoken was already delivered through its flag,
            // and one for an id no longer known is moot.
            if let Some(i) = queue.iter().position(|r| r.session_id == session_id) {
                queue.remove(i);
                write_audio_stop(writer, &session_id).await?;
            }
        }
    }
    Ok(())
}

async fn write_audio_stop(writer: &mut Writer<BoxWrite>, session_id: &str) -> Result {
    let stop = AudioStop {
        session_id: session_id.to_string(),
        cutoff_ms: None,
    };
    writer
        .write_event(&Event::new(
            event_type::AUDIO_STOP,
            serde_json::to_value(&stop)?,
        ))
        .await?;
    writer.flush().await?;
    Ok(())
}

/// Read `speak` requests and cancels from stdin. A cancel for the utterance
/// being spoken trips its flag here, at once; everything is also forwarded to
/// the speaking loop. Unknown and malformed events are ignored (wire leniency
/// is contract). EOF or a read error cancels the utterance in progress and
/// ends the stage: the platform is gone.
fn spawn_speak_reader(
    reader: BoxReadSend,
    current: Current,
    tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
) {
    tokio::spawn(async move {
        let mut reader = Reader::new(reader);
        loop {
            match reader.read_event().await {
                Ok(Some(ev)) if ev.event_type == event_type::SPEAK => {
                    match serde_json::from_value::<Speak>(ev.data) {
                        Ok(req) => {
                            if tx.send(Inbound::Speak(req)).is_err() {
                                break;
                            }
                        }
                        Err(e) => stage_log::warn(&format!("speak: undecodable request: {e}")),
                    }
                }
                Ok(Some(ev)) if ev.event_type == event_type::AUDIO_STOP => {
                    let Ok(stop) = serde_json::from_value::<AudioStop>(ev.data) else {
                        continue;
                    };
                    if let Ok(g) = current.lock() {
                        if let Some((id, flag)) = g.as_ref() {
                            if *id == stop.session_id {
                                flag.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                    if tx.send(Inbound::Cancel(stop.session_id)).is_err() {
                        break;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break,
            }
        }
        if let Ok(g) = current.lock() {
            if let Some((_, flag)) = g.as_ref() {
                flag.store(true, Ordering::Relaxed);
            }
        }
        // Dropping `tx` tells the speaking loop the platform is gone.
    });
}

// ---- Request stages: one answer per request ----

/// A request stage's work: answer one request. The fourth stage shape,
/// beside the audio consumer, the source and the speech engine — text or
/// data in, one answer out, for work a plugin wants done in a confined
/// process of its own (a language model, a translator, a classifier).
///
/// The capability declares `stage_type: "request"`. Requests are answered
/// one at a time, in arrival order.
#[allow(async_fn_in_trait)]
pub trait RequestHandler {
    /// Answer one request's `body`. An `Err` becomes this request's reply
    /// `error`, and the stage goes on to the next request. A failure that
    /// makes the stage unusable (a model that does not load) belongs before
    /// [`serve_requests`], where [`run`] turns it into exit 1.
    ///
    /// Work that blocks a thread (inference) runs under
    /// `tokio::task::spawn_blocking`.
    async fn handle(
        &mut self,
        body: serde_json::Value,
    ) -> std::result::Result<serde_json::Value, String>;
}

/// Serve a request stage on stdin/stdout: send the capability, then answer
/// every `request` with exactly one `reply` carrying its `request_id`, until
/// stdin closes. A `request` that cannot be read (no `request_id`) gets an
/// `error` event, since there is no id to reply to; other event types are
/// ignored, as wire leniency is contract.
pub async fn serve_requests<H: RequestHandler>(cap: Capability, handler: &mut H) -> Result {
    serve_requests_on(
        Box::new(tokio::io::stdin()),
        Box::new(tokio::io::stdout()),
        cap,
        handler,
    )
    .await
}

/// [`serve_requests`] over explicit transports, for tests.
pub async fn serve_requests_on<H: RequestHandler>(
    reader: BoxRead,
    writer: BoxWrite,
    cap: Capability,
    handler: &mut H,
) -> Result {
    let mut reader = Reader::new(reader);
    let mut writer = Writer::new(writer);
    writer
        .write_event(&Event::new(
            event_type::CAPABILITY,
            serde_json::to_value(&cap)?,
        ))
        .await?;
    writer.flush().await?;

    while let Some(ev) = reader.read_event().await? {
        if ev.event_type != event_type::REQUEST {
            continue;
        }
        let req: Request = match serde_json::from_value(ev.data) {
            Ok(req) => req,
            Err(e) => {
                writer
                    .write_event(&Event::new(
                        event_type::ERROR,
                        serde_json::json!({
                            "code": "bad_request",
                            "message": format!("unreadable request: {e}"),
                            "fatal": false,
                        }),
                    ))
                    .await?;
                writer.flush().await?;
                continue;
            }
        };
        let reply = match handler.handle(req.body).await {
            Ok(body) => Reply {
                request_id: req.request_id,
                body: Some(body),
                error: None,
            },
            Err(error) => Reply {
                request_id: req.request_id,
                body: None,
                error: Some(error),
            },
        };
        writer
            .write_event(&Event::new(
                event_type::REPLY,
                serde_json::to_value(&reply)?,
            ))
            .await?;
        writer.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{AudioFormat, FlowCredit};
    use serde_json::json;
    use tokio::io::duplex;

    fn cap(stage_type: &str) -> Capability {
        Capability {
            stage_type: stage_type.into(),
            stage_name: "test".into(),
            audio_formats: vec![AudioFormat::PCM_16K_MONO],
            lifecycle_modes: vec!["persistent".into()],
            feature_flags: serde_json::Map::new(),
            emits: vec![],
            consumes: vec![],
            ..Default::default()
        }
    }

    /// Drive `serve_audio_consumer_on` with a scripted inbound event list, returning
    /// everything the stage wrote.
    async fn run_consumer<C: AudioConsumer>(
        handler: &mut C,
        policy: CreditPolicy,
        inbound: Vec<Event>,
    ) -> Vec<Event> {
        let (harness_w, stage_r) = duplex(1 << 16);
        let (stage_w, harness_r) = duplex(1 << 16);

        // Write the whole script up front, then EOF, so the loop terminates.
        let mut w = Writer::new(harness_w);
        for ev in &inbound {
            w.write_event(ev).await.unwrap();
        }
        drop(w); // EOF — shutdown() flushes but does not close.

        serve_audio_consumer_on(
            Box::new(stage_r),
            Box::new(stage_w),
            cap("processor"),
            policy,
            handler,
        )
        .await
        .unwrap();

        let mut out = Vec::new();
        let mut r = Reader::new(harness_r);
        while let Some(ev) = r.read_event().await.unwrap() {
            out.push(ev);
        }
        out
    }

    fn chunk_event(session: &str) -> Event {
        Event::with_payload(
            event_type::AUDIO_CHUNK,
            json!({ "session_id": session, "timestamp_ms": 0 }),
            vec![0u8; 4],
        )
    }

    fn credits(out: &[Event]) -> Vec<FlowCredit> {
        out.iter()
            .filter(|e| e.event_type == event_type::FLOW_CREDIT)
            .map(|e| serde_json::from_value(e.data.clone()).unwrap())
            .collect()
    }

    #[derive(Default)]
    struct Counter {
        starts: u32,
        chunks: u32,
        stops: u32,
        others: u32,
        eofs: u32,
        drop_chunks: bool,
        stop_on_stop: bool,
    }

    impl AudioConsumer for Counter {
        async fn on_audio_start(&mut self, _ev: AudioStart, _ctx: &mut AudioCtx) -> Result {
            self.starts += 1;
            Ok(())
        }
        async fn on_audio_chunk(
            &mut self,
            _ev: AudioChunk,
            _payload: &[u8],
            _ctx: &mut AudioCtx,
        ) -> Result<Chunk> {
            self.chunks += 1;
            Ok(if self.drop_chunks {
                Chunk::Dropped
            } else {
                Chunk::Counted
            })
        }
        async fn on_audio_stop(&mut self, _ev: AudioStop, _ctx: &mut AudioCtx) -> Result<Flow> {
            self.stops += 1;
            Ok(if self.stop_on_stop {
                Flow::Stop
            } else {
                Flow::Continue
            })
        }
        async fn on_other(&mut self, _ev: Event, _ctx: &mut AudioCtx) -> Result<Flow> {
            self.others += 1;
            Ok(Flow::Continue)
        }
        async fn on_eof(&mut self, _ctx: &mut AudioCtx) -> Result {
            self.eofs += 1;
            Ok(())
        }
    }

    const ON_START: CreditPolicy = CreditPolicy {
        initial: 32,
        every: 2,
        grant: 8,
        when: InitialGrant::OnStart,
    };

    #[tokio::test]
    async fn capability_is_the_first_event_and_needs_no_prompting() {
        let mut h = Counter::default();
        let out = run_consumer(&mut h, CreditPolicy::NONE, vec![]).await;
        assert_eq!(out[0].event_type, event_type::CAPABILITY);
        assert_eq!(out[0].data["stage_type"], "processor");
        // EOF with no inbound events still reaches the handler.
        assert_eq!(h.eofs, 1);
    }

    #[tokio::test]
    async fn on_start_policy_grants_the_initial_window_before_any_session() {
        let mut h = Counter::default();
        let out = run_consumer(&mut h, ON_START, vec![]).await;
        let grants = credits(&out);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].frames, 32);
        // Empty session id: the window opens before any session exists.
        assert_eq!(grants[0].session_id, "");
    }

    #[tokio::test]
    async fn on_session_start_policy_grants_per_session_not_at_startup() {
        let policy = CreditPolicy {
            when: InitialGrant::OnSessionStart,
            ..ON_START
        };
        let mut h = Counter::default();
        let start = Event::new(
            event_type::AUDIO_START,
            json!({ "session_id": "s1", "format": AudioFormat::PCM_16K_MONO }),
        );
        let out = run_consumer(&mut h, policy, vec![start]).await;
        let grants = credits(&out);
        assert_eq!(grants.len(), 1, "no startup grant, one session grant");
        assert_eq!(grants[0].frames, 32);
        assert_eq!(grants[0].session_id, "s1");
    }

    #[tokio::test]
    async fn counted_chunks_drive_the_cadence() {
        let mut h = Counter::default();
        // every=2 → grants after chunk 2 and 4, not 5.
        let out = run_consumer(
            &mut h,
            ON_START,
            (0..5).map(|_| chunk_event("s1")).collect(),
        )
        .await;
        let grants = credits(&out);
        assert_eq!(h.chunks, 5);
        assert_eq!(grants.len(), 3, "1 initial + 2 cadence");
        assert!(
            grants[1..]
                .iter()
                .all(|g| g.frames == 8 && g.session_id == "s1")
        );
    }

    #[tokio::test]
    async fn dropped_chunks_do_not_drive_the_cadence() {
        let mut h = Counter {
            drop_chunks: true,
            ..Default::default()
        };
        let out = run_consumer(
            &mut h,
            ON_START,
            (0..5).map(|_| chunk_event("s1")).collect(),
        )
        .await;
        assert_eq!(h.chunks, 5, "handler still sees every chunk");
        assert_eq!(
            credits(&out).len(),
            1,
            "initial window only — no cadence grants"
        );
    }

    #[tokio::test]
    async fn a_policy_of_none_never_grants() {
        let mut h = Counter::default();
        let out = run_consumer(
            &mut h,
            CreditPolicy::NONE,
            (0..5).map(|_| chunk_event("s1")).collect(),
        )
        .await;
        assert_eq!(h.chunks, 5);
        assert!(credits(&out).is_empty());
    }

    #[tokio::test]
    async fn unknown_event_types_reach_on_other_and_do_not_terminate() {
        let mut h = Counter::default();
        let inbound = vec![
            Event::new("ext.acme.custom", json!({"a": 1})),
            Event::new("totally_unknown", json!({})),
            chunk_event("s1"),
        ];
        run_consumer(&mut h, CreditPolicy::NONE, inbound).await;
        assert_eq!(h.others, 2);
        assert_eq!(h.chunks, 1, "the loop kept going past both unknowns");
    }

    #[tokio::test]
    async fn stop_from_audio_stop_ends_the_loop_and_skips_the_rest() {
        let mut h = Counter {
            stop_on_stop: true,
            ..Default::default()
        };
        let inbound = vec![
            Event::new(event_type::AUDIO_STOP, json!({ "session_id": "s1" })),
            chunk_event("s1"),
        ];
        run_consumer(&mut h, CreditPolicy::NONE, inbound).await;
        assert_eq!(h.stops, 1);
        assert_eq!(h.chunks, 0, "events after Flow::Stop are not delivered");
        assert_eq!(h.eofs, 0, "stopping is not EOF");
    }

    #[tokio::test]
    async fn source_emits_capability_then_runs_the_body() {
        let (stage_w, harness_r) = duplex(1 << 16);
        serve_source_on(
            Box::new(stage_w),
            cap("monitor"),
            SourceOptions::default(),
            |mut ctx| async move {
                assert!(!ctx.stopped());
                ctx.emit("ext.acme.tick", &json!({ "n": 1 })).await?;
                ctx.request_stop();
                // Already stopped: this must return immediately, not hang.
                ctx.stopped_signal().await;
                assert!(ctx.stopped());
                Ok(())
            },
        )
        .await
        .unwrap();

        let mut r = Reader::new(harness_r);
        let mut out = Vec::new();
        while let Some(ev) = r.read_event().await.unwrap() {
            out.push(ev);
        }
        assert_eq!(out[0].event_type, event_type::CAPABILITY);
        assert_eq!(out[1].event_type, "ext.acme.tick");
    }

    #[tokio::test]
    async fn stopped_signal_does_not_miss_a_stop_published_before_the_await() {
        let (stage_w, _harness_r) = duplex(1 << 16);
        // A stop landing between the flag check and the await must still wake
        // the body — `notify_waiters` keeps no permit, so this is the case the
        // register-then-check loop exists for.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            serve_source_on(
                Box::new(stage_w),
                cap("monitor"),
                SourceOptions::default(),
                |ctx| async move {
                    ctx.request_stop();
                    ctx.stopped_signal().await;
                    Ok(())
                },
            ),
        )
        .await
        .expect("stopped_signal hung on an already-published stop")
        .unwrap();
    }

    // ---- speech engine ----

    /// An engine that "speaks" one 4-byte chunk per word, yielding between
    /// chunks so the reader task runs (a real engine awaits its synthesis).
    /// A word "fail" makes the utterance fail after its first chunk.
    #[derive(Default)]
    struct WordEngine {
        spoken: Vec<String>,
    }

    impl SpeechEngine for WordEngine {
        async fn speak(&mut self, req: Speak, ctx: &mut SpeakCtx<'_>) -> Result {
            self.spoken.push(req.session_id.clone());
            ctx.start(AudioFormat::PCM_16K_MONO).await?;
            for word in req.text.split_whitespace() {
                if ctx.audio(&[0u8; 4]).await? == Flow::Stop {
                    return Ok(());
                }
                if word == "fail" {
                    return Err("engine broke".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok(())
        }
    }

    fn speak_event(session: &str, text: &str) -> Event {
        Event::new(
            event_type::SPEAK,
            json!({ "session_id": session, "text": text }),
        )
    }

    fn stop_event(session: &str) -> Event {
        Event::new(event_type::AUDIO_STOP, json!({ "session_id": session }))
    }

    /// Serve `engine` with `inbound` written up front and then EOF after
    /// `hold_open`, returning everything it wrote.
    async fn run_engine(
        engine: &mut WordEngine,
        inbound: Vec<Event>,
        hold_open: std::time::Duration,
    ) -> Vec<Event> {
        let (harness_w, stage_r) = duplex(1 << 16);
        let (stage_w, harness_r) = duplex(1 << 16);
        let mut w = Writer::new(harness_w);
        for ev in &inbound {
            w.write_event(ev).await.unwrap();
        }
        w.flush().await.unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(hold_open).await;
            drop(w);
        });
        serve_speech_engine_on(Box::new(stage_r), Box::new(stage_w), cap("tts"), engine)
            .await
            .unwrap();
        let mut r = Reader::new(harness_r);
        let mut out = Vec::new();
        while let Some(ev) = r.read_event().await.unwrap() {
            out.push(ev);
        }
        out
    }

    fn for_session<'a>(out: &'a [Event], session: &str) -> Vec<&'a Event> {
        out.iter()
            .filter(|e| e.data["session_id"] == session)
            .collect()
    }

    fn tags(evs: &[&Event]) -> Vec<String> {
        evs.iter().map(|e| e.event_type.clone()).collect()
    }

    #[tokio::test]
    async fn an_utterance_is_start_chunks_stop_on_its_session_and_never_credit() {
        let mut e = WordEngine::default();
        let out = run_engine(
            &mut e,
            vec![speak_event("u1", "snap left now")],
            std::time::Duration::from_millis(200),
        )
        .await;
        assert_eq!(out[0].event_type, event_type::CAPABILITY);
        assert_eq!(
            tags(&for_session(&out, "u1")),
            [
                "audio_start",
                "audio_chunk",
                "audio_chunk",
                "audio_chunk",
                "audio_stop"
            ]
        );
        assert!(
            out.iter().all(|e| e.event_type != event_type::FLOW_CREDIT),
            "a producer implements no credit"
        );
        let chunk = out
            .iter()
            .find(|e| e.event_type == event_type::AUDIO_CHUNK)
            .unwrap();
        assert_eq!(chunk.payload.len(), 4);
        assert!(
            chunk.data["timestamp_ms"].as_u64().unwrap() > 0,
            "shared clock"
        );
    }

    #[tokio::test]
    async fn utterances_are_spoken_in_the_order_they_arrive() {
        let mut e = WordEngine::default();
        run_engine(
            &mut e,
            vec![speak_event("a", "one"), speak_event("b", "two")],
            std::time::Duration::from_millis(200),
        )
        .await;
        assert_eq!(e.spoken, ["a", "b"]);
    }

    #[tokio::test]
    async fn a_cancel_stops_the_utterance_in_progress_and_closes_it_once() {
        let (harness_w, stage_r) = duplex(1 << 16);
        let (stage_w, harness_r) = duplex(1 << 16);
        let words = vec!["word"; 200].join(" ");
        // The platform's side runs on its own task; the engine in this one.
        let harness = tokio::spawn(async move {
            let mut w = Writer::new(harness_w);
            let mut r = Reader::new(harness_r);
            w.write_event(&speak_event("long", &words)).await.unwrap();
            w.flush().await.unwrap();
            // Wait for the first chunk: the utterance is under way.
            loop {
                let ev = r.read_event().await.unwrap().unwrap();
                if ev.event_type == event_type::AUDIO_CHUNK {
                    break;
                }
            }
            w.write_event(&stop_event("long")).await.unwrap();
            w.flush().await.unwrap();
            let mut after = Vec::new();
            loop {
                let ev = r.read_event().await.unwrap().unwrap();
                let done = ev.event_type == event_type::AUDIO_STOP;
                after.push(ev.event_type);
                if done {
                    break;
                }
            }
            drop(w);
            // Nothing more for the session after its audio_stop.
            let mut later = Vec::new();
            while let Some(ev) = r.read_event().await.unwrap() {
                later.push(ev);
            }
            (after, later)
        });
        let mut e = WordEngine::default();
        serve_speech_engine_on(Box::new(stage_r), Box::new(stage_w), cap("tts"), &mut e)
            .await
            .unwrap();
        let (after, later) = harness.await.unwrap();
        assert!(
            after.len() < 10,
            "the engine stopped within a few chunks of the cancel, not after 200: {after:?}"
        );
        assert!(
            later.iter().all(|ev| ev.data["session_id"] != "long"),
            "no event after the close"
        );
    }

    #[tokio::test]
    async fn a_queued_utterance_cancelled_before_it_begins_is_closed_unspoken() {
        let mut e = WordEngine::default();
        let out = run_engine(
            &mut e,
            vec![
                speak_event("a", "one two three four"),
                speak_event("b", "never"),
                stop_event("b"),
            ],
            std::time::Duration::from_millis(300),
        )
        .await;
        assert_eq!(e.spoken, ["a"], "b never reached the engine");
        assert_eq!(tags(&for_session(&out, "b")), ["audio_stop"]);
    }

    #[tokio::test]
    async fn a_failed_utterance_reports_an_error_closes_and_the_next_one_plays() {
        let mut e = WordEngine::default();
        let out = run_engine(
            &mut e,
            vec![speak_event("bad", "fail here"), speak_event("good", "fine")],
            std::time::Duration::from_millis(200),
        )
        .await;
        assert_eq!(
            tags(&for_session(&out, "bad")),
            ["audio_start", "audio_chunk", "error", "audio_stop"]
        );
        let err = for_session(&out, "bad")[2];
        assert_eq!(err.data["fatal"], false);
        assert_eq!(
            tags(&for_session(&out, "good")),
            ["audio_start", "audio_chunk", "audio_stop"]
        );
    }

    #[tokio::test]
    async fn unknown_and_malformed_inbound_is_ignored() {
        let mut e = WordEngine::default();
        let out = run_engine(
            &mut e,
            vec![
                Event::new("ext.acme.thing", json!({})),
                Event::new(event_type::SPEAK, json!({ "no": "text" })),
                speak_event("ok", "hello"),
            ],
            std::time::Duration::from_millis(200),
        )
        .await;
        assert_eq!(e.spoken, ["ok"]);
        assert_eq!(
            tags(&for_session(&out, "ok")),
            ["audio_start", "audio_chunk", "audio_stop"]
        );
    }

    #[test]
    fn the_shared_clock_moves_forward() {
        let a = shared_clock_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(shared_clock_ms() >= a + 4);
    }

    // ---- request stages ----

    struct Upper;
    impl RequestHandler for Upper {
        async fn handle(
            &mut self,
            body: serde_json::Value,
        ) -> std::result::Result<serde_json::Value, String> {
            match body.get("text").and_then(|t| t.as_str()) {
                Some(t) => Ok(json!({ "text": t.to_uppercase() })),
                None => Err("no text".into()),
            }
        }
    }

    async fn run_requests(inbound: Vec<Event>) -> Vec<Event> {
        let (harness_w, stage_r) = duplex(1 << 16);
        let (stage_w, harness_r) = duplex(1 << 16);
        let mut w = Writer::new(harness_w);
        for ev in &inbound {
            w.write_event(ev).await.unwrap();
        }
        drop(w);
        serve_requests_on(
            Box::new(stage_r),
            Box::new(stage_w),
            cap("request"),
            &mut Upper,
        )
        .await
        .unwrap();
        let mut out = Vec::new();
        let mut r = Reader::new(harness_r);
        while let Some(ev) = r.read_event().await.unwrap() {
            out.push(ev);
        }
        out
    }

    #[tokio::test]
    async fn every_request_gets_one_reply_with_its_id_in_order() {
        let out = run_requests(vec![
            Event::new(
                event_type::REQUEST,
                json!({"request_id": "a", "body": {"text": "one"}}),
            ),
            Event::new("vocabulary_update", json!({})),
            Event::new(event_type::REQUEST, json!({"request_id": "b", "body": {}})),
            Event::new(
                event_type::REQUEST,
                json!({"request_id": "c", "body": {"text": "three"}}),
            ),
        ])
        .await;
        assert_eq!(out[0].event_type, event_type::CAPABILITY);
        let replies: Vec<Reply> = out[1..]
            .iter()
            .map(|e| {
                assert_eq!(e.event_type, event_type::REPLY);
                serde_json::from_value(e.data.clone()).unwrap()
            })
            .collect();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[0].request_id, "a");
        assert_eq!(replies[0].body, Some(json!({"text": "ONE"})));
        assert_eq!(replies[1].request_id, "b");
        assert_eq!(replies[1].error.as_deref(), Some("no text"));
        assert_eq!(replies[1].body, None);
        assert_eq!(replies[2].body, Some(json!({"text": "THREE"})));
    }

    #[tokio::test]
    async fn an_unreadable_request_is_an_error_event_and_serving_continues() {
        let out = run_requests(vec![
            Event::new(event_type::REQUEST, json!({"body": {"text": "no id"}})),
            Event::new(
                event_type::REQUEST,
                json!({"request_id": "z", "body": {"text": "ok"}}),
            ),
        ])
        .await;
        assert_eq!(out[1].event_type, event_type::ERROR);
        assert_eq!(out[1].data["code"], "bad_request");
        assert_eq!(out[2].event_type, event_type::REPLY);
        assert_eq!(out[2].data["request_id"], "z");
    }
}
