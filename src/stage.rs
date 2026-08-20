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
//! There are two, because there are two loop shapes (verified across all
//! first-party stages — `notes/DESIGN_STAGE_AUTHORING.md`):
//!
//! - [`serve_consumer`] — **read-driven**. The stage's work is a reaction to
//!   inbound events. VAD gates, STT engines, command recognizers.
//! - [`serve_source`] — **notifier-driven**. The stage produces spontaneously
//!   from a device, OS notification, or timer, and may never read stdin at
//!   all. Power/display/location monitors, microphones.
//!
//! An audio source is the second shape plus a stdin listener for the stop
//! request; see [`SourceOptions::stop_on_stdin_eof`].
//!
//! # Flow credit: which side are you on
//!
//! The pipeline's backpressure is asymmetric, and getting this backwards is
//! the most expensive mistake available to a stage author:
//!
//! - **Consuming audio → you must grant credit.** That is what
//!   [`CreditPolicy`] and [`crate::credit::CreditGranter`] are for, and
//!   [`serve_consumer`] drives it for you.
//! - **Producing audio → you must not implement credit at all.** The platform
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
use crate::events::{event_type, AudioChunk, AudioStart, AudioStop, Capability};
use crate::stage_log;
use crate::wire::{Event, Reader, Writer};

/// Boxed error, matching what stage `main`s already return.
pub type BoxError = Box<dyn std::error::Error>;

/// Result alias used throughout the runtime.
pub type Result<T = ()> = std::result::Result<T, BoxError>;

type BoxRead = Box<dyn AsyncRead + Unpin>;
type BoxWrite = Box<dyn AsyncWrite + Unpin>;

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
/// site (decision (e), `notes/DESIGN_STAGE_SDK.md`); only the mechanism is
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
pub struct Ctx {
    writer: Writer<BoxWrite>,
    credit: CreditGranter,
    policy: CreditPolicy,
}

impl Ctx {
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
/// routes anything it does not decode to [`Consumer::on_other`].
//
// `async fn` in a public trait: stages run on a single-threaded runtime
// (`#[tokio::main(flavor = "current_thread")]`, all of them), so the absent
// `Send` bound the lint warns about is not a constraint any stage can hit.
#[allow(async_fn_in_trait)]
pub trait Consumer {
    /// A session began upstream.
    async fn on_audio_start(&mut self, _ev: AudioStart, _ctx: &mut Ctx) -> Result {
        Ok(())
    }

    /// One frame of audio. Return [`Chunk::Dropped`] to keep it out of the
    /// credit cadence.
    async fn on_audio_chunk(
        &mut self,
        _ev: AudioChunk,
        _payload: &[u8],
        _ctx: &mut Ctx,
    ) -> Result<Chunk> {
        Ok(Chunk::Counted)
    }

    /// The session ended. A `per_run` stage emits its final result here and
    /// returns [`Flow::Stop`].
    async fn on_audio_stop(&mut self, _ev: AudioStop, _ctx: &mut Ctx) -> Result<Flow> {
        Ok(Flow::Continue)
    }

    /// Any event the runtime did not decode, including unknown types.
    /// Ignoring them is the default because wire leniency is contract.
    async fn on_other(&mut self, _ev: Event, _ctx: &mut Ctx) -> Result<Flow> {
        Ok(Flow::Continue)
    }

    /// Clean EOF on stdin — upstream closed.
    async fn on_eof(&mut self, _ctx: &mut Ctx) -> Result {
        Ok(())
    }
}

/// Serve a read-driven stage on stdin/stdout.
pub async fn serve_consumer<C: Consumer>(
    cap: Capability,
    policy: CreditPolicy,
    handler: &mut C,
) -> Result {
    serve_consumer_on(
        Box::new(tokio::io::stdin()),
        Box::new(tokio::io::stdout()),
        cap,
        policy,
        handler,
    )
    .await
}

/// [`serve_consumer`] over explicit transports. The stdio wrapper is the one
/// stages use; this exists so the runtime itself is testable over a duplex.
pub async fn serve_consumer_on<C: Consumer>(
    reader: BoxRead,
    writer: BoxWrite,
    cap: Capability,
    policy: CreditPolicy,
    handler: &mut C,
) -> Result {
    let mut reader = Reader::new(reader);
    let mut ctx = Ctx {
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

/// Options for [`serve_source`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SourceOptions {
    /// Listen on stdin for the platform's stop request and stop when it
    /// arrives (or on EOF, which means the platform is gone).
    ///
    /// This is what makes an audio source out of an event source: the runner
    /// ends a session by writing `audio_stop` to the source's stdin, and the
    /// stop may carry a `cutoff_ms` the source must forward verbatim on its
    /// own downstream `audio_stop` (`notes/DESIGN_DICTATION_AUDIO_CUTOFF.md`).
    /// Read it back with [`SourceCtx::stop_request`].
    ///
    /// Off by default, deliberately: the monitor stages never open stdin
    /// today, and turning that on for them would be a behavior change rather
    /// than an extraction.
    pub listen_for_stop: bool,
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

    ctx.emit(event_type::CAPABILITY, &cap).await?;

    spawn_signal_watcher(ctx.stop.clone(), ctx.notify.clone());
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
fn spawn_signal_watcher(stop: Arc<AtomicBool>, notify: Arc<Notify>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let (mut term, mut int) = match (
                signal(SignalKind::terminate()),
                signal(SignalKind::interrupt()),
            ) {
                (Ok(t), Ok(i)) => (t, i),
                _ => return,
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
        }
    }

    /// Drive `serve_consumer_on` with a scripted inbound event list, returning
    /// everything the stage wrote.
    async fn run_consumer<C: Consumer>(
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

        serve_consumer_on(
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

    impl Consumer for Counter {
        async fn on_audio_start(&mut self, _ev: AudioStart, _ctx: &mut Ctx) -> Result {
            self.starts += 1;
            Ok(())
        }
        async fn on_audio_chunk(
            &mut self,
            _ev: AudioChunk,
            _payload: &[u8],
            _ctx: &mut Ctx,
        ) -> Result<Chunk> {
            self.chunks += 1;
            Ok(if self.drop_chunks {
                Chunk::Dropped
            } else {
                Chunk::Counted
            })
        }
        async fn on_audio_stop(&mut self, _ev: AudioStop, _ctx: &mut Ctx) -> Result<Flow> {
            self.stops += 1;
            Ok(if self.stop_on_stop {
                Flow::Stop
            } else {
                Flow::Continue
            })
        }
        async fn on_other(&mut self, _ev: Event, _ctx: &mut Ctx) -> Result<Flow> {
            self.others += 1;
            Ok(Flow::Continue)
        }
        async fn on_eof(&mut self, _ctx: &mut Ctx) -> Result {
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
        assert!(grants[1..]
            .iter()
            .all(|g| g.frames == 8 && g.session_id == "s1"));
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
}
