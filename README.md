# branchkit-stage-sdk

Write a **pipeline stage** for [BranchKit](https://github.com/branchkit), an
accessibility plugin platform for the desktop: a standalone binary the platform
spawns, sandboxes, supervises, and routes.

Stages are not plugins. A plugin is a process that holds commands, state and
behavior, written with the Go, TypeScript or Python SDK. A stage is one step of
a pipeline: an input source (a device, an OS notification, a sensor) or a
transform on a stream such as audio. Stages are written in Rust, against this
crate, and a plugin ships them.

A stage speaks a small line protocol over stdin and stdout — one JSON header
line, optionally followed by a binary payload. This crate is the layer above
that: it owns the obligations every stage otherwise hand-rolls, and that fail
*silently* when hand-rolled wrong.

MIT licensed. Depends on nothing from the BranchKit platform.

## Install

This README describes 0.2. crates.io has only 0.1.0 so far, which predates
this API, so until 0.2 is published depend on the repository:

```toml
[dependencies]
branchkit-stage-sdk = { git = "https://github.com/branchkit/stage-sdk" }
tokio = { version = "1", features = ["full"] }
serde_json = "1"   # the examples build event data with serde_json::json!
```

Once 0.2 is on crates.io, `branchkit-stage-sdk = "0.2"` replaces the git line.
Requires Rust 1.85 or newer (edition 2024).

A stage is a `tokio` binary, so you bring your own runtime. The `schema`
feature is for regenerating the platform's pipeline schema and is not
something a stage enables.

**Coming from 0.1:** build a `Capability` with `Capability::new(type, name)`
and its setters (`.persistent()`, `.emits([...])`, `.stream(...)`, …), and
`SourceOptions` with `SourceOptions::new().listen_for_stop(true)`. Both are
`#[non_exhaustive]` from 0.2, so a struct literal no longer compiles, and a
field added in a later release will not break your stage.

## Three shapes

```rust
use branchkit_stage_sdk::events::Capability;
use branchkit_stage_sdk::stage::{self, Result, SourceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

async fn run() -> Result {
    let cap = Capability::new("monitor", "pedal")
        .persistent()
        .emits(["ext.acme.pedal"]);

    stage::serve_source(cap, SourceOptions::default(), |mut ctx| async move {
        while !ctx.stopped() {
            // ...wait on your device...
            ctx.emit("ext.acme.pedal", &serde_json::json!({ "down": true }))
                .await?;
        }
        Ok(())
    })
    .await
}
```

- `stage::serve_source` — **notifier-driven**. Your device, an OS
  notification, a timer. May never read stdin. Domain-free.
- `stage::serve_audio_consumer` — **read-driven**, for a stage in an audio
  chain. Audio-bound, because audio is the only *stream* the wire has today.
- `stage::serve_speech_engine` — **request-driven**, for a text-to-speech
  engine (`stage_type` `tts`). Each `speak` request becomes an audio session
  you produce, streaming; you implement how to say one request, and the
  runtime owns the order, cancellation, and closing every utterance.

### A speech engine

```rust
use branchkit_stage_sdk::events::{AudioFormat, Capability, Speak, VoiceInfo};
use branchkit_stage_sdk::stage::{self, Flow, Result, SpeakCtx, SpeechEngine};

struct MyVoice;

impl SpeechEngine for MyVoice {
    async fn speak(&mut self, req: Speak, ctx: &mut SpeakCtx<'_>) -> Result {
        ctx.start(AudioFormat::PCM_16K_MONO).await?;
        for piece in synthesize(&req.text) {
            // Send each piece as it is made; Flow::Stop means cancelled.
            if ctx.audio(&piece).await? == Flow::Stop {
                break;
            }
        }
        Ok(())
    }
}

async fn run() -> Result {
    let cap = Capability::new("tts", "my_voice")
        .persistent()
        .voice(VoiceInfo::new("warm", "Warm (English)", "en-US"));
    stage::serve_speech_engine(cap, &mut MyVoice).await
}
```

Declare at least one voice; the first is the default. A request may name a
voice, a pace (`rate`, 1.0 = normal) and a language. Send audio in pieces as
you synthesize it, never the whole utterance at the end, so the first words
play while the rest are made. Synthesis that blocks a thread goes under
`tokio::task::spawn_blocking` and watches `ctx.cancel_flag()`: the platform
cancels an utterance the moment the person starts talking.
`examples/hum.rs` is a complete one that hums a tone per word.

## Your own vocabulary

The typed event vocabulary is closed and small — the platform types an event
only when it must read the contents to do its own job. Everything else is
yours, under **`ext.<vendor>.<name>`**:

- Declare what you emit in `Capability::emits`.
- Prefer three-segment names. A plugin subscribing to `ext.<vendor>.*`
  receives `ext.acme.pedal` but not `ext.acme.pedal.left`: a subscription's
  `*` is exactly one segment. `ext.<vendor>.**` receives every depth.
  (Your own `ext.<vendor>.*` in `emits` covers every depth either way;
  `events::declaration_covers` is that rule.)
- Declare what you accept from the stage above you in `Capability::consumes`.
  A declared type is delivered to you; an undeclared one goes to the platform's
  event bus instead, where plugins subscribe.
- Binary payloads ride along, so a frame source hands real bytes to the stage
  after it.
- The bus admits up to 1000 of a stage's events per second, with up to 64 KB
  of JSON `data` each. A sensor faster than that batches samples into fewer
  events.
- Declare each steady stream's rate in `Capability::streams`. The platform
  checks the declarations when your stage starts and refuses to run it —
  naming the stream and the limit — if it cannot carry them, rather than
  dropping events later. `Capability::check_streams` is that check, so a
  unit test can run it too.
- A stream that is *state* — a gaze position, a pointer, a pedal's travel —
  can be declared `latest`: each subscriber gets the newest value at the pace
  it can take, instead of every sample or a backlog of stale ones. Put the
  whole value in each event's `data`. Things that *happen* (a blink, a key)
  keep the default, `every`.
- A type you consume that your upstream never emits fails when the pipeline
  starts, naming both stages — rather than leaving you waiting.

The platform never decodes these. Two stages agree on a format between
themselves, and the platform routes bytes.

A stage with steady streams declares them:

```rust
use branchkit_stage_sdk::events::StreamDecl;

let cap = Capability::new("sensor", "gaze")
    .persistent()
    .emits(["ext.acme.*"])
    .stream(StreamDecl::latest("ext.acme.gaze_point", 250))
    .stream(StreamDecl::every("ext.acme.blink", 5));
```

`examples/gaze.rs` is a complete one. A platform that predates `streams`
ignores the field and delivers every event, so declaring is always safe.

## Flow credit: which side are you on

Get this backwards and you get a stall, not an error.

- **Consuming audio → you must grant credit.** `CreditPolicy` drives it for you.
- **Producing audio → do not implement credit at all** (a speech engine
  included). The platform holds the
  sender-side window; a producer that outruns it blocks on the pipe. There is
  deliberately no sender-side helper here.

## Testing

A stage is a program that speaks its protocol on stdin and stdout, so you can
run it and read what it says. Its first line is the capability handshake;
every line after is an event:

```bash
cargo run --example foot_pedal
```

A source stage stops on SIGTERM, which is how the platform ends it, or on
Ctrl-C.

The rules the platform applies to your declarations when the stage starts are
functions in this crate, so a unit test can hold your capability to them:

- `Capability::check_streams` — the start-up check for declared streams.
- `events::declaration_covers` — whether an `emits` entry covers an event type.
  The platform does not route a type your declaration does not cover.

Both examples carry such tests: `cargo test --example gaze`.

## Shipping it

A stage is shipped **by a plugin** — `provides.stages` in `plugin.json` — and
runs under that plugin's sandbox profile. The platform docs that ship with the
app cover the manifest side in `how-to/pipelines/contributing-a-stage.md`
(`branchkit-cli docs path` prints the directory; see
[branchkit-cli](https://github.com/branchkit/branchkit-cli)).

## License

MIT. See [LICENSE](LICENSE).
