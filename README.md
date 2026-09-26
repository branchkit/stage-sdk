# branchkit-stage-sdk

Write a **pipeline stage** for [BranchKit](https://branchkit.dev): a standalone
binary the platform spawns, sandboxes, supervises, and routes.

A stage speaks a small line protocol over stdin and stdout — one JSON header
line, optionally followed by a binary payload. This crate is the layer above
that: it owns the obligations every stage otherwise hand-rolls, and that fail
*silently* when hand-rolled wrong.

MIT licensed. Depends on nothing from the BranchKit platform.

## Install

```bash
cargo add branchkit-stage-sdk
```

Or by hand:

```toml
[dependencies]
branchkit-stage-sdk = "0.2"
tokio = { version = "1", features = ["full"] }
```

A stage is a `tokio` binary, so you bring your own runtime. The `schema`
feature is for regenerating the platform's pipeline schema and is not
something a stage enables.

**Coming from 0.1:** build a `Capability` with `Capability::new(type, name)`
and its setters (`.persistent()`, `.emits([...])`, `.stream(...)`, …), and
`SourceOptions` with `SourceOptions::new().listen_for_stop(true)`. Both are
`#[non_exhaustive]` from 0.2, so a struct literal no longer compiles, and a
field added in a later release will not break your stage.

## Two shapes

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

- [`stage::serve_source`] — **notifier-driven**. Your device, an OS
  notification, a timer. May never read stdin. Domain-free.
- [`stage::serve_audio_consumer`] — **read-driven**, for a stage in an audio
  chain. Audio-bound, because audio is the only *stream* the wire has today.

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
  dropping events later. `Capability::check_streams` is that check; the
  conformance harness runs it too.
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
- **Producing audio → do not implement credit at all.** The platform holds the
  sender-side window; a producer that outruns it blocks on the pipe. There is
  deliberately no sender-side helper here.

## Testing

The conformance harness is the acceptance bar. Point it at your binary and it
picks a suite from your own capability declaration:

```bash
cargo run -p branchkit-stage-sdk-test -- ./target/debug/my_stage
```

It checks the handshake, drives a credit-accounted session as a true sender
(so a stage whose grants trail its cadence starves and fails), verifies
unknown-event leniency and error-path termination, and validates every byte you
emit against the strict framing rules. A source stage's declared streams are
checked with the platform's own start-up rule and held to their declared
rates.

## Shipping it

A stage is shipped **by a plugin** — `provides.stages` in `plugin.json` — and
runs under that plugin's sandbox profile. See
[Contributing a Pipeline Stage](https://branchkit.dev/how-to/pipelines/contributing-a-stage).

## License

MIT. See [LICENSE](LICENSE).
