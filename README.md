# branchkit-stage-sdk

Write a **pipeline stage** for [BranchKit](https://branchkit.dev): a standalone
binary the platform spawns, sandboxes, supervises, and routes.

A stage speaks a small line protocol over stdin and stdout — one JSON header
line, optionally followed by a binary payload. This crate is the layer above
that: it owns the obligations every stage otherwise hand-rolls, and that fail
*silently* when hand-rolled wrong.

MIT licensed. Depends on nothing from the BranchKit platform.

## Two shapes

```rust
use branchkit_stage_sdk::events::Capability;
use branchkit_stage_sdk::stage::{self, Result, SourceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

async fn run() -> Result {
    let cap = Capability {
        stage_type: "monitor".into(),
        stage_name: "pedal".into(),
        lifecycle_modes: vec!["persistent".into()],
        emits: vec!["ext.acme.pedal".into()],
        ..Default::default()
    };

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
- Declare what you accept from the stage above you in `Capability::consumes`.
  A declared type is delivered to you; an undeclared one goes to the platform's
  event bus instead, where plugins subscribe.
- Binary payloads ride along, so a frame source hands real bytes to the stage
  after it.
- A type you consume that your upstream never emits fails when the pipeline
  starts, naming both stages — rather than leaving you waiting.

The platform never decodes these. Two stages agree on a format between
themselves, and the platform routes bytes.

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
emit against the strict framing rules.

## Shipping it

A stage is shipped **by a plugin** — `provides.stages` in `plugin.json` — and
runs under that plugin's sandbox profile. See
[Contributing a Pipeline Stage](https://branchkit.dev/how-to/pipelines/contributing-a-stage).

## License

MIT. See [LICENSE](LICENSE).
