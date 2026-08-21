//! A complete source stage in about forty lines — the shape a third-party
//! stage takes when its domain is one the platform has never heard of.
//!
//! Run it directly to watch the wire protocol:
//!
//! ```text
//! cargo run --example foot_pedal
//! ```
//!
//! and through the conformance harness to check it against the contract:
//!
//! ```text
//! cargo build --example foot_pedal
//! cargo run -p branchkit-stage-sdk-test -- ./target/debug/examples/foot_pedal
//! ```

use branchkit_stage_sdk::events::Capability;
use branchkit_stage_sdk::stage::{self, Result, SourceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

async fn run() -> Result {
    // `emits` is the vendor namespace this stage owns. The platform routes
    // these without decoding them — it never learns what a pedal is.
    let cap = Capability {
        stage_type: "sensor".into(),
        stage_name: "foot_pedal".into(),
        lifecycle_modes: vec!["persistent".into()],
        emits: vec!["ext.example.pedal.*".into()],
        ..Default::default()
    };

    stage::serve_source(cap, SourceOptions::default(), |mut ctx| async move {
        // A real stage would wait on its device here. This one simulates a
        // press every 300ms so the shape is visible.
        let mut down = false;
        while !ctx.stopped() {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {}
                _ = ctx.stopped_signal() => break,
            }
            down = !down;
            let event = if down {
                "ext.example.pedal.down"
            } else {
                "ext.example.pedal.up"
            };
            ctx.emit(event, &serde_json::json!({ "pedal": 1 })).await?;
        }
        branchkit_stage_sdk::stage_log::info("pedal stage shutting down");
        Ok(())
    })
    .await
}
