//! A complete source stage in about forty lines — the shape a third-party
//! stage takes when its domain is one the platform has never heard of.
//!
//! Run it directly to watch the wire protocol on stdout — the capability
//! handshake first, then one event per line. Ctrl-C stops it cleanly, the way
//! the platform's SIGTERM does:
//!
//! ```text
//! cargo run --example foot_pedal
//! ```
//!
//! Its tests check the capability with the rules the platform applies when
//! the stage starts:
//!
//! ```text
//! cargo test --example foot_pedal
//! ```

use branchkit_stage_sdk::events::Capability;
use branchkit_stage_sdk::stage::{self, Result, SourceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

const PEDAL_DOWN: &str = "ext.example.pedal.down";
const PEDAL_UP: &str = "ext.example.pedal.up";

/// What the stage declares in its handshake.
fn capability() -> Capability {
    // `emits` is the vendor namespace this stage owns. The platform routes
    // these without decoding them — it never learns what a pedal is.
    Capability::new("sensor", "foot_pedal")
        .persistent()
        .emits(["ext.example.pedal.*"])
}

async fn run() -> Result {
    let cap = capability();
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
            let event = if down { PEDAL_DOWN } else { PEDAL_UP };
            ctx.emit(event, &serde_json::json!({ "pedal": 1 })).await?;
        }
        branchkit_stage_sdk::stage_log::info("pedal stage shutting down");
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use branchkit_stage_sdk::events::declaration_covers;

    /// Every event the stage emits is one its `emits` declaration covers.
    /// The platform drops an undeclared type rather than routing it.
    #[test]
    fn declares_what_it_emits() {
        let cap = capability();
        for event in [PEDAL_DOWN, PEDAL_UP] {
            assert!(
                cap.emits.iter().any(|d| declaration_covers(d, event)),
                "{event} is not covered by {:?}",
                cap.emits
            );
        }
    }

    /// The platform refuses to start a stage whose stream declarations it
    /// cannot carry; this one declares none, so there is nothing to refuse.
    #[test]
    fn the_platform_admits_it() {
        assert_eq!(capability().check_streams(), Ok(()));
    }
}
