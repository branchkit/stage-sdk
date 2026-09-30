//! A sensor that runs at a rate: the shape of an eye tracker.
//!
//! Two streams, declared in the capability so the platform can check them
//! when the stage starts:
//!
//! - `ext.example.gaze_point` — where the person is looking, 250 times a
//!   second. That is STATE: each sample replaces the last, and a subscriber
//!   moving a cursor wants the newest position at the pace it can draw, not
//!   a queue of old ones. Declared `latest`.
//! - `ext.example.blink` — a thing that HAPPENED. Every one matters, so it
//!   keeps the default, `every`.
//!
//! Run it directly to watch the wire protocol on stdout — the capability
//! handshake first, then one event per line. Ctrl-C stops it cleanly, the way
//! the platform's SIGTERM does:
//!
//! ```text
//! cargo run --example gaze
//! ```
//!
//! Its tests check the stream declarations with the rule the platform applies
//! when the stage starts, [`Capability::check_streams`]:
//!
//! ```text
//! cargo test --example gaze
//! ```

use std::time::Duration;

use branchkit_stage_sdk::events::{Capability, StreamDecl};
use branchkit_stage_sdk::stage::{self, Result, SourceOptions};

/// The tracker's sampling rate, and so the stream's declared rate.
const GAZE_HZ: u32 = 250;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

/// What the stage declares in its handshake.
fn capability() -> Capability {
    Capability::new("sensor", "gaze")
        .persistent()
        .emits(["ext.example.*"])
        .stream(StreamDecl::latest("ext.example.gaze_point", GAZE_HZ))
        .stream(StreamDecl::every("ext.example.blink", 2))
}

async fn run() -> Result {
    let cap = capability();
    stage::serve_source(cap, SourceOptions::default(), |mut ctx| async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1) / GAZE_HZ);
        // A late tick is skipped, never made up in a burst: the stream must
        // stay within the rate it declared.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut n: u64 = 0;
        while !ctx.stopped() {
            tokio::select! {
                _ = tick.tick() => {}
                _ = ctx.stopped_signal() => break,
            }
            n += 1;
            // A real stage reads its device here. This one traces a slow
            // circle so the values are visibly state.
            let angle = n as f64 / f64::from(GAZE_HZ);
            ctx.emit(
                "ext.example.gaze_point",
                &serde_json::json!({ "x": 0.5 + 0.25 * angle.cos(), "y": 0.5 + 0.25 * angle.sin() }),
            )
            .await?;
            if n % u64::from(GAZE_HZ) == 0 {
                ctx.emit("ext.example.blink", &serde_json::json!({ "eye": "both" }))
                    .await?;
            }
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The platform checks these declarations when the stage starts and
    /// refuses to run it if it cannot carry them. Both streams fit.
    #[test]
    fn the_platform_admits_its_streams() {
        assert_eq!(capability().check_streams(), Ok(()));
    }
}
