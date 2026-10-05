//! A complete request stage that upper-cases text: the shape every request
//! stage takes (a language model, a translator, a classifier). Swap the
//! upper-casing for real work and nothing else changes.
//!
//! Run it and type a request to watch the wire protocol (the handshake, then
//! one `reply` per `request`, with its id):
//!
//! ```text
//! cargo run --example upper
//! {"type":"request","data":{"request_id":"r1","body":{"text":"snap left"}}}
//! ```
//!
//! The conformance harness holds it to the request stage contract:
//!
//! ```text
//! cargo build --example upper && branchkit-stage-sdk-test target/debug/examples/upper
//! ```

use branchkit_stage_sdk::events::Capability;
use branchkit_stage_sdk::stage::{self, RequestHandler, Result};
use serde_json::{Value, json};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

async fn run() -> Result {
    let cap = Capability::new("request", "upper").persistent();
    stage::serve_requests(cap, &mut Upper).await
}

struct Upper;

impl RequestHandler for Upper {
    /// `{"text": "..."}` in, `{"text": "..."}` out; anything else is refused,
    /// which becomes the reply's `error`.
    async fn handle(&mut self, body: Value) -> std::result::Result<Value, String> {
        let text = body
            .get("text")
            .and_then(Value::as_str)
            .ok_or("expected {\"text\": \"...\"}")?;
        Ok(json!({ "text": text.to_uppercase() }))
    }
}
