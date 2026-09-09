//! Receiver-side flow-credit granting.
//!
//! The pipeline contract mandates window-based backpressure: the receiver
//! advertises credit, the sender decrements per `audio_chunk` and blocks at
//! zero. HOW MUCH credit to advertise — the initial window, the grant cadence,
//! the grant size — is receiver-chosen buffering policy and deliberately NOT
//! part of the contract (a VAD gate buffering freely wants a wide window; an
//! STT engine wants shallow queues). This helper owns only the mechanism every
//! receiver used to hand-roll: the chunks-since-last-grant counter and the
//! `flow_credit` emission. Each stage keeps its exact numbers at the call
//! site.

use std::io;

use tokio::io::AsyncWrite;

use crate::events::{FlowCredit, event_type};
use crate::wire::{Event, Writer};

/// Counts processed chunks and grants `grant` frames of credit after every
/// `every` chunks. Unconditional grants (the initial window at startup or
/// session start, a re-grant at an utterance boundary) go through
/// [`CreditGranter::grant_now`], which also resets the cadence counter.
///
/// The counter deliberately survives session boundaries — a stage that wants
/// per-session cadence resets via `grant_now`'s initial grant (whisper-stt),
/// and one that doesn't just never resets (vad-gate).
pub struct CreditGranter {
    every: u32,
    grant: u32,
    since: u32,
}

impl CreditGranter {
    pub fn new(every: u32, grant: u32) -> Self {
        Self {
            every,
            grant,
            since: 0,
        }
    }

    /// Emit `frames` of credit unconditionally and reset the cadence counter.
    pub async fn grant_now<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut Writer<W>,
        session_id: &str,
        frames: u32,
    ) -> io::Result<()> {
        self.since = 0;
        emit(writer, session_id, frames).await
    }

    /// Count one processed chunk; after every `every` chunks, emit `grant`
    /// frames of credit and reset the counter.
    pub async fn on_chunk<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut Writer<W>,
        session_id: &str,
    ) -> io::Result<()> {
        self.since += 1;
        if self.since >= self.every {
            self.since = 0;
            emit(writer, session_id, self.grant).await?;
        }
        Ok(())
    }
}

async fn emit<W: AsyncWrite + Unpin>(
    writer: &mut Writer<W>,
    session_id: &str,
    frames: u32,
) -> io::Result<()> {
    let data = serde_json::to_value(&FlowCredit {
        session_id: session_id.to_string(),
        frames,
    })
    .expect("FlowCredit serializes");
    writer
        .write_event(&Event::new(event_type::FLOW_CREDIT, data))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Reader;
    use tokio::io::duplex;

    async fn drain_credits(
        reader: &mut Reader<tokio::io::DuplexStream>,
        n: usize,
    ) -> Vec<FlowCredit> {
        let mut out = Vec::new();
        for _ in 0..n {
            let ev = reader.read_event().await.unwrap().expect("event");
            assert_eq!(ev.event_type, event_type::FLOW_CREDIT);
            out.push(serde_json::from_value(ev.data).unwrap());
        }
        out
    }

    #[tokio::test]
    async fn grants_on_cadence_with_configured_size() {
        let (a, b) = duplex(65536);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let mut credit = CreditGranter::new(4, 8);
        // 9 chunks at every=4 → grants after chunk 4 and chunk 8, not 9.
        for _ in 0..9 {
            credit.on_chunk(&mut writer, "s1").await.unwrap();
        }
        let grants = drain_credits(&mut reader, 2).await;
        assert!(grants.iter().all(|g| g.frames == 8 && g.session_id == "s1"));
    }

    #[tokio::test]
    async fn grant_now_resets_the_cadence_counter() {
        let (a, b) = duplex(65536);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let mut credit = CreditGranter::new(4, 4);
        // 3 chunks (no grant yet), then an unconditional boundary re-grant.
        for _ in 0..3 {
            credit.on_chunk(&mut writer, "s1").await.unwrap();
        }
        credit.grant_now(&mut writer, "", 16).await.unwrap();
        // Counter was reset: the next grant needs 4 MORE chunks, so after 3
        // more only the boundary grant is on the wire.
        for _ in 0..3 {
            credit.on_chunk(&mut writer, "s1").await.unwrap();
        }
        let grants = drain_credits(&mut reader, 1).await;
        assert_eq!(grants[0].frames, 16);
        assert_eq!(grants[0].session_id, "");
        // The 4th post-boundary chunk completes the cadence.
        credit.on_chunk(&mut writer, "s1").await.unwrap();
        let grants = drain_credits(&mut reader, 1).await;
        assert_eq!(grants[0].frames, 4);
    }
}
