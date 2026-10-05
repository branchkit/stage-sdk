//! A complete speech engine that hums instead of talking: one short tone per
//! word. The shape every text-to-speech stage takes — swap the tone for a
//! real synthesizer and nothing else changes.
//!
//! Run it and type a request to watch the wire protocol (the handshake, then
//! `audio_start`, a chunk per word, and `audio_stop`):
//!
//! ```text
//! cargo run --example hum
//! {"type":"speak","data":{"session_id":"u1","text":"snap left"}}
//! ```
//!
//! The conformance harness holds it to the speech engine contract:
//!
//! ```text
//! cargo build --example hum && branchkit-stage-sdk-test target/debug/examples/hum
//! ```

use std::time::Duration;

use branchkit_stage_sdk::events::{AudioFormat, Capability, Speak, VoiceInfo};
use branchkit_stage_sdk::stage::{self, Flow, Result, SpeakCtx, SpeechEngine};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    stage::run(run()).await
}

/// What the stage declares in its handshake. The first voice is the default.
fn capability() -> Capability {
    Capability::new("tts", "hum")
        .persistent()
        .voice(VoiceInfo::new("low", "Low hum", "en"))
        .voice(VoiceInfo::new("high", "High hum", "en"))
}

const RATE: u32 = 16_000;
/// One word's tone at normal pace.
const WORD_MS: f32 = 120.0;

struct Hum;

impl SpeechEngine for Hum {
    async fn speak(&mut self, req: Speak, ctx: &mut SpeakCtx<'_>) -> Result {
        // An unknown voice falls back to the default, as the contract says.
        let pitch = match req.voice.as_deref() {
            Some("high") => 440.0,
            _ => 220.0,
        };
        let pace = req.rate.filter(|r| *r > 0.0).unwrap_or(1.0);
        ctx.start(AudioFormat::PCM_16K_MONO).await?;
        for _word in req.text.split_whitespace() {
            // Synthesize one piece, send it, and check for a cancel before
            // making the next: the first word plays while the rest are made.
            let pcm = tone(pitch, WORD_MS / pace);
            if ctx.audio(&pcm).await? == Flow::Stop {
                return Ok(());
            }
            // A real engine spends this time synthesizing.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

/// `ms` of a sine at `hz`, 16 kHz mono int16 little-endian.
fn tone(hz: f32, ms: f32) -> Vec<u8> {
    let samples = (RATE as f32 * ms / 1000.0) as usize;
    let mut out = Vec::with_capacity(samples * 2);
    for i in 0..samples {
        let t = i as f32 / RATE as f32;
        let s = (t * hz * std::f32::consts::TAU).sin() * 0.2 * i16::MAX as f32;
        out.extend_from_slice(&(s as i16).to_le_bytes());
    }
    out
}

async fn run() -> Result {
    stage::serve_speech_engine(capability(), &mut Hum).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declares_a_default_voice() {
        let cap = capability();
        assert_eq!(cap.stage_type, "tts");
        assert_eq!(cap.voices[0].id, "low");
    }

    #[test]
    fn a_word_is_whole_frames() {
        assert_eq!(tone(220.0, WORD_MS).len() % 2, 0);
    }
}
