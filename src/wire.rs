//! Binary streaming framing for the pipeline dispatch path.
//!
//! Each wire event is one JSON header line terminated by `\n`, optionally
//! followed by exactly `payload_length` bytes of binary payload. Same
//! framing across stdio child-process, Unix socket, and TCP. Locked
//! 2026-05-03 — see `docs/design/DESIGN_PIPELINE_PRIMITIVE.md` section 8.1.
//!
//! Wire shape:
//!
//! ```text
//! {"type":"audio_chunk","data":{"session_id":"...","timestamp_ms":120},"payload_length":640}\n
//! <640 bytes of int16 PCM>
//! ```
//!
//! Rules:
//! - Header is a single-line JSON object terminated by `\n`. No length
//!   prefix on the JSON; readers are line-delimited.
//! - `type` is required. `data` carries event-specific JSON (omitted when
//!   empty / null). `payload_length` is omitted when zero, otherwise gives
//!   the exact byte count of the binary block following the newline.
//! - Binary block (when present) is exactly `payload_length` bytes — no
//!   inner framing, no escaping; read with a single fixed-length read.
//! - The reader and writer are split halves of a single underlying
//!   bidirectional channel (e.g. one for the stage's stdin, one for its
//!   stdout). Each half is single-owner — the caller must serialize
//!   externally if multiple producers share a writer.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

/// One wire-format message: a typed event tag, an opaque JSON `data`
/// blob (the event's typed body, kept as `Value` at the wire layer), and
/// an optional binary payload (used today only by `audio_chunk`).
#[derive(Debug, Clone)]
pub struct Event {
    pub event_type: String,
    pub data: Value,
    pub payload: Vec<u8>,
}

impl Event {
    pub fn new(event_type: impl Into<String>, data: Value) -> Self {
        Self {
            event_type: event_type.into(),
            data,
            payload: Vec::new(),
        }
    }

    pub fn with_payload(event_type: impl Into<String>, data: Value, payload: Vec<u8>) -> Self {
        Self {
            event_type: event_type.into(),
            data,
            payload,
        }
    }
}

/// The serialized header line — one JSON object per event, newline-terminated,
/// followed by exactly `payload_length` raw bytes when non-zero. `data` is
/// omitted when empty/null; `payload_length` is omitted when zero.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub(crate) struct WireHeader {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default, skip_serializing_if = "is_empty_value")]
    data: Value,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    payload_length: usize,
}

fn is_empty_value(v: &Value) -> bool {
    matches!(v, Value::Null) || matches!(v, Value::Object(m) if m.is_empty())
}

fn is_zero_usize(n: &usize) -> bool {
    *n == 0
}

/// Reads framed events from any `AsyncRead`. Single-owner — do not share
/// across tasks.
pub struct Reader<R> {
    inner: BufReader<R>,
    line_buf: String,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub fn new(r: R) -> Self {
        Self {
            inner: BufReader::new(r),
            line_buf: String::with_capacity(256),
        }
    }

    /// Maximum payload size we'll allocate (16 MB). Prevents a
    /// misbehaving stage from OOM-ing the actuator with a bogus header.
    const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

    /// Read the next event, or `Ok(None)` on clean EOF.
    pub async fn read_event(&mut self) -> io::Result<Option<Event>> {
        self.line_buf.clear();
        let n = self.inner.read_line(&mut self.line_buf).await?;
        if n == 0 {
            return Ok(None);
        }
        let line = self.line_buf.trim_end_matches('\n');
        let header: WireHeader = serde_json::from_str(line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("wire: bad header {line:?}: {e}"),
            )
        })?;
        if header.payload_length > Self::MAX_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wire: payload_length {} exceeds 16 MB cap",
                    header.payload_length
                ),
            ));
        }
        let mut payload = vec![0u8; header.payload_length];
        if header.payload_length > 0 {
            self.inner.read_exact(&mut payload).await?;
        }
        Ok(Some(Event {
            event_type: header.event_type,
            data: header.data,
            payload,
        }))
    }
}

/// Writes framed events to any `AsyncWrite`. Single-owner — do not share
/// across tasks; if multiple producers must share, wrap in a `Mutex`.
pub struct Writer<W> {
    inner: tokio::io::BufWriter<W>,
}

impl<W: AsyncWrite + Unpin> Writer<W> {
    pub fn new(w: W) -> Self {
        Self {
            inner: tokio::io::BufWriter::new(w),
        }
    }

    /// Write an event and flush immediately. BufWriter coalesces the
    /// header + payload into fewer syscalls than raw writes.
    pub async fn write_event(&mut self, ev: &Event) -> io::Result<()> {
        let header = WireHeader {
            event_type: ev.event_type.clone(),
            data: ev.data.clone(),
            payload_length: ev.payload.len(),
        };
        let mut buf = serde_json::to_vec(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        buf.push(b'\n');
        self.inner.write_all(&buf).await?;
        if !ev.payload.is_empty() {
            self.inner.write_all(&ev.payload).await?;
        }
        self.inner.flush().await?;
        Ok(())
    }

    /// Write header + borrowed payload without constructing an Event.
    /// Avoids per-frame payload allocation in hot loops where the same
    /// buffer is reused across iterations.
    pub async fn write_raw(
        &mut self,
        event_type: &str,
        data: Value,
        payload: &[u8],
    ) -> io::Result<()> {
        let header = WireHeader {
            event_type: event_type.to_string(),
            data,
            payload_length: payload.len(),
        };
        let mut buf = serde_json::to_vec(&header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        buf.push(b'\n');
        self.inner.write_all(&buf).await?;
        if !payload.is_empty() {
            self.inner.write_all(payload).await?;
        }
        Ok(())
    }

    /// Flush the underlying buffer to the transport.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::duplex;

    #[tokio::test]
    async fn roundtrip_event_with_no_payload() {
        let (a, b) = duplex(4096);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let ev = Event::new("audio_stop", json!({"session_id": "abc"}));
        writer.write_event(&ev).await.unwrap();
        let got = reader.read_event().await.unwrap().expect("event");
        assert_eq!(got.event_type, "audio_stop");
        assert_eq!(got.data, json!({"session_id": "abc"}));
        assert!(got.payload.is_empty());
    }

    #[tokio::test]
    async fn roundtrip_event_with_payload() {
        let (a, b) = duplex(4096);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let payload: Vec<u8> = (0..640).map(|i| (i & 0xff) as u8).collect();
        let ev = Event::with_payload(
            "audio_chunk",
            json!({"session_id": "abc", "timestamp_ms": 120}),
            payload.clone(),
        );
        writer.write_event(&ev).await.unwrap();
        let got = reader.read_event().await.unwrap().expect("event");
        assert_eq!(got.event_type, "audio_chunk");
        assert_eq!(got.payload, payload);
    }

    #[tokio::test]
    async fn multiple_events_roundtrip_in_order() {
        let (a, b) = duplex(8192);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        for i in 0u32..5 {
            let ev = Event::with_payload(
                "audio_chunk",
                json!({"session_id": "s", "timestamp_ms": i * 20}),
                vec![i as u8; 16],
            );
            writer.write_event(&ev).await.unwrap();
        }
        for i in 0u32..5 {
            let got = reader.read_event().await.unwrap().expect("event");
            assert_eq!(got.event_type, "audio_chunk");
            assert_eq!(got.payload, vec![i as u8; 16]);
        }
    }

    #[tokio::test]
    async fn malformed_header_is_invalid_data_error() {
        let (mut a, b) = duplex(4096);
        let mut reader = Reader::new(b);
        a.write_all(b"not-json\n").await.unwrap();
        let err = reader.read_event().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn payload_length_mismatch_is_unexpected_eof() {
        let (mut a, b) = duplex(4096);
        let mut reader = Reader::new(b);
        // Header claims 640 bytes but we only send 16.
        a.write_all(br#"{"type":"audio_chunk","data":{},"payload_length":640}"#)
            .await
            .unwrap();
        a.write_all(b"\n").await.unwrap();
        a.write_all(&[0u8; 16]).await.unwrap();
        drop(a); // close write end so read_exact gets EOF rather than blocking
        let err = reader.read_event().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (a, b) = duplex(4096);
        let mut reader = Reader::new(b);
        drop(a);
        let got = reader.read_event().await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn header_omits_zero_payload_length() {
        let (a, b) = duplex(4096);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let ev = Event::new("audio_stop", serde_json::json!({"session_id": "x"}));
        writer.write_event(&ev).await.unwrap();
        // Re-read at the byte level to verify the header doesn't include
        // `payload_length` when zero.
        let mut raw = [0u8; 256];
        let n = reader.inner.read(&mut raw).await.unwrap();
        let line = std::str::from_utf8(&raw[..n]).unwrap();
        assert!(!line.contains("payload_length"), "header was: {line}");
    }

    #[tokio::test]
    async fn header_omits_empty_data_object() {
        let (a, b) = duplex(4096);
        let mut writer = Writer::new(a);
        let mut reader = Reader::new(b);
        let ev = Event::new("audio_stop", serde_json::json!({}));
        writer.write_event(&ev).await.unwrap();
        let mut raw = [0u8; 256];
        let n = reader.inner.read(&mut raw).await.unwrap();
        let line = std::str::from_utf8(&raw[..n]).unwrap();
        assert!(!line.contains("\"data\""), "header was: {line}");
    }

    #[tokio::test]
    async fn oversized_payload_length_rejected() {
        let (mut a, b) = duplex(4096);
        let mut reader = Reader::new(b);
        // Claim 32 MB payload — exceeds the 16 MB cap.
        a.write_all(br#"{"type":"audio_chunk","data":{},"payload_length":33554432}"#)
            .await
            .unwrap();
        a.write_all(b"\n").await.unwrap();
        let err = reader.read_event().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("16 MB cap"));
    }
}
