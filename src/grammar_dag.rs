//! The engine-facing wire projection of the command-grammar DAG.
//!
//! Only the serde wire types live here — what a recognition stage *decodes*.
//! The DAG builder (compilation from the command registry, eligibility
//! scoping, `to_wire()`) is platform logic and stays in
//! `branch_actuator::pipeline::grammar_dag`, which re-exports these types so
//! producer and consumer share one definition — no second copy to drift.
//!
//! A recognition stage DECODES these; the builder that produces them lives
//! in the platform, which is why only the wire types are here.

use serde::{Deserialize, Serialize};

/// A DAG state. `START` is the entry/accept state; every command-final arc
/// returns here.
pub type StateId = usize;

/// The start (and accept) state of every compiled grammar.
pub const START: StateId = 0;

/// The engine-facing wire projection of the compiled grammar DAG (Phase D2
/// contract).
///
/// This is the JSON the actuator attaches to a `vocabulary_update` and the CTC
/// stage feeds to the patched `GrammarBuilder` to build `G`. It is a *reduced*
/// view of the DAG — exactly what FST construction needs, nothing the matcher
/// keeps:
/// - `start` (always 0) is `G`'s start AND its sole base accept state: every
///   command-final arc returns there, so accepting at `start` accepts any
///   sequence of complete commands.
/// - `arcs` are word-level `(from → word → to)` transitions; the builder maps
///   `word` to the same word id `H ∘ L` emits and rides `weight` onto the arc.
/// - `open_states` are states whose continuation D1 could not enumerate (free
///   text, dependent capture, list repeat/optional). The builder realizes each
///   as final + a self-loop over the full word alphabet (the back-arc into a
///   word loop), so those commands still recognize their open tail.
///
/// D3 extends this with per-arc command ids; D2 carries words only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GrammarDagWire {
    pub num_states: usize,
    pub start: StateId,
    pub arcs: Vec<WireArc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_states: Vec<OpenState>,
    /// Alternatives the compiler dropped under the empty-list policy —
    /// telemetry only, the FST builder ignores it.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub dropped_alts: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl GrammarDagWire {
    /// Structural fingerprint of the wire form — every field that changes the
    /// serialized JSON feeds the hash (arcs in order, open states with their
    /// alphabets, `dropped_alts`), so hash-equal ⟺ byte-equal for the
    /// deterministic compiler output. Replaces hashing the serialized JSON
    /// string in the broadcast dedup gate: same discrimination, no
    /// serialization on a hot path. Not `derive(Hash)`
    /// because `weight: Option<f32>` needs `to_bits`.
    pub fn structural_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        // Full destructuring (no `..`) on every struct so adding a field to
        // any of them is a COMPILE ERROR here — this hash is the broadcast
        // dedup gate's view of the DAG, and a field it silently misses would
        // let a real grammar change dedup away (stale grammar, no error
        // surface). D3 already plans to grow `WireArc`.
        let GrammarDagWire {
            num_states,
            start,
            arcs,
            open_states,
            dropped_alts,
        } = self;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        num_states.hash(&mut h);
        start.hash(&mut h);
        arcs.len().hash(&mut h);
        for a in arcs {
            let WireArc {
                from,
                word,
                to,
                weight,
            } = a;
            from.hash(&mut h);
            word.hash(&mut h);
            to.hash(&mut h);
            weight.map(f32::to_bits).hash(&mut h);
        }
        open_states.len().hash(&mut h);
        for o in open_states {
            let OpenState { state, alphabet } = o;
            state.hash(&mut h);
            alphabet.hash(&mut h);
        }
        dropped_alts.hash(&mut h);
        h.finish()
    }
}

/// A state whose continuation D1 could not fully enumerate. The builder realizes
/// it as final + a self-loop. `alphabet` bounds that self-loop when known;
/// omitted (`None`) means loop over the full recognition union — the phase-2
/// default and the always-safe fallback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OpenState {
    pub state: StateId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alphabet: Option<Vec<String>>,
}

/// One word-level arc in [`GrammarDagWire`]. `weight` is omitted when absent
/// (D1 leaves it unset; a later weight policy populates it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct WireArc {
    pub from: StateId,
    pub word: String,
    pub to: StateId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase2_none_alphabet_adds_no_wire_bytes() {
        // The phase-2 no-op guarantee: while every open tail is unbounded
        // (`alphabet: None`), an open state serializes to exactly `{"state":N}`
        // — the field is skipped — so the wire the CTC stage parses is unchanged.
        let os = OpenState {
            state: 4,
            alphabet: None,
        };
        assert_eq!(serde_json::to_string(&os).unwrap(), r#"{"state":4}"#);
        // And a bounded one (phase 3+) carries its words.
        let bounded = OpenState {
            state: 4,
            alphabet: Some(vec!["a".into(), "b".into()]),
        };
        assert_eq!(
            serde_json::to_string(&bounded).unwrap(),
            r#"{"state":4,"alphabet":["a","b"]}"#
        );
    }
}
