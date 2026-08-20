//! Regenerate the Go and TypeScript event vocabulary from the pipeline
//! contract. Usage: `emit-stage-sdk <go-output-path> <ts-output-path>`.

use std::io::Write;
use std::process::{Command, Stdio};

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(go_path), Some(ts_path)) = (args.next(), args.next()) else {
        eprintln!("usage: emit-stage-sdk <go-output-path> <ts-output-path>");
        std::process::exit(2);
    };
    let doc = branchkit_stage_sdk::schema::pipeline_schema_value();
    let go = gofmt(branchkit_stage_sdk::codegen::go_source(&doc));
    std::fs::write(&go_path, go).expect("write Go output");
    std::fs::write(&ts_path, branchkit_stage_sdk::codegen::ts_source(&doc))
        .expect("write TS output");
}

/// Pipe emitted Go through `gofmt`, so "generated Go" is by definition
/// gofmt-clean rather than a permanent `gofmt -l` finding that a drift gate
/// then pins in place. Same reasoning — and the same fixed-point loop — as
/// `emit-sdk`'s formatter: gofmt expands statement layout first and aligns
/// second on some inputs, so one pass is not always enough.
///
/// A missing gofmt fails loudly. It is not a new dependency in practice: the
/// lanes that run this build Go plugins anyway.
fn gofmt(source: String) -> String {
    let mut current = source;
    for _ in 0..4 {
        let next = one_pass(&current);
        if next == current {
            return current;
        }
        current = next;
    }
    current
}

fn one_pass(source: &str) -> String {
    let mut child = Command::new("gofmt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("gofmt must be on PATH to emit generated Go");
    child
        .stdin
        .take()
        .expect("gofmt stdin")
        .write_all(source.as_bytes())
        .expect("write to gofmt");
    let out = child.wait_with_output().expect("gofmt runs");
    if !out.status.success() {
        panic!(
            "gofmt rejected generated source:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    String::from_utf8(out.stdout).expect("gofmt emits utf-8")
}
