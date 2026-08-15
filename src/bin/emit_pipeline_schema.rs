//! Regenerate `contracts/pipeline.json` from the crate's types.
//! Usage: `emit-pipeline-schema <output-path>` (or no arg for stdout).

fn main() {
    let doc = branchkit_stage_sdk::schema::pipeline_schema_json();
    match std::env::args().nth(1) {
        Some(path) => std::fs::write(&path, doc).expect("write schema output"),
        None => print!("{doc}"),
    }
}
