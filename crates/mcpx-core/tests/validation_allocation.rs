#![cfg(not(target_family = "wasm"))]

use mcpx_core::{ToolBinding, ToolDescriptor};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> = &stats_alloc::INSTRUMENTED_SYSTEM;

#[test]
fn a_wide_missing_required_object_does_not_allocate_thousands_of_diagnostics() {
    let descriptor: ToolDescriptor = serde_json::from_value(json!({
        "name":"wide", "inputSchema":{"type":"object","required":
            (0..4096).map(|i| format!("p{i:04}")).collect::<Vec<_>>()},
    }))
    .unwrap();
    let binding = ToolBinding::new(descriptor).unwrap();
    let region = stats_alloc::Region::new(ALLOCATOR);
    let error = binding.args().finish().err().unwrap();
    let traffic = region.change().bytes_allocated;
    assert_eq!(error.code, "ValidationFailed");
    assert_eq!(error.issues.len(), 1);
    // Deliberately loose relative to the measured 408-byte path. The previous
    // eager/prefix strategy requested over 1 MiB for this same flat error shape.
    assert!(
        traffic < 64 * 1024,
        "diagnostic allocation traffic: {traffic}"
    );
}
