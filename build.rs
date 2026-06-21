fn main() {
    protobuf_codegen::Codegen::new()
        .pure()
        .include("src/cla")
        .input("src/cla/bundle.proto")
        .cargo_out_dir("proto")
        .run_from_script();

    // Strip inner attributes from generated file so include! works
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let path = std::path::Path::new(&out_dir).join("proto/bundle.rs");
    let content = std::fs::read_to_string(&path).unwrap();
    let cleaned = content
        .lines()
        .filter(|line| !line.starts_with("#![") && !line.starts_with("//!"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, cleaned).unwrap();
}