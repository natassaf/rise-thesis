To run this code on macOS
1) Install Rust
2) Add the ARM64 Linux target (Raspberry Pi 5 uses ARMv8/AArch64):
    rustup target add aarch64-unknown-linux-gnu
3) Create this file .cargo/config.tomp and add the following:
    [target.aarch64-unknown-linux-gnu]
    linker = "aarch64-unknown-linux-gnu-gcc"
4) Run :
    cargo build --release --target aarch64-unknown-linux-gnu



Size optimizations:
1) Add the following to cargo.toml 
    [profile.release]
    lto = true
    codegen-units = 1
    opt-level = "z"      # optimize for size
    strip = "debuginfo"  # strip debug info
2) strip target/aarch64-unknown-linux-gnu/release/rise-thesis

