cargo build --release --target aarch64-unknown-linux-gnu
aarch64-unknown-linux-gnu-strip target/aarch64-unknown-linux-gnu/release/rise-thesis

du -h target/aarch64-unknown-linux-gnu/release/rise-thesis
