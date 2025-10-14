To run this code on Rasberry
1) Install Rust
2) Add the ARM64 Linux target (Raspberry Pi 5 uses ARMv8/AArch64):
    rustup target add aarch64-unknown-linux-gnu
3) Create this file .cargo/config.tomp and add the following:
    [target.aarch64-unknown-linux-gnu]
    linker = "aarch64-unknown-linux-gnu-gcc"
4) Run:
    cargo build --release --target aarch64-unknown-linux-gnu
    aarch64-unknown-linux-gnu-strip target/aarch64-unknown-linux-gnu/release/rise-thesis
    scp target/aarch64-unknown-linux-gnu/release/rise-thesis pi@192.168.0.234:workspace
    scp -r wasm-modules/ pi@192.168.0.234:workspace/ 




scp target/aarch64-unknown-linux-gnu/release/rise-thesis pi@192.168.0.234:workspace
aarch64-unknown-linux-gnu-strip target/aarch64-unknown-linux-gnu/release/rise-thesis

Size optimizations:
1) Add the following to cargo.toml 
    [profile.release]
    lto = true
    codegen-units = 1
    opt-level = "z"      # optimize for size
    strip = "debuginfo"  # strip debug info
2) strip target/aarch64-unknown-linux-gnu/release/rise-thesis


# How to compile on raspberry pi 5:

# Copy code into the raspberri
rm -rf target
scp -r . pi@192.168.8.110:/home/pi/rise-thesis

# Update system
sudo apt update && sudo apt upgrade -y

# Install basic development tools
sudo apt install -y build-essential pkg-config libssl-dev git curl

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# Install ONNX Runtime development package
# Download and install ONNX Runtime 1.22.0 fresh
# Remove the old system libraries
<!-- sudo rm /lib/aarch64-linux-gnu/libonnxruntime* -->

# Remove any old libraries from /usr/local/lib
<!-- sudo rm /usr/local/lib/libonnxruntime* -->

cd /tmp
wget https://github.com/microsoft/onnxruntime/releases/download/v1.22.0/onnxruntime-linux-aarch64-1.22.0.tgz
tar -xzf onnxruntime-linux-aarch64-1.22.0.tgz

# Install to /usr/local/lib
sudo cp -r onnxruntime-linux-aarch64-1.22.0/lib/* /usr/local/lib/

# Update library cache
sudo ldconfig

# Verify - you should now see version 1.22
ldconfig -p | grep onnxruntime


cargo build --release
target/release/rise-thesis