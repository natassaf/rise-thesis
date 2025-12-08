# Rise Thesis - Scheduler

## Overview

This project is a scheduler system that manages and prioritizes WebAssembly (WASM) tasks based on memory and execution time predictions.

## Required Repositories

To execute this project, you need the following repositories:

1. **rise-thesis** - This repository (the scheduler)
2. **rise-scheduler-experiments** - Creates requests for this scheduler

## Additional Repositories Used

Three more repositories were used to create this project:

3. **rise-thesis-wasm-tasks** - Repository where all WASM components were created and uploaded. They all have a common input and output format.
4. **rise-scheduler-models** - Creation and training of models that predict memory and execution time requirements of given tasks.
5. **wasm-memory-calculation** - Used to gather data to train the memory-prediction and execution time models. In this repo, for each request, memory and time are measured and stored by running each task sequentially on a separate process initialized for each request and getting the peak memory used by this process.

## Running the App

### To run this repo  on  Raspberry Pi 5 with Ubuntu

Cross compilation didn't work on macOS due to the `ort` dependency that fails. Until this dependency issue is solved, the way to run it is:

1. Find the IP of your Raspberry Pi and connect via SSH:
   ```bash
   ssh pi@<ip>
   # Example: ssh pi@192.168.8.110
   ```

2. Install dependencies:
   ```bash
   sudo apt update && sudo apt upgrade -y
   
   # Install basic development tools
   sudo apt install -y build-essential pkg-config libssl-dev git curl
   
   # Install Rust
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
   
   # Install ONNX Runtime development package
   sudo apt install libonnxruntime-dev
   
   # Verify it's installed
   dpkg -L libonnxruntime-dev | grep "\.so"
   # Should show: /usr/lib/aarch64-linux-gnu/libonnxruntime.so
   
   # Set the environment variable to point to the correct directory
   export ORT_LIB_LOCATION=/usr/lib/aarch64-linux-gnu
   export ORT_STRATEGY=system
   
   # Also add it to the library path
   export LD_LIBRARY_PATH=/usr/lib/aarch64-linux-gnu:$LD_LIBRARY_PATH
   ```

3. Copy the project from host to Pi:
   ```bash
   # On Pi: Create folder
   mkdir rise-thesis
   
   # On Host: Copy project
   cargo clean 
   
   scp -r . pi@192.168.8.110:/home/pi/rise-thesis
   ```

4. Build the project:
   ```bash
   cargo build --release
   ```

5. Run the application :
   sudo systemd-run --scope -p MemoryMax=5M ./target/release/rise-thesis
   ```bash
   ./target/release/rise-thesis
   ```
6. To run on memory constraint mode:
   Enable the Linux cgroup memory controller
   ```bash
   sudo nano /boot/firmware/cmdline.txt
   ```
   
   Add these parameters to the single existing line:  
   cgroup_enable=memory cgroup_memory=1 systemd.unified_cgroup_hierarchy=1
   ```bash
   sudo reboot
   ```
   Run the app:
   ```bash
   sudo systemd-run --scope -p MemoryMax=5M ./target/release/rise-thesis
   ```

### On  the repo rise-scheduler-experiments

1. Create Python virtual environment and install requirements:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Activate environment (if not already activated)

3. Navigate to src directory:
   ```bash
   cd src
   ```

4. Run the main script:
   ```bash
   python main.py
   ```


