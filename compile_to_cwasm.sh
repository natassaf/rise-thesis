#!/bin/bash

# Script to compile all .wasm files to .cwasm format using wasmtime
# Usage: ./compile_to_cwasm.sh

WASM_DIR="wasm-modules"
SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

echo "Starting compilation of .wasm files to .cwasm format..."
echo "==========================================================="
echo ""

# Check if wasmtime is installed
if ! command -v wasmtime &> /dev/null; then
    echo "Error: wasmtime is not installed or not in PATH"
    echo "Please install wasmtime first: https://wasmtime.dev/"
    exit 1
fi

# Find all .wasm files in wasm-modules directory (excluding subdirectories)
for wasm_file in "$WASM_DIR"/*.wasm; do
    # Check if any .wasm files exist
    if [ ! -f "$wasm_file" ]; then
        echo "No .wasm files found in $WASM_DIR/"
        exit 0
    fi
    
    # Get filename without extension
    filename=$(basename "$wasm_file" .wasm)
    cwasm_file="$WASM_DIR/$filename.cwasm"
    
    # Optional: Skip if .cwasm already exists (comment out to always recompile)
    # if [ -f "$cwasm_file" ]; then
    #     echo "⏭️  Skipping $filename.wasm (already compiled)"
    #     SKIP_COUNT=$((SKIP_COUNT + 1))
    #     continue
    # fi
    
    echo "Compiling: $filename.wasm..."
    
    # Compile .wasm to .cwasm
    if wasmtime compile "$wasm_file" -o "$cwasm_file"; then
        echo "✅ Success: $filename.cwasm created"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "❌ Failed: $filename.wasm compilation failed"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    echo ""
done

echo "==========================================================="
echo "Compilation complete!"
echo "✅ Successful: $SUCCESS_COUNT"
echo "❌ Failed: $FAIL_COUNT"
# echo "⏭️  Skipped: $SKIP_COUNT"
echo "==========================================================="

exit 0

