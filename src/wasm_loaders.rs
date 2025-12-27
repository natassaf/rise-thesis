use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use wasmtime::component::{Component, Func, Linker, Val};
use wasmtime::*;
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{self, IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi::{DirPerms, FilePerms};
use wasmtime_wasi_nn::Backend;
use wasmtime_wasi_nn::backend::onnx::OnnxBackend;
use wasmtime_wasi_nn::wit::add_to_linker as add_wasi_nn;
use wasmtime_wasi_nn::wit::{WasiNnCtx, WasiNnView};
use wasmtime_wasi_nn::{InMemoryRegistry, Registry};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct HostState {
    wasi: WasiCtx,
    table: wasmtime::component::ResourceTable,
    wasi_nn: WasiNnCtx,
}

impl HostState {
    fn wasi_nn_view(&mut self) -> WasiNnView<'_> {
        WasiNnView::new(&mut self.table, &mut self.wasi_nn)
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}
impl IoView for HostState {
    fn table(&mut self) -> &mut wasmtime::component::ResourceTable {
        &mut self.table
    }
}


/// Per-worker store and execution context
pub struct WasmComponentLoader {
    store: Store<HostState>,
    linker: Linker<HostState>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WasmResult {
    output: String,
}

/// Shared component cache - handles compilation and caching (thread-safe)
pub struct ComponentCache {
    engine: Engine,
    linker: Linker<HostState>,
    component_cache: Arc<Mutex<HashMap<String, Component>>>,
}


impl ComponentCache {
    pub fn new(_folder_to_mount: String) -> Self {
        println!("Initializing component cache");

        // initialize engine
        let mut config = Config::new();
        config.async_support(true).wasm_component_model(true);
        let engine = Engine::new(&config).unwrap();

        // initialize linker (shared across all workers)
        let mut linker: Linker<HostState> = Linker::new(&engine);
        p2::add_to_linker_async(&mut linker)
            .context("add_to_linker_async failed")
            .unwrap();
        // Add wasm-nn support
        add_wasi_nn(&mut linker, |host: &mut HostState| {
            HostState::wasi_nn_view(host)
        })
        .context("failed to add wasi-nn to linker")
        .unwrap();

        Self {
            engine,
            linker,
            component_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get or compile a component (thread-safe, uses Mutex for cache access)
    pub async fn get_component(&self, wasm_component_path: &str) -> Component {
        let mut component_cache = self.component_cache.lock().await;
        component_cache
            .entry(wasm_component_path.to_string())
            .or_insert_with(|| {
                Component::from_file(&self.engine, wasm_component_path)
                    .with_context(|| format!("failed to compile component at {:?}", wasm_component_path))
                    .unwrap()
            })
            .clone()
    }

    /// Get the linker (read-only, can be shared)
    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    /// Get the engine (read-only, can be shared)
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}

impl WasmComponentLoader {
    /// Create a new loader with its own Store (one per worker for parallelism)
    pub fn new(folder_to_mount: String, cache: &ComponentCache) -> Self {
        let wasi = match folder_to_mount.as_str() {
            "" => WasiCtxBuilder::new().inherit_stdio().build(),
            _ => {
                // Canonicalize the path to ensure it's absolute and exists
                let host_path = std::fs::canonicalize(&folder_to_mount).unwrap_or_else(|e| {
                    eprintln!(
                        "Warning: Failed to canonicalize path '{}': {}. Using as-is.",
                        folder_to_mount, e
                    );
                    std::path::PathBuf::from(&folder_to_mount)
                });

                // Use the same path for guest path so WASM code can access it with absolute paths
                let host_path_str = host_path.to_string_lossy().to_string();
                let guest_path = host_path_str.clone();

                println!(
                    "Preopening directory - host: '{}', guest: '{}'",
                    host_path_str, guest_path
                );

                // Verify the directory exists before preopening
                if !std::path::Path::new(&host_path_str).exists() {
                    eprintln!(
                        "ERROR: Directory '{}' does not exist! Cannot preopen.",
                        host_path_str
                    );
                }

                WasiCtxBuilder::new()
                    .inherit_stdio()
                    .preopened_dir(
                        host_path_str.clone(), // host path (canonicalized absolute path)
                        guest_path.clone(), // guest path (same as host so WASM can use absolute paths)
                        DirPerms::READ,
                        FilePerms::READ,
                    )
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "ERROR: Failed to preopen directory '{}': {}",
                            host_path_str, e
                        );
                        panic!("Failed to preopen directory: {}", e);
                    })
                    .build()
            }
        };

        // Initialize ONNX backend (each worker gets its own)
        let onnx_backend = Backend::from(OnnxBackend::default());
        let my_registry = InMemoryRegistry::new();
        let registry = Registry::from(my_registry);
        let wasi_nn = WasiNnCtx::new(vec![onnx_backend], registry);

        // Create a new Store for this worker (allows parallel execution)
        let store: Store<HostState> = Store::new(
            cache.linker().engine(),
            HostState {
                wasi,
                table: wasmtime::component::ResourceTable::new(),
                wasi_nn,
            },
        );

        // Clone the linker (it's read-only after setup)
        let linker = cache.linker().clone();

        Self {
            store,
            linker,
        }
    }

    /// Load a function from a component (uses shared cache for compilation, but instantiates in this worker's store)
    pub async fn load_func(
        &mut self,
        cache: &ComponentCache,
        wasm_component_path: String,
        func_name: String,
    ) -> Func {
        // Get component from shared cache (this is the only part that needs synchronization)
        let component = cache.get_component(&wasm_component_path).await;

        // Instantiate in this worker's own store (no lock needed - each worker has its own store)
        let instance = self
            .linker
            .instantiate_async(&mut self.store, &component)
            .await
            .context("instantiate_async failed")
            .unwrap();

        // Lookup exported function
        let func: Func = instance
            .get_func(&mut self.store, &func_name)
            .ok_or_else(|| anyhow!("exported function `{func_name}` not found"))
            .unwrap();

        func
    }

    pub async fn run_func(
        &mut self,
        input: Vec<Val>,
        func: Func,
    ) -> Result<Vec<Val>, anyhow::Error> {
        let results_len = func.results(&self.store).len();

        // Initialize with empty string for WasmResult output
        let mut results = vec![Val::String("".into()); results_len];

        let input_args = input;
        func.call_async(&mut self.store, &input_args, &mut results)
            .await?;
        // println!("load result {:?}", results);
        return Ok(results);
    }

    /// Clear the store by creating a new one, dropping all instances and freeing memory
    /// This should be called after each task completes to prevent memory leaks
    /// The folder_to_mount should be the same as used when creating the loader (e.g., "models")
    pub fn clear_store(&mut self, folder_to_mount: &str) {
        // Get the engine from the current store (needed to create new store)
        let engine = self.store.engine();
        
        // Recreate HostState (same as in new())
        let wasi = match folder_to_mount {
            "" => WasiCtxBuilder::new().inherit_stdio().build(),
            _ => {
                // Canonicalize the path to ensure it's absolute and exists
                let host_path = std::fs::canonicalize(folder_to_mount).unwrap_or_else(|e| {
                    eprintln!(
                        "Warning: Failed to canonicalize path '{}': {}. Using as-is.",
                        folder_to_mount, e
                    );
                    std::path::PathBuf::from(folder_to_mount)
                });

                let host_path_str = host_path.to_string_lossy().to_string();
                let guest_path = host_path_str.clone();

                WasiCtxBuilder::new()
                    .inherit_stdio()
                    .preopened_dir(
                        host_path_str.clone(),
                        guest_path.clone(),
                        DirPerms::READ,
                        FilePerms::READ,
                    )
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "ERROR: Failed to preopen directory '{}': {}",
                            host_path_str, e
                        );
                        panic!("Failed to preopen directory: {}", e);
                    })
                    .build()
            }
        };

        // Initialize ONNX backend (each worker gets its own)
        let onnx_backend = Backend::from(OnnxBackend::default());
        let my_registry = InMemoryRegistry::new();
        let registry = Registry::from(my_registry);
        let wasi_nn = WasiNnCtx::new(vec![onnx_backend], registry);

        // Create a new Store to replace the old one - this drops all instances
        let new_store: Store<HostState> = Store::new(
            &engine,
            HostState {
                wasi,
                table: wasmtime::component::ResourceTable::new(),
                wasi_nn,
            },
        );

        // Replace the old store with the new one
        // This will drop the old store and all its instances, freeing memory
        self.store = new_store;
    }

    
    pub fn clear_memory(&mut self, folder_to_mount: &str) {
        // Clear the store multiple times to ensure all memory is freed
        // Each clear creates a new store, dropping the old one
        for i in 0..5 {
            self.clear_store(folder_to_mount);
            // After each clear, allow OS to reclaim memory between clears
            if i < 4 {
                // Small delay to allow OS to reclaim memory
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}
