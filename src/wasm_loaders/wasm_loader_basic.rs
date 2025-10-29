use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use wasmtime::*;
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{self, IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime::component::{Component, Func, Linker, Val};
use wasmtime_wasi::{DirPerms, FilePerms};

struct HostState {
    wasi: WasiCtx,
    table: wasmtime::component::ResourceTable,
}

impl HostState {
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


pub struct WasmComponentLoader{
    engine:Engine,
    //  pub store: Store<WasiP1Ctx>,
    store: Store<HostState>,
    linker: Linker<HostState>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WasmResult {
    output: String,
}


impl WasmComponentLoader{
    pub fn new(folder_to_mount:String)->Self{
        println!("Loading wasm component");

        // initialize engine
        let mut config = Config::new();
        config.async_support(true).wasm_component_model(true);
        let engine = Engine::new(&config).unwrap();

        // initialize linker
        let mut linker: Linker<HostState> = Linker::new(&engine);
        p2::add_to_linker_async(&mut linker).context("add_to_linker_async failed").unwrap();
       let wasi = match folder_to_mount.as_str() {
            "" => {WasiCtxBuilder::new()
            .inherit_stdio()
            .build()}
            _ => {WasiCtxBuilder::new()
            .inherit_stdio()
            .preopened_dir(
                folder_to_mount.clone(),   // host path
                folder_to_mount.clone(),  // guest path
                DirPerms::READ,
                FilePerms::READ,
            )
            .unwrap()
            .build()
            }};

        let store: Store<HostState> = Store::new(
            &engine,
            HostState {
                wasi,
                table: wasmtime::component::ResourceTable::new(),
            },
        );

        Self {engine, store, linker}
    }

    pub async fn load_func(&mut self, wasm_component_path:String, func_name:String)->Func{
        let component = Component::from_file(&self.engine, wasm_component_path.clone())
        .with_context(|| format!("failed to compile component at {:?}", wasm_component_path)).unwrap();

        // 4) Instantiate
        let instance = self.linker.instantiate_async(&mut self.store, &component)
        .await
        .context("instantiate_async failed").unwrap();

        // 5) Lookup exported function by its world export name (usually the same as in the WIT).
        let func: Func = instance
            .get_func(&mut self.store, &func_name)
            .ok_or_else(|| anyhow!("exported function `{func_name}` not found")).unwrap();

        return func;
    }

    pub async fn run_func(&mut self, input:Vec<Val>, func:Func)->Result<Vec<Val>, anyhow::Error>{
        let results_len = func.results(&self.store).len();
        
        // Initialize with empty string for WasmResult output
        let mut results = vec![Val::String("".into()); results_len];

        let input_args = input;
        func.call_async(&mut self.store, &input_args, &mut results).await?;
        // println!("load result {:?}", results);
        return Ok(results)
    }
}