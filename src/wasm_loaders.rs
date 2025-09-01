
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use wasmtime::*;
use wasmtime_wasi::preview1::{WasiP1Ctx};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{self, IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_nn::wit::{add_to_linker as add_wasi_nn};
use wasmtime_wasi_nn::wit::{ WasiNnCtx, WasiNnView};
use wasmtime::component::{Component, Func, Linker, Val};
use wasmtime_wasi_nn::{InMemoryRegistry, Registry};
use wasmtime_wasi_nn::Backend;
use wasmtime_wasi::{DirPerms, FilePerms};
use wasmtime_wasi_nn::backend::onnx::OnnxBackend;
// pub struct ModuleWasmLoader{
//     engine:Engine,
//     //  pub store: Store<WasiP1Ctx>,
//     pub store: Store<()>,
//     pub linker: Option<Linker<WasiP1Ctx>>,
// }

// impl ModuleWasmLoader{

//     // pub fn new_async(data:())->Self{
//     //     println!("Loading wasm module");
//     //     let args = std::env::args().skip(1).collect::<Vec<_>>();
//     //     let mut config = Config::new();
//     //     config.async_support(true);
//     //     let engine = Engine::new(&config).unwrap();
//     //     let mut linker: Linker<WasiP1Ctx> = Linker::new(&engine);
//     //     preview1::add_to_linker_async(&mut linker, |t| t);

//     //     let wasi_ctx = WasiCtxBuilder::new()
//     //         .inherit_stdio()
//     //         .inherit_env()
//     //         .args(&args)
//     //         .build_p1();

//     //     let  store = Store::new(&engine, wasi_ctx);
//     //     Self {engine, store, linker}
//     // }

//     pub fn new(data:())->Self{
//         println!("Loading wasm module");
//         let engine = Engine::default();
//         let mut store = Store::new(&engine, ());
//         Self {engine, store, linker:None}
//     }

    
//     pub fn load<I:WasmParams, O:WasmResults>(&mut self, path_to_module:String, func_name:String)->(TypedFunc<I, O>, Memory ){
//         let module = Module::from_file(&self.engine, path_to_module).unwrap();
//         let instance = Instance::new(&mut self.store, &module, &[]).unwrap();
//         // let module = Module::from_file(&self.engine, path_to_module).unwrap();
//         // let pre = self.linker.instantiate_pre(&module).unwrap();
//         // let instance = pre.instantiate(&mut self.store).unwrap();
//         // let instance = self.linker.instantiate_async(&mut self.store, &module).await.unwrap();

//         // memory stuff
//         let memory = instance.get_memory(&mut self.store, "memory").expect("No `memory` export found in Wasm module");
//         let loaded_func = instance.get_typed_func::<I, O>(&mut self.store, &func_name).unwrap();
//         (loaded_func, memory)
//     }
// }
struct HostState {
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


pub struct WasmComponentLoader{
    engine:Engine,
    //  pub store: Store<WasiP1Ctx>,
    pub store: Store<HostState>,
    pub linker: Linker<HostState>,
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
        // Add wasm-nn support
        add_wasi_nn(&mut linker, |host: &mut HostState| {
            // The linker will call into this to get a `WasiNnView<'_>`
            HostState::wasi_nn_view(host)
        }).context("failed to add wasi-nn to linker").unwrap();
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



        // Initialize ONNX backend
        let onnx_backend = Backend::from(OnnxBackend::default());
        
        
        let my_registry = InMemoryRegistry::new();
        let registry = Registry::from(my_registry);
        
        // Create the WasiNnCtx with the ONNX backend
        let wasi_nn = WasiNnCtx::new(vec![onnx_backend], registry);

        let store: Store<HostState> = Store::new(
            &engine,
            HostState {
                wasi,
                table: wasmtime::component::ResourceTable::new(),
                wasi_nn,
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

        // 6) Prepare results buffer with the right length; types/initial values are ignored by Wasmtime.


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