

use wasmtime::*;
use wasmtime::{Engine, Module, Store, TypedFunc, WasmParams, WasmResults};
use wasmtime_wasi::preview1::{WasiP1Ctx};

wasmtime::component::bindgen!({
    path: "wasm-modules/math_tasks.wit",
    world: "mathtasks",
    async: true,
});

pub struct ModuleWasmLoader{
    engine:Engine,
    //  pub store: Store<WasiP1Ctx>,
    pub store: Store<()>,
    pub linker: Option<Linker<WasiP1Ctx>>,
}

impl ModuleWasmLoader{

    // pub fn new_async(data:())->Self{
    //     println!("Loading wasm module");
    //     let args = std::env::args().skip(1).collect::<Vec<_>>();
    //     let mut config = Config::new();
    //     config.async_support(true);
    //     let engine = Engine::new(&config).unwrap();
    //     let mut linker: Linker<WasiP1Ctx> = Linker::new(&engine);
    //     preview1::add_to_linker_async(&mut linker, |t| t);

    //     let wasi_ctx = WasiCtxBuilder::new()
    //         .inherit_stdio()
    //         .inherit_env()
    //         .args(&args)
    //         .build_p1();

    //     let  store = Store::new(&engine, wasi_ctx);
    //     Self {engine, store, linker}
    // }

    pub fn new(data:())->Self{
        println!("Loading wasm module");
        let engine = Engine::default();
        let mut store = Store::new(&engine, ());
        Self {engine, store, linker:None}
    }

    
    pub fn load<I:WasmParams, O:WasmResults>(&mut self, path_to_module:String, func_name:String)->(TypedFunc<I, O>, Memory ){
        let module = Module::from_file(&self.engine, path_to_module).unwrap();
        let instance = Instance::new(&mut self.store, &module, &[]).unwrap();
        // let module = Module::from_file(&self.engine, path_to_module).unwrap();
        // let pre = self.linker.instantiate_pre(&module).unwrap();
        // let instance = pre.instantiate(&mut self.store).unwrap();
        // let instance = self.linker.instantiate_async(&mut self.store, &module).await.unwrap();

        // memory stuff
        let memory = instance.get_memory(&mut self.store, "memory").expect("No `memory` export found in Wasm module");
        let loaded_func = instance.get_typed_func::<I, O>(&mut self.store, &func_name).unwrap();
        (loaded_func, memory)
    }
}

