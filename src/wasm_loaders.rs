
use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc, WasmParams, WasmResults};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime_wasi::p2::{add_to_linker_async, WasiCtx, WasiCtxBuilder, WasiView, IoView};


wasmtime::component::bindgen!({
    path: "wasm-modules/math_tasks.wit",
    world: "mathtasks",
    async: true,
});

pub struct ModuleWasmLoader{
    engine:Engine,
    pub store: Store<()>
}

impl ModuleWasmLoader{

    pub fn new(data:())->Self{
        println!("Loading wasm module");
        let engine = Engine::default();
        let mut store: Store<()> = Store::new(&engine, data);
        Self {engine, store}
    }

    pub fn load<I:WasmParams, O:WasmResults>(&mut self, module_name:&str, path_to_module:String, func_name:String)-> TypedFunc<I, O> {
        let module = Module::from_file(&self.engine, path_to_module).unwrap();
        let instance = Instance::new(&mut self.store, &module, &[]).unwrap();
        let loaded_func: TypedFunc<I, O> = instance.get_typed_func(&mut self.store, &func_name).unwrap();
        loaded_func
    }

}


// pub struct ComponentWasmLoader<T>{
//     engine:Engine,
//     pub(crate) store: Store<T>
// }

// impl<T> ComponentWasmLoader<T>{

//     pub fn new(data:T)->Self{
//         println!("Loading wasm component loader");
//         let engine = Engine::default();
//         let mut store: Store<T> = Store::new(&engine, data);
//         Self {engine, store}
//     }

//     pub async fn load(&mut self, component_name:&str, config:Config)->Mathtasks{
//         println!("Running: {}", component_name);
//         let engine = Engine::new(&config).unwrap();
//         // let component = Component::from_file(&engine, "wasm-modules/target/wasm32-wasip2/release/multi_wasm.wasm").unwrap();
//         let component_path = "wasm-modules/".to_owned()+ component_name+".wasm";
//         let component = Component::from_file(&engine, component_path).unwrap();
//         let mut linker = Linker::<MyState>::new(&engine);
//         add_to_linker_async(&mut linker).unwrap();
//         let wasi_ctx = WasiCtxBuilder::new().inherit_stdio().build();
//         let mut store = Store::new(
//             &engine,
//             MyState {
//                 ctx: wasi_ctx,
//                 table: ResourceTable::new(),
//             },
//         );
//         let bindings = Mathtasks::instantiate_async(&mut store, &component, &linker).await.unwrap();
//         bindings
//     }
// }


// struct MyState {
//     ctx: WasiCtx,
//     table: ResourceTable,
// }

// impl WasiView for MyState {
//     fn ctx(&mut self) -> &mut WasiCtx {
//         &mut self.ctx
//     }
// }

// impl IoView for MyState {
//     fn table(&mut self) -> &mut ResourceTable {
//         &mut self.table
//     }
// }
