
// use wasmtime::*;
// use wasmtime::{Config, Engine, Module, Store, TypedFunc, WasmParams, WasmResults};
// use wasmtime_wasi::preview1::{self, WasiP1Ctx};
// use wasmtime_wasi::p2::WasiCtxBuilder;
// use async_trait::async_trait;


// wasmtime::component::bindgen!({
//     path: "wasm-modules/math_tasks.wit",
//     world: "mathtasks",
//     async: true,
// });



// #[async_trait]
// pub trait WasmLoaderI {
//     fn new(data: ()) -> Self;

//     async fn load< I, O>(
//         &mut self,
//         path_to_module: String,
//         func_name: String,
//     ) -> (TypedFunc<I, O>, Memory)
//     where
//         I: WasmParams + Send ,
//         O: WasmResults + Send;
// }

// pub struct ModuleWasmLoader{
//     engine:Engine,
//     pub store: Store<WasiP1Ctx>,
//     pub linker: Linker<WasiP1Ctx>,
// }

// #[async_trait]
// impl WasmLoaderI for  ModuleWasmLoader{

//     fn new(data:())->Self{
//         println!("Loading wasm module");
//         let args = std::env::args().skip(1).collect::<Vec<_>>();
//         let mut config = Config::new();
//         config.async_support(true);
//         let engine = Engine::new(&config).unwrap();
//         let mut linker: Linker<WasiP1Ctx> = Linker::new(&engine);
//         preview1::add_to_linker_async(&mut linker, |t| t);

//         let wasi_ctx = WasiCtxBuilder::new()
//             .inherit_stdio()
//             .inherit_env()
//             .args(&args)
//             .build_p1();

//         let  store = Store::new(&engine, wasi_ctx);
//         Self {engine, store, linker}
//     }

//     async fn load<I:WasmParams + Send, O:WasmResults + Send>(&mut self, path_to_module:String, func_name:String)->(TypedFunc<I, O>, Memory )
//         {

//         let module = Module::from_file(&self.engine, path_to_module).unwrap();
//         let pre = self.linker.instantiate_pre(&module).unwrap();
//         let instance = pre.instantiate_async(&mut self.store).await.unwrap();
//         // let instance = self.linker.instantiate_async(&mut self.store, &module).await.unwrap();

//         // memory stuff
//         let memory = instance.get_memory(&mut self.store, "memory").expect("No `memory` export found in Wasm module");
//         let loaded_func = instance.get_typed_func::<I, O>(&mut self.store, &func_name).unwrap();
//         (loaded_func, memory)
//     }
// }





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
