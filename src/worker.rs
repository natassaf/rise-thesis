    use std::{collections::VecDeque, fmt::Display, sync::Arc};
    use std::fmt::Debug;
    use core_affinity::{get_core_ids, CoreId};
    use serde_json::Value;
    use tokio::{sync::Mutex};
    use tokio::{self, task};
    use wasmtime::{TypedFunc, WasmParams, WasmResults};
    use crate::{all_tasks::fibonacci, various::{WasmJob}, wasm_loaders::{ ModuleWasmLoader}};

    // Worker is mapped to a core id and runs the tasks located in each queue
    pub struct Worker{
        worker_id:usize,
        core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<WasmJob>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
            let core_id = core_ids[6];
            Worker{worker_id, core_id, thread_queue}
        }

        pub async fn add_to_queue(&self, jobs:Vec<WasmJob>){
            let mut queue = self.thread_queue.lock().await;
            for j in jobs.iter(){
                queue.push_back(j.clone());
            }
        }

        pub fn store_result<T:std::fmt::Display>(task_id:usize, result:&T){
            // Store result in a file named after the task
            let file_name = format!("results/result_{}.txt", task_id);
            std::fs::write(&file_name, format!("Result: {}", result)).expect("Failed to write result to file");
        }

        // pub async fn run_job(core_id: CoreId, task_id: usize, task_n: usize)->task::JoinHandle<Value>{
        //     task::spawn_blocking(move || {     
        //                     core_affinity::set_for_current(core_id);
        //                     println!("Running task {} on core {:?}", task_id, core_id);
        //                     let result:Value = fibonacci(task_n);
        //                     Self::store_result(task_id, &result);
        //                     println!("Finished task {}", task_id);
        //                     result
        //                 })
        // }

        pub async fn run_wasm_job_module<I:Clone + Debug + Display + Send + Sync + 'static + WasmParams + WasmResults, O:Clone + Debug + Display + Send + Sync + 'static + WasmParams + WasmResults>(core_id: CoreId, task_id: usize, task_input: I, path_to_module:String, func_name:String){
            // Set up Wasmtime engine and module outside blocking
            let mut wasm_loader = ModuleWasmLoader::new(());
            let func_to_run:TypedFunc<I, O> = wasm_loader.load("fib", path_to_module, func_name);
            let handle = task::spawn_blocking(move || {
                println!{"Task {task_id} running on core {:?}", core_id.clone()};
                core_affinity::set_for_current(core_id);
                let result:O = func_to_run.call(&mut wasm_loader.store, task_input).unwrap();
                Self::store_result(task_id, &result);
                println!("Finished wasm task {}", task_id);
                result
            });
            let _result = match handle.await {
                Ok(result) => Some(result),
                Err(e) => None,
            };
            ()
        }

        // pub async fn run_wasm_job_component(core_id: CoreId, task_id: usize, task_n: usize, path_to_module:Option<String>)->task::JoinHandle<u64>{
        //     // Set up Wasmtime engine and module outside blocking
        //     let component_name ="math_tasks";
        //     let mut wasm_loader = ComponentWasmLoader::new(());
        //     let func_to_run = wasm_loader.load(component_name).await;
        //     task::spawn_blocking(move || {
        //         println!{"Task {task_id} running on core {:?}", core_id.clone()};
        //         core_affinity::set_for_current(core_id);
        //         let input = 64 as u64;
        //         let bindings = wasm_loader.load(component_name);
        //         let result: Result<u32, anyhow::Error> = func_to_run.call_is_prime(&mut wasm_loader.store, input).await.unwrap();
        //         Self::store_result(task_id, result);
        //         println!("Finished wasm task {}", task_id);
        //         result
        //     })
        // }

        pub async fn  run_job<I:Clone + Debug + Display + Send + Sync + 'static + WasmParams + WasmResults, O:Clone + Debug + Display + Send + Sync + 'static + WasmParams + WasmResults>(&self, task_id:usize, binary_path: String, func_name:String, task_input:I ){
            let core_id = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
            let task_module_path = binary_path;
            // Spawn a blocking task to map the worker to the core 
            Self::run_wasm_job_module::<I, O>(core_id, task_id, task_input ,task_module_path, func_name).await;
            // tokio::time::sleep(std::time::Duration::from_secs(20)).await; // for testing
            ()
 
        }
        pub async fn start(&self){
            println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
            loop{
                tokio::time::sleep(std::time::Duration::from_secs(5)).await; // for testing

                // Retrieve the next task from the queue runs it buy awaiting and returns result
                let mut queue = self.thread_queue.lock().await; // lock the mutex
                println!("Worker: {:?} queue: {:?} ", self.worker_id, queue);
                let my_task:Option<WasmJob> = queue.pop_front();
                match my_task{
                    None=> (),
                    Some(my_task_1)=>{
                        let task_id = my_task_1.job_input.id.clone(); // for testing
                        let task_input: Value = my_task_1.job_input.input.clone();
                        let func_name = my_task_1.func_name.clone();
                        match func_name.as_str() {
                            "is_prime" => self.run_job::<u64, u32>(task_id, my_task_1.binary_path, func_name, task_input.as_u64().unwrap()).await,
                            "fib"=>self.run_job::<u64, u64>(task_id, my_task_1.binary_path, func_name, task_input.as_u64().unwrap()).await,
                            _ => println!("Unknown function: {}", func_name)
                        }
                        ()
                        }
                    }
                }
            }
    }






