    use std::{collections::{VecDeque}, sync::Arc};

    use anyhow::Error;
    use core_affinity::CoreId;
    use tokio::{sync::Mutex};
    use wasmtime::{Engine, Module, Store, Instance, TypedFunc};
    use tokio::{self, task};
    use crate::{all_tasks::fibonacci, various::{WasmJob}};

    
    // Worker is mapped to a core id and runs the tasks located in each queue
    pub struct Worker{
        worker_id:usize,
        core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<WasmJob>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            // let core = core_ids[i % num_cores];
            Worker{worker_id, core_id, thread_queue}
        }

        pub async fn add_to_queue(&self, jobs:Vec<WasmJob>){
            let mut queue = self.thread_queue.lock().await;
            for j in jobs.iter(){
                queue.push_back(j.clone());
            }
        }

        pub fn store_result(task_id:usize, result:u64){
            // Store result in a file named after the task
            let file_name = format!("results/result_{}.txt", task_id);
            std::fs::write(&file_name, format!("Result: {}", result)).expect("Failed to write result to file");
        }

        pub async fn run_job(core_id: CoreId, task_id: usize, task_n: usize)->task::JoinHandle<u64>{
            task::spawn_blocking(move || {     
                            core_affinity::set_for_current(core_id);
                            println!("Running task {} on core {:?}", task_id, core_id);
                            let result = fibonacci(task_n);
                            Self::store_result(task_id, result);
                            println!("Finished task {}", task_id);
                            result
                        })
        }

        pub async fn run_wasm_job(core_id: CoreId, task_id: usize, task_n: usize)->task::JoinHandle<u64>{
            // Set up Wasmtime engine and module outside blocking
            let engine = Engine::default();
            
            let path_to_module: &'static str = "wasm-modules/target/wasm32-unknown-unknown/release/fib.wasm";
            let module = Module::from_file(&engine, path_to_module).unwrap();

            // For each input spawn a blocking task that instantiates and runs wasm fib
            let mut store = Store::new(&engine, ());
            let instance = Instance::new(&mut store, &module, &[]).unwrap();
            let fib: TypedFunc<u64, u64> = instance.get_typed_func(&mut store, "fib").unwrap();
            task::spawn_blocking(move || {
                let result = fib.call(&mut store, task_n.try_into().unwrap()).unwrap();
                Self::store_result(task_id, result);
                println!("Finished wasm task {}", task_id);
                result
            })

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
                        let task_n: usize = my_task_1.job_input.n.clone();
                        let core_id = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
                        
                        // Spawn a blocking task to map the worker to the core 
                        let handle: task::JoinHandle<u64> = Self::run_wasm_job(core_id, task_id, task_n.try_into().unwrap()).await;
                        let _result = match handle.await {
                            Ok(result) => Some(result),
                            Err(e) => None,
                        };
                        ()
                        }
                    }
                }
            }
    }
