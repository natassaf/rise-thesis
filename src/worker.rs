    use std::{collections::VecDeque, sync::Arc};
    use std::io::{self};
    use core_affinity::{CoreId};
    use tokio::{sync::Mutex};
    use tokio::{self, task};
    use crate::various::store_encoded_result;
    use crate::{various::{WasmJob}, wasm_loaders::{ ModuleWasmLoader}};



    // Worker is mapped to a core id and runs the tasks located in each queue
    pub struct Worker{
        worker_id:usize,
        pub core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<WasmJob>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            // let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
            // let core_id = core_ids[2];
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

        pub async fn run_wasm_job_module(core_id: CoreId, task_id: usize, task_input_bytes: Vec<u8>, path_to_module:String, func_name:String){
            // Set up Wasmtime engine and module outside blocking
            // let encoding_config = config::standard();
            // let (original_input, _): ((Vec<Vec<f32>>, Vec<Vec<f32>>), _) = decode_from_slice(&task_input_bytes, encoding_config).expect("Failed to decode mat1");
            let handle = task::spawn_blocking(move || {
                let mut wasm_loader = ModuleWasmLoader::new(());
                let (loaded_func, memory) = wasm_loader.load::<(u32, u32, u32), ()>(path_to_module, func_name);
                println!{"Task {task_id} running on core {:?}", core_id.clone()};
                
                core_affinity::set_for_current(core_id);
                let input_ptr = 0;
                let out_ptr = task_input_bytes.len();
                let output_len = task_input_bytes.len(); 
                memory.write(&mut wasm_loader.store, input_ptr as usize, &task_input_bytes).unwrap();
                memory.write(&mut wasm_loader.store, out_ptr as usize, &vec![0u8; output_len]).unwrap();

                // Call Wasm function
                loaded_func.call(
                    &mut wasm_loader.store,
                    (
                        input_ptr as u32,
                        task_input_bytes.len() as u32,
                        out_ptr as u32
                    ),
                ).unwrap();

                // Read result
                let mut result_bytes = vec![0u8; output_len];
                memory.read(&mut wasm_loader.store, out_ptr as usize, &mut result_bytes).unwrap();
                let save_file_name = format!("results/result_{}.txt", task_id);
                let _res: std::result::Result<(), io::Error> = store_encoded_result(&result_bytes.clone(), &save_file_name);
                // let (result, _bytes_read): (u64, _) = decode_from_slice(&result_bytes, encoding_config).unwrap();
                // println!("result: {:?}", result);
                // println!("Finished wasm task {:?}", task_id);
                result_bytes
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

        pub async fn  run_job(&self, task_id:usize, binary_path: String, func_name:String, task_input:Vec<u8> ){
            let core_id: CoreId = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
            let task_module_path = binary_path;
            // Spawn a blocking task to map the worker to the core 
            Self::run_wasm_job_module(core_id, task_id, task_input ,task_module_path, func_name).await;
            // tokio::time::sleep(std::time::Duration::from_secs(20)).await; // for testing
            ()
 
        }
        pub async fn start(&self){
            println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
            loop{
                tokio::time::sleep(std::time::Duration::from_secs(5)).await; // for testing

                // Retrieve the next task from the queue runs it buy awaiting and returns result
                let mut queue = self.thread_queue.lock().await; // lock the mutex
                // println!("Worker: {:?} queue: {:?} ", self.worker_id, queue);
                let my_task:Option<WasmJob> = queue.pop_front();
                match my_task{
                    None=> (),
                    Some(my_task_1)=>{
                        let task_id = my_task_1.job_input.id.clone(); // for testing
                        let task_input: Vec<u8> = my_task_1.job_input.input.clone();
                        let func_name = my_task_1.job_input.func_name.clone();
                        let _res = self.run_job(task_id, my_task_1.binary_path, func_name, task_input).await;
                        ()
                        }
                    }
                }
            }
    }






