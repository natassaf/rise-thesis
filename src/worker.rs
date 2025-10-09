use std::usize;
use std::{collections::VecDeque, sync::Arc};
use core_affinity::{CoreId};
use serde_json::json;
use tokio::{sync::Mutex, task};
use crate::various::Job;
use crate::{various::{WasmJobRequest}, wasm_loaders::WasmComponentLoader};
use wasmtime::component::Val;
use flate2::write::GzEncoder;
use flate2::Compression;


// Worker is mapped to a core id and runs the tasks located in each queue
pub struct Worker{
    worker_id:usize,
    pub core_id:CoreId,
    thread_queue: Arc<Mutex<VecDeque<Job>>>,
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>
}

fn input_to_wasm_event_val(input:String) -> wasmtime::component::Val {
    let event_val = wasmtime::component::Val::String(input.into());
    let record_fields = vec![
        ("event".to_string(), event_val)
    ];
    wasmtime::component::Val::Record(record_fields.into())
}

fn create_wasm_event_val_for_matrix() -> wasmtime::component::Val {
    // Define the input matrices as Rust vectors.
    let mat1 = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
    let mat2 = vec![vec![5.0, 6.0], vec![7.0, 8.0]];

    // Construct the JSON string to pass as the input event.
    let input_json = json!({
        "mat1": mat1,
        "mat2": mat2
    }).to_string();

    // Convert the Rust String into the Wasmtime Val::String type.
    let event_val = wasmtime::component::Val::String(input_json.into());

    // Create a vector of tuples, where each tuple contains the field name
    // and its corresponding Val.
    let record_fields = vec![
        ("event".to_string(), event_val)
    ];

    // Return the final Val::Record.
    wasmtime::component::Val::Record(record_fields.into())
}

impl Worker{
    pub fn new(worker_id:usize, core_id:CoreId, wasm_loader: Arc<Mutex<WasmComponentLoader>>)->Self{
        let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
        let shutdown_flag = Arc::new(Mutex::new(false));
        // let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
        // let core_id = core_ids[2];
        Worker{worker_id, core_id, thread_queue, shutdown_flag, wasm_loader}
    }

    pub async fn add_to_queue(&self, jobs:Vec<Job>){
        let mut queue = self.thread_queue.lock().await;
        for j in jobs.iter(){
            queue.push_back(j.clone());
        }
    }

    pub async fn shutdown(&self) {
        let mut flag = self.shutdown_flag.lock().await;
        *flag = true;
    }

    pub fn store_result_uncompressed<T:std::fmt::Display>(task_id:usize, result:&T){
        // Store compressed result in a file named after the task
        let file_name = format!("results/result_{}.txt", task_id);
        
        // Format the result
        let result_string = format!("Result: {}", result);
        
        
        // Write the compressed data to file
        std::fs::write(&file_name, &result_string).expect("Failed to write compressed result to file");
        
        println!("Stored result for task {}: {} ", 
                 task_id, result_string.len());
    }


    pub fn store_result<T:std::fmt::Display>(task_id:usize, result:&T){
        // Store compressed result in a file named after the task
        let file_name = format!("results/result_{}.gz", task_id);
        
        // Format the result
        let result_string = format!("Result: {}", result);
        
        // Compress the result using gzip
        let mut compressed_data = Vec::new();
        {
            let mut encoder = GzEncoder::new(&mut compressed_data, Compression::default());
            std::io::Write::write_all(&mut encoder, result_string.as_bytes())
                .expect("Failed to compress result");
            encoder.finish().expect("Failed to finish compression");
        }
        
        // Write the compressed data to file
        std::fs::write(&file_name, &compressed_data).expect("Failed to write compressed result to file");
        
        println!("Stored compressed result for task {}: {} bytes -> {} bytes", 
                 task_id, result_string.len(), compressed_data.len());
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

    // pub async fn run_wasm_job_module(core_id: CoreId, task_id: usize, task_input_bytes: Vec<u8>, path_to_module:String, func_name:String){
    //     // Set up Wasmtime engine and module outside blocking
    //     // let encoding_config = config::standard();
    //     // let (original_input, _): ((Vec<Vec<f32>>, Vec<Vec<f32>>), _) = decode_from_slice(&task_input_bytes, encoding_config).expect("Failed to decode mat1");
    //     let handle = task::spawn_blocking(move || {
    //         core_affinity::set_for_current(core_id);
    //         let mut wasm_loader = ModuleWasmLoader::new();
    //         let (loaded_func, memory) = wasm_loader.load::<(u32, u32, u32, u32), ()>(path_to_module, func_name);
    //         println!{"Task {task_id} running on core {:?}", core_id.clone()};
            
    //         let input_ptr = 0;
    //         let out_ptr = task_input_bytes.len();
    //         let output_len_ptr = out_ptr + 1024; // reserve some extra space for output_len
    //         memory.write(&mut wasm_loader.store, input_ptr as usize, &task_input_bytes).unwrap();
    //         memory.write(&mut wasm_loader.store, output_len_ptr as usize, &[0u8; 4]).unwrap();

    //         // Call Wasm function
    //         loaded_func.call(
    //             &mut wasm_loader.store,
    //             (
    //                 input_ptr as u32,
    //                 task_input_bytes.len() as u32,
    //                 out_ptr as u32,
    //                 output_len_ptr as u32
    //             ),
    //         ).unwrap();
    //         // Read result
    //         let mut output_len_buf = [0u8; 4];
    //         memory.read(&mut wasm_loader.store, output_len_ptr as usize, &mut output_len_buf).unwrap();
    //         let output_len = u32::from_le_bytes(output_len_buf);
    //         let mut result_bytes = vec![0u8; output_len as usize];
    //         memory.read(&mut wasm_loader.store, out_ptr as usize, &mut result_bytes).unwrap();
    //         let save_file_name = format!("results/result_{}.txt", task_id);
    //         let _res: std::result::Result<(), io::Error> = store_encoded_result(&result_bytes.clone(), &save_file_name);
            
    //         // let encoding_config = bincode::config::standard();
    //         // let (result, _bytes_read): (Vec<Vec<f32>>, _) = decode_from_slice(&result_bytes, encoding_config).unwrap();
    //         // println!("result: {:?}", result);
    //         println!("Finished wasm task {:?}", task_id);
    //         result_bytes
    //     });
    //     let _result = match handle.await {
    //         Ok(result) => Some(result),
    //         Err(e) => None,
    //     };
    //     ()
    // }
    
    pub async fn run_wasm_job_component(core_id: CoreId, task_id: usize, component_name:String, func_name:String, payload:String, folder_to_mount:String, shared_wasm_loader: Arc<Mutex<WasmComponentLoader>>)->task::JoinHandle<Result<Vec<Val>, anyhow::Error>>{
        // Set up Wasmtime engine and module outside blocking
        // let component_name ="math_tasks".to_string();
        println!("Running component {:?}, func: {:?}", component_name, func_name);
        
        // Use the shared wasm_loader instead of creating a new one
        let func_to_run: wasmtime::component::Func = shared_wasm_loader.lock().await.load_func(component_name, func_name).await;
        
        let shared_wasm_loader_clone = shared_wasm_loader.clone();
        let handler = task::spawn_blocking(move || {
            println!{"Task {task_id} running on core {:?}", core_id.clone()};
            core_affinity::set_for_current(core_id);
            // Create a local runtime for async operations
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            
            let input = vec![input_to_wasm_event_val(payload)];

            let result: Result<Vec<Val>, anyhow::Error> = rt.block_on(async move {
                shared_wasm_loader_clone.lock().await.run_func(input, func_to_run).await
            });
            match &result {
                Ok(val) => Self::store_result_uncompressed(task_id, &format!("{:?}", val)),
                Err(e) => Self::store_result_uncompressed(task_id, &format!("Error: {:?}", e)),
            }
            println!("Finished wasm task {}", task_id);
            result
        });
        handler
    }

    pub async fn  run_job(&self, task_id:usize, binary_path: String, func_name:String, payload:String, folder_to_mount:String){
        let core_id: CoreId = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
        let task_module_path = binary_path;
        // Spawn a blocking task to map the worker to the core 
        let handler = Self::run_wasm_job_component(core_id, task_id ,task_module_path, func_name, payload, folder_to_mount, self.wasm_loader.clone()).await;
        // tokio::time::sleep(std::time::Duration::from_secs(20)).await; // for testing
        ()
    }

    pub async fn start(&self){
        println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
        loop{
            // Check for shutdown signal
            if *self.shutdown_flag.lock().await {
                println!("Worker: {:?} shutting down", self.worker_id);
                break;
            }
            
            tokio::time::sleep(std::time::Duration::from_secs(5)).await; // for testing

            // Retrieve the next task from the queue runs it buy awaiting and returns result
            let mut queue = self.thread_queue.lock().await; // lock the mutex
            // println!("Worker: {:?} queue: {:?} ", self.worker_id, queue);
            let my_task:Option<Job> = queue.pop_front();
            match my_task{
                None=> (),
                Some(my_task_1)=>{
                    let task_id = my_task_1.id.clone(); // for testing
                    let payload: String = my_task_1.payload.clone();
                    let func_name = my_task_1.func_name.clone();
                    let folder_to_mount = my_task_1.folder_to_mount.clone();
                    let _res = self.run_job(task_id, my_task_1.binary_path, func_name, payload, folder_to_mount).await;
                    ()
                    }
                }
            }
        }
}






