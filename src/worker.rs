use std::usize;
use std::sync::Arc;
use core_affinity::{CoreId};
use serde_json::json;
use tokio::{sync::{Mutex, Notify}, task};
use crate::{api::api_objects::SubmittedJobs, scheduler::SchedulerEngine};
use crate::api::api_objects::Job;
use crate::wasm_loaders::WasmComponentLoader;
use wasmtime::component::Val;
use flate2::write::GzEncoder;
use flate2::Compression;
use flate2::read::GzDecoder;
use std::io::Read;
use base64::{Engine as _, engine::general_purpose};

/// Decompress a gzip-compressed base64 payload
fn decompress_payload(compressed_base64: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Decode base64 to get compressed bytes
    let compressed_bytes = general_purpose::STANDARD.decode(compressed_base64)?;
    
    // Decompress using gzip
    let mut decoder = GzDecoder::new(&compressed_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    
    Ok(decompressed)
}


// Worker is mapped to a core id and runs the tasks from SubmittedJobs
pub struct Worker{
    worker_id:usize,
    pub core_id:CoreId,
    submitted_jobs: actix_web::web::Data<SubmittedJobs>, // Reference to submitted jobs
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>,
    execution_notify: Arc<Notify>, // Wait for signal to start processing
    scheduler: Arc<Mutex<Option<Arc<Mutex<SchedulerEngine>>>>>, // Reference to scheduler for completion notification
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
    pub fn new(worker_id:usize, core_id:CoreId, wasm_loader: Arc<Mutex<WasmComponentLoader>>, submitted_jobs: actix_web::web::Data<SubmittedJobs>, execution_notify: Arc<Notify>)->Self{
        let shutdown_flag = Arc::new(Mutex::new(false));
        let scheduler = Arc::new(Mutex::new(None));
        Worker{worker_id, core_id, submitted_jobs, shutdown_flag, wasm_loader, execution_notify, scheduler}
    }

    pub async fn set_scheduler(&self, scheduler: Arc<tokio::sync::Mutex<crate::scheduler::SchedulerEngine>>) {
        *self.scheduler.lock().await = Some(scheduler);
    }

    pub async fn shutdown(&self) {
        let mut flag = self.shutdown_flag.lock().await;
        *flag = true;
    }

    pub fn store_result_uncompressed<T:std::fmt::Display>(task_id:&str, result:&T){
        // Store compressed result in a file named after the task
        let file_name = format!("results/result_{}.txt", task_id);
        
        // Format the result
        let result_string = format!("Result: {}", result);
        
        
        // Write the compressed data to file
        std::fs::write(&file_name, &result_string).expect("Failed to write compressed result to file");
        
        println!("Stored result for task {}: {} ", 
                 task_id, result_string.len());
    }


    pub fn store_result<T:std::fmt::Display>(task_id:&str, result:&T){
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
    
    pub async fn run_wasm_job_component(core_id: CoreId, task_id: String, component_name:String, func_name:String, payload:String, folder_to_mount:String, shared_wasm_loader: Arc<Mutex<WasmComponentLoader>>)->task::JoinHandle<(String, Result<Vec<Val>, anyhow::Error>)>{
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
                Ok(val) => Self::store_result_uncompressed(&task_id, &format!("{:?}", val)),
                Err(e) => Self::store_result_uncompressed(&task_id, &format!("Error: {:?}", e)),
            }
            println!("Finished wasm task {}", task_id);
            
            // Return task_id along with result so we can notify scheduler
            (task_id, result)
        });
        handler
    }

    pub async fn  run_job(&self, task_id:String, binary_path: String, func_name:String, payload:String, folder_to_mount:String){
        let core_id: CoreId = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
        let task_module_path = binary_path;
        let scheduler_ref = self.scheduler.clone();
        
        // Spawn a blocking task to map the worker to the core 
        let handler = Self::run_wasm_job_component(core_id, task_id.clone() ,task_module_path, func_name, payload, folder_to_mount, self.wasm_loader.clone()).await;
        
        // Wait for task to complete and get the task_id back
        // Note: handler.await returns Result<(String, Result<...>), JoinError>
        let completed_task_id = match handler.await {
            Ok((task_id_result, _result)) => task_id_result,
            Err(_) => task_id.clone(), // If join failed, use original task_id
        };
        
        // Notify scheduler that task is complete
        if let Some(scheduler) = scheduler_ref.lock().await.as_ref() {
            scheduler.lock().await.on_task_completed(completed_task_id).await;
        }
    }

    pub async fn start(&self){
        println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
        loop{
            // Check for shutdown signal
            if *self.shutdown_flag.lock().await {
                println!("Worker: {:?} shutting down", self.worker_id);
                break;
            }
            
            // Wait for signal to start processing (from execute_jobs endpoint)
            self.execution_notify.notified().await;
            
            // Process all available tasks until queue is empty
            loop {
                // Check for shutdown signal
                if *self.shutdown_flag.lock().await {
                    println!("Worker: {:?} shutting down", self.worker_id);
                    return;
                }
                
                // Try to get one I/O-bound and one CPU-bound task
                let io_task_opt = self.submitted_jobs.get_next_io_bounded_job().await;
                let cpu_task_opt = self.submitted_jobs.get_next_cpu_bounded_job().await;
                
                // Check if we have any tasks
                let has_io_task = io_task_opt.is_some();
                let has_cpu_task = cpu_task_opt.is_some();
                
                // Process both tasks concurrently if available
                let mut handles = Vec::new();
                
                // Spawn I/O-bound task if available
                if let Some(my_task_1) = io_task_opt {
                    let task_id = my_task_1.id.clone();
                    // Decompress payload if needed
                    let payload: String = if my_task_1.payload_compressed {
                        match decompress_payload(&my_task_1.payload) {
                            Ok(decompressed) => decompressed,
                            Err(e) => {
                                eprintln!("Failed to decompress payload for task {}: {}", task_id, e);
                                my_task_1.payload.clone()
                            }
                        }
                    } else {
                        my_task_1.payload.clone()
                    };
                    let func_name = my_task_1.func_name.clone();
                    let folder_to_mount = my_task_1.folder_to_mount.clone();
                    let binary_path = my_task_1.binary_path.clone();
                    let wasm_loader = self.wasm_loader.clone();
                    let scheduler_ref = self.scheduler.clone();
                    let core_id = self.core_id;
                    
                    // Spawn I/O-bound task to run concurrently
                    let handle = tokio::spawn(async move {
                        let handler = Worker::run_wasm_job_component(
                            core_id, task_id.clone(), binary_path, func_name, payload, folder_to_mount, wasm_loader
                        ).await;
                        let completed_task_id = match handler.await {
                            Ok((task_id_result, _result)) => task_id_result,
                            Err(_) => task_id.clone(),
                        };
                        if let Some(scheduler) = scheduler_ref.lock().await.as_ref() {
                            scheduler.lock().await.on_task_completed(completed_task_id).await;
                        }
                    });
                    handles.push(handle);
                }
                
                // Spawn CPU-bound task if available
                if let Some(my_task_1) = cpu_task_opt {
                    let task_id = my_task_1.id.clone();
                    // Decompress payload if needed
                    let payload: String = if my_task_1.payload_compressed {
                        match decompress_payload(&my_task_1.payload) {
                            Ok(decompressed) => decompressed,
                            Err(e) => {
                                eprintln!("Failed to decompress payload for task {}: {}", task_id, e);
                                my_task_1.payload.clone()
                            }
                        }
                    } else {
                        my_task_1.payload.clone()
                    };
                    let func_name = my_task_1.func_name.clone();
                    let folder_to_mount = my_task_1.folder_to_mount.clone();
                    let binary_path = my_task_1.binary_path.clone();
                    let wasm_loader = self.wasm_loader.clone();
                    let scheduler_ref = self.scheduler.clone();
                    let core_id = self.core_id;
                    
                    // Spawn CPU-bound task to run concurrently
                    let handle = tokio::spawn(async move {
                        let handler = Worker::run_wasm_job_component(
                            core_id, task_id.clone(), binary_path, func_name, payload, folder_to_mount, wasm_loader
                        ).await;
                        let completed_task_id = match handler.await {
                            Ok((task_id_result, _result)) => task_id_result,
                            Err(_) => task_id.clone(),
                        };
                        if let Some(scheduler) = scheduler_ref.lock().await.as_ref() {
                            scheduler.lock().await.on_task_completed(completed_task_id).await;
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all spawned tasks to complete
                for handle in handles {
                    let _ = handle.await;
                }
                
                // If no tasks were found, check if queue is empty
                if !has_io_task && !has_cpu_task {
                    if self.submitted_jobs.get_num_tasks().await == 0 {
                        break;
                    }
                }
            }
        }
    }
}


