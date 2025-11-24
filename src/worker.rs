use std::sync::Arc;
use core_affinity::{CoreId};
use serde_json::json;
use tokio::sync::{Mutex, Notify, mpsc};
use crate::evaluation_metrics::EvaluationMetrics;
use crate::api::api_objects::Job;
use crate::wasm_loaders::WasmComponentLoader;
use wasmtime::component::Val;
use std::collections::VecDeque;
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


// Worker is mapped to a core id and pulls tasks from shared channels when free
pub struct Worker{
    worker_id:usize,
    pub core_id:CoreId,
    io_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, // Shared IO-bound task channel receiver
    cpu_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, // Shared CPU-bound task channel receiver
    local_io_queue: Arc<Mutex<VecDeque<Job>>>, // Local queue for IO-bound tasks
    local_cpu_queue: Arc<Mutex<VecDeque<Job>>>, // Local queue for CPU-bound tasks
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>,
    execution_notify: Arc<Notify>, // Wait for signal to start processing
    evaluation_metrics: Arc<EvaluationMetrics>, // Reference to evaluation metrics
}

fn input_to_wasm_event_val(input:String) -> wasmtime::component::Val {
    let event_val = wasmtime::component::Val::String(input.into());
    let record_fields = vec![
        ("event".to_string(), event_val)
    ];
    wasmtime::component::Val::Record(record_fields.into())
}

impl Worker{
    pub fn new(worker_id:usize, core_id:CoreId, wasm_loader: Arc<Mutex<WasmComponentLoader>>, io_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, cpu_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, execution_notify: Arc<Notify>, evaluation_metrics: Arc<EvaluationMetrics>)->Self{
        let shutdown_flag = Arc::new(Mutex::new(false));
        Worker{
            worker_id, 
            core_id, 
            io_bound_rx,
            cpu_bound_rx,
            local_io_queue: Arc::new(Mutex::new(VecDeque::new())),
            local_cpu_queue: Arc::new(Mutex::new(VecDeque::new())),
            shutdown_flag, 
            wasm_loader, 
            execution_notify, 
            evaluation_metrics
        }
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
    
    pub async fn run_wasm_job_component(
        core_id: CoreId, 
        task_id: String, 
        component_name: String,
        func_name: String, 
        payload: String, 
        shared_wasm_loader: Arc<Mutex<WasmComponentLoader>>, 
        evaluation_metrics: Arc<EvaluationMetrics>, 
        start_time: std::time::Instant
    ) -> Result<(String, Result<Vec<Val>, anyhow::Error>), anyhow::Error> {
        println!("Task {task_id} running on core {:?}", core_id);
        println!("Running component {:?}, func: {:?}", component_name, func_name);
        
        // Load function - runs on pinned thread
        let func_to_run = shared_wasm_loader.lock().await.load_func(component_name, func_name).await;
        
        // Prepare input
        let input = vec![input_to_wasm_event_val(payload)];
        
        let result = shared_wasm_loader.lock().await.run_func(input, func_to_run).await;
        
        match &result {
            Ok(val) => Self::store_result_uncompressed(&task_id, &format!("{:?}", val)),
            Err(e) => Self::store_result_uncompressed(&task_id, &format!("Error: {:?}", e)),
        }
        println!("Finished wasm task {}", task_id);
        
        // Set response time
        let end_time = std::time::Instant::now();
        let response_time = end_time.duration_since(start_time);
        evaluation_metrics.set_response_time(task_id.clone(), response_time);
        
        Ok((task_id, result))
    }

// pub async fn  run_job(&self, task_id:String, binary_path: String, func_name:String, payload:String, folder_to_mount:String){
    //     let core_id: CoreId = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
    //     let task_module_path = binary_path;
    //     let scheduler_ref = self.scheduler.clone();
        
    //     // Spawn a blocking task to map the worker to the core 
    //     let handler = Self::run_wasm_job_component(core_id, task_id.clone() ,task_module_path, func_name, payload, folder_to_mount, self.wasm_loader.clone()).await;
        
    //     // Wait for task to complete and get the task_id back
    //     // Note: handler.await returns Result<(String, Result<...>), JoinError>
    //     let completed_task_id = match handler.await {
    //         Ok((task_id_result, _result)) => task_id_result,
    //         Err(_) => task_id.clone(), // If join failed, use original task_id
    //     };
        
    //     // Notify scheduler that task is complete
    //     if let Some(scheduler) = scheduler_ref.lock().await.as_ref() {
    //         scheduler.lock().await.on_task_completed(completed_task_id).await;
    //     }
    // }

    // Run a single task - this runs directly on the worker's runtime
    // Each worker processes one task at a time, but multiple workers run in parallel


    async fn run_task(&self, task: Job) {
        let task_id = task.id.clone();
        
        // Decompress payload if needed
        let payload: String = if task.payload_compressed {
            match decompress_payload(&task.payload) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    eprintln!("Failed to decompress payload for task {}: {}", task_id, e);
                    task.payload.clone()
                }
            }
        } else {
            task.payload.clone()
        };
        
        let func_name = task.func_name.clone();
        let binary_path = task.binary_path.clone();
        let wasm_loader = self.wasm_loader.clone();
        let evaluation_metrics = self.evaluation_metrics.clone();
        let core_id = self.core_id;
        
        // Get start_time from evaluation metrics
        let start_time = evaluation_metrics.get_execution_start_time();
        if start_time.is_none() {
            eprintln!("Worker {}: Execution not started yet, skipping task {}", self.worker_id, task_id);
            return;
        }
        let start_time = start_time.unwrap();
        
        // Run the task directly on this worker's runtime (which is pinned to the core)
        // This allows multiple workers to run tasks in parallel
        let handler = Worker::run_wasm_job_component(
            core_id, 
            task_id.clone(), 
            binary_path, 
            func_name, 
            payload, 
            wasm_loader, 
            evaluation_metrics.clone(), 
            start_time
        ).await;
        
        let completed_task_id = match handler {
            Ok((task_id_result, _result)) => task_id_result,
            Err(_) => task_id.clone(),
        };
        
        let completion_time = std::time::Instant::now();
        evaluation_metrics.set_task_status(completed_task_id.clone(), 1).await;
        
        // Check if all tasks are completed
        let completed_count = evaluation_metrics.get_completed_count().await;
        let total_tasks = evaluation_metrics.get_total_tasks().await;
        
        if completed_count > 0 && completed_count == total_tasks {
            let throughput = evaluation_metrics.calculate_throughput(completion_time, total_tasks).await;
            let average_response_time_millis = evaluation_metrics.calculate_average_response_time().await;
            let total_time_duration = if let Some(start) = evaluation_metrics.get_execution_start_time() {
                completion_time.duration_since(start)
            } else {
                std::time::Duration::from_secs(0)
            };
            
            println!("=== Execution completed ===");
            println!("Total tasks processed: {}", completed_count);
            println!("Total execution time: {:.2} seconds ({:.2} ms)", total_time_duration.as_secs(), total_time_duration.as_millis());
            println!("Average response time per task: {:.2} ms", average_response_time_millis);
            println!("Throughput: {:.2} tasks/second", throughput);
            
            // Store metrics to file
            crate::evaluation_metrics::store_evaluation_metrics(
                completed_count,
                total_time_duration.as_secs_f64(),
                total_time_duration.as_millis() as f64,
                average_response_time_millis,
                throughput
            );
            
            // Reset timing for next execution
            evaluation_metrics.reset().await;
        }
    }

    // Receive tasks from channels and add to local queues
    // Returns true if any tasks were received, false otherwise
    // Uses a work-stealing approach: try to get one task at a time to ensure fair distribution
    async fn receive_tasks(&self) -> bool {
        let mut received_any = false;
        
        // Try to receive ONE task from IO-bound channel (fair distribution)
        // Only take one at a time to prevent one worker from grabbing all tasks
        {
            let mut rx = self.io_bound_rx.lock().await;
            match rx.try_recv() {
                Ok(job) => {
                    drop(rx);
                    let mut queue = self.local_io_queue.lock().await;
                    queue.push_back(job);
                    received_any = true;
                    // Only take one task to allow other workers a chance
                    return received_any;
                }
                Err(mpsc::error::TryRecvError::Empty) => {},
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    println!("Worker {}: IO-bound channel disconnected", self.worker_id);
                }
            }
        }
        
        // Try to receive ONE task from CPU-bound channel (fair distribution)
        {
            let mut rx = self.cpu_bound_rx.lock().await;
            match rx.try_recv() {
                Ok(job) => {
                    drop(rx);
                    let mut queue = self.local_cpu_queue.lock().await;
                    queue.push_back(job);
                    received_any = true;
                }
                Err(mpsc::error::TryRecvError::Empty) => {},
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    println!("Worker {}: CPU-bound channel disconnected", self.worker_id);
                }
            }
        }
        
        received_any
    }

    pub async fn start(&self){
        println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
        loop{
            // Check for shutdown signal
            if *self.shutdown_flag.lock().await {
                println!("Worker: {:?} shutting down", self.worker_id);
                break;
            }
            
            // Wait for signal to start processing (from execute_jobs endpoint)-code blocks here until the signal is received
            self.execution_notify.notified().await;
            
            println!("Worker {}: Starting to process tasks", self.worker_id);
            
            // Process tasks one at a time - each worker processes one task at a time
            // Multiple workers run in parallel (one task per worker)
            loop {
                // Check for shutdown signal
                if *self.shutdown_flag.lock().await {
                    println!("Worker: {:?} shutting down", self.worker_id);
                    return;
                }
                
                // Get one task to process (prefer IO-bound, then CPU-bound)
                let task_to_process = {
                    let mut io_queue = self.local_io_queue.lock().await;
                    if let Some(task) = io_queue.pop_front() {
                        Some(task)
                    } else {
                        drop(io_queue);
                        let mut cpu_queue = self.local_cpu_queue.lock().await;
                        cpu_queue.pop_front()
                    }
                };
                
                if let Some(task) = task_to_process {
                    // Process the task
                    self.run_task(task).await;
                    
                    // After processing, try to receive ONE more task from channels
                    // Taking only one ensures fair distribution among workers
                    self.receive_tasks().await;
                } else {
                    // No tasks in local queues - try to receive from channels
                    let received = self.receive_tasks().await;
                    
                    if !received {
                        // If we've completed all tasks, we're done
                        if self.evaluation_metrics.are_all_tasks_completed().await {
                            println!("Worker {}: All {} tasks completed, exiting", self.worker_id, self.evaluation_metrics.get_total_tasks().await);
                            break;
                        }
                        // Not all tasks completed yet - keep trying with a small delay
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}


