use std::sync::Arc;
use core_affinity::{CoreId};
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
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>,
    execution_notify: Arc<Notify>, // Wait for signal to start processing
    evaluation_metrics: Arc<EvaluationMetrics>, // Reference to evaluation metrics
    num_concurrent_tasks: usize,
}

fn input_to_wasm_event_val(input:String) -> wasmtime::component::Val {
    let event_val = wasmtime::component::Val::String(input.into());
    let record_fields = vec![
        ("event".to_string(), event_val)
    ];
    wasmtime::component::Val::Record(record_fields.into())
}

impl Worker{
    pub fn new(worker_id:usize, core_id:CoreId, wasm_loader: Arc<Mutex<WasmComponentLoader>>, io_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, cpu_bound_rx: Arc<Mutex<mpsc::Receiver<Job>>>, execution_notify: Arc<Notify>, evaluation_metrics: Arc<EvaluationMetrics>, num_concurrent_tasks: usize)->Self{
        let shutdown_flag = Arc::new(Mutex::new(false));
        Worker{
            worker_id, 
            core_id, 
            io_bound_rx,
            cpu_bound_rx,
            shutdown_flag, 
            wasm_loader, 
            execution_notify, 
            evaluation_metrics,
            num_concurrent_tasks,
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
    async fn receive_tasks(&self, queue1: &mut Arc<Mutex<VecDeque<Job>>>, queue2: &mut Arc<Mutex<VecDeque<Job>>>) -> bool {
        let mut received_any = false;
        
        // Try to receive ONE task from IO-bound channel (fair distribution)
        // Only take one at a time to prevent one worker from grabbing all tasks
        {
            let mut rx = self.io_bound_rx.lock().await;
            match rx.try_recv() {
                Ok(job) => {
                    drop(rx);
                    let mut queue = queue1.lock().await;
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
                    let mut queue = queue2.lock().await;
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

    async fn get_task_from_local_queue(local_queue: &mut Arc<Mutex<VecDeque<Job>>>) -> Option<Job> {
        // Get one task to process (prefer IO-bound, then CPU-bound)
        let mut lq = local_queue.lock().await;
        if let Some(task) = lq.pop_front() {
            Some(task)
        } else {
            None
        }

    }

    pub async fn start(&self){
        println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
        let mut local_io_queue:Arc<Mutex<VecDeque<Job>>> = Arc::new(Mutex::new(VecDeque::new()));
        let mut local_cpu_queue:Arc<Mutex<VecDeque<Job>>> = Arc::new(Mutex::new(VecDeque::new()));
        loop{
            // Check for shutdown signal
            if *self.shutdown_flag.lock().await {
                println!("Worker: {:?} shutting down", self.worker_id);
                break;
            }
            
            // Wait for signal to start processing (from execute_jobs endpoint)-code blocks here until the signal is received
            self.execution_notify.notified().await;
            
            println!("Worker {}: Starting to process tasks", self.worker_id);
            
            loop {
                // Check for shutdown signal
                if *self.shutdown_flag.lock().await {
                    println!("Worker: {:?} shutting down", self.worker_id);
                    return;
                }
                
                // Collect tasks from local queues
                let mut tasks_to_process = Vec::new();
                
                // Collect up to num_concurrent_tasks (or 1 if sequential)
                let max_tasks = if self.num_concurrent_tasks == 1 { 1 } else { self.num_concurrent_tasks };
                
                if let Some(task) = Self::get_task_from_local_queue(&mut local_io_queue).await {
                    tasks_to_process.push(task);
                }
                if tasks_to_process.len() < max_tasks {
                    if let Some(task) = Self::get_task_from_local_queue(&mut local_cpu_queue).await {
                        tasks_to_process.push(task);
                    }
                }
                
                // Process tasks or fetch more from channels
                if tasks_to_process.is_empty() {
                    let received = self.receive_tasks(&mut local_io_queue, &mut local_cpu_queue).await;
                    if !received && self.evaluation_metrics.are_all_tasks_completed().await {
                        println!("Worker {}: All {} tasks completed, exiting", self.worker_id, self.evaluation_metrics.get_total_tasks().await);
                        break;
                    }
                    if !received {
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                } else if self.num_concurrent_tasks == 2 && tasks_to_process.len() == 2 {
                    // Concurrent: process 2 tasks at the same time
                    let task_2 = tasks_to_process.pop().unwrap();
                    let task_1 = tasks_to_process.pop().unwrap();
                    tokio::join!(
                        self.run_task(task_1),
                        self.run_task(task_2)
                    );
                } else {
                    // Sequential: process tasks one at a time
                    for task in tasks_to_process {
                        self.run_task(task).await;
                    }
                }
            }
        }
    }
}


