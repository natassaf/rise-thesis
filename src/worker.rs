use crate::api::api_objects::Job;
use crate::channel_objects::{JobAskStatus, JobType, Message, ToSchedulerMessage, ToWorkerMessage};
use crate::memory_monitoring::{get_memory_current_kb, get_memory_max};
use crate::wasm_loaders::WasmComponentLoader;
use crate::{channel_objects::MessageType, evaluation_metrics::EvaluationMetrics};
use base64::{Engine, engine::general_purpose};
use core_affinity::CoreId;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, mpsc};
use wasmtime::component::Val;


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
pub struct Worker {
    pub worker_id: usize,
    pub core_id: CoreId,
    worker_channel_tx: mpsc::Sender<(MessageType, Message)>,
    worker_channel_rx: Arc<Mutex<mpsc::Receiver<(MessageType, Message)>>>,
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>,
   workers_notification_channel: Arc<Notify>, // Wait for signal to start processing
    evaluation_metrics: Arc<EvaluationMetrics>, // Reference to evaluation metrics
    num_concurrent_tasks: usize,
}

fn input_to_wasm_event_val(input: String) -> wasmtime::component::Val {
    let event_val = wasmtime::component::Val::String(input.into());
    let record_fields = vec![("event".to_string(), event_val)];
    wasmtime::component::Val::Record(record_fields.into())
}

impl Worker {
    pub fn new(
        worker_id: usize,
        core_id: CoreId,
        wasm_loader: Arc<Mutex<WasmComponentLoader>>,
        worker_channel_tx: mpsc::Sender<(MessageType, Message)>,
        worker_channel_rx: mpsc::Receiver<(MessageType, Message)>,
        workers_notification_channel: Arc<Notify>,
        evaluation_metrics: Arc<EvaluationMetrics>,
        num_concurrent_tasks: usize,
    ) -> Self {
        let shutdown_flag = Arc::new(Mutex::new(false));
        let worker_channel_rx = Arc::new(Mutex::new(worker_channel_rx));
        Worker {
            worker_id,
            core_id,
            worker_channel_tx,
            worker_channel_rx,
            shutdown_flag,
            wasm_loader,
            workers_notification_channel,
            evaluation_metrics,
            num_concurrent_tasks,
        }
    }
    
    pub async fn shutdown(&self) {
        let mut flag = self.shutdown_flag.lock().await;
        *flag = true;
    }

    pub fn store_result_uncompressed<T: std::fmt::Display>(task_id: &str, result: &T) {
        // Store compressed result in a file named after the task
        let file_name = format!("results/result_{}.txt", task_id);

        // Format the result
        let result_string = format!("Result: {}", result);

        // Write the compressed data to file
        std::fs::write(&file_name, &result_string)
            .expect("Failed to write compressed result to file");

        println!(
            "Stored result for task {}: {} ",
            task_id,
            result_string.len()
        );
    }

    pub fn store_result<T: std::fmt::Display>(task_id: &str, result: &T) {
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
        std::fs::write(&file_name, &compressed_data)
            .expect("Failed to write compressed result to file");

        println!(
            "Stored compressed result for task {}: {} bytes -> {} bytes",
            task_id,
            result_string.len(),
            compressed_data.len()
        );
    }

    pub async fn run_wasm_job_component(
        core_id: CoreId,
        task_id: String,
        component_name: String,
        func_name: String,
        payload: String,
        shared_wasm_loader: Arc<Mutex<WasmComponentLoader>>,
        evaluation_metrics: Arc<EvaluationMetrics>,
        start_time: std::time::Instant,
    ) -> Result<(String, Result<Vec<Val>, anyhow::Error>), anyhow::Error> {
        println!("Task {task_id} running on core {:?}", core_id);
        println!(
            "Running component {:?}, func: {:?}",
            component_name, func_name
        );

        // Load function - runs on pinned thread
        let func_to_run = shared_wasm_loader
            .lock()
            .await
            .load_func(component_name, func_name)
            .await;

        // Prepare input
        let input = vec![input_to_wasm_event_val(payload)];

        let result = shared_wasm_loader
            .lock()
            .await
            .run_func(input, func_to_run)
            .await;

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
            eprintln!(
                "Worker {}: Execution not started yet, skipping task {}",
                self.worker_id, task_id
            );
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
            start_time,
        )
        .await;

        let completed_task_id = match handler {
            Ok((task_id_result, _result)) => task_id_result,
            Err(_) => task_id.clone(),
        };

        let completion_time = std::time::Instant::now();
        evaluation_metrics
            .set_task_status(completed_task_id.clone(), 1)
            .await;

        // Check if all tasks are completed
        let completed_count = evaluation_metrics.get_completed_count().await;
        let total_tasks = evaluation_metrics.get_total_tasks().await;

        if self.evaluation_metrics.are_all_tasks_completed().await {
            let peak_memory = self.evaluation_metrics.get_peak_memory().await;
            let throughput = evaluation_metrics
                .calculate_throughput(completion_time, total_tasks)
                .await;
            let average_response_time_millis =
                evaluation_metrics.calculate_average_response_time().await;
            let total_time_duration =
                if let Some(start) = evaluation_metrics.get_execution_start_time() {
                    completion_time.duration_since(start)
                } else {
                    std::time::Duration::from_secs(0)
                };

            println!("=== Execution completed ===");
            println!("Total tasks processed: {}", completed_count);
            println!(
                "Total execution time: {:.2} seconds ({:.2} ms)",
                total_time_duration.as_secs(),
                total_time_duration.as_millis()
            );
            println!(
                "Average response time per task: {:.2} ms",
                average_response_time_millis
            );
            println!("Throughput: {:.2} tasks/second", throughput);

            // Store metrics to file
            crate::evaluation_metrics::store_evaluation_metrics(
                completed_count,
                total_time_duration.as_secs_f64(),
                total_time_duration.as_millis() as f64,
                average_response_time_millis,
                throughput,
                peak_memory
            );

        }
    }

    pub async fn request_tasks(&self, job_type: JobType) {
        // let available_memory =Self::get_available_memory_kb().unwrap_or(100000) as usize;
        let available_memory = get_memory_current_kb();
        let memory_limit = get_memory_max();
        println!("Available memory {}", available_memory);
        println!("Memory limit: {}", memory_limit);
        // Create ToSchedulerMessage with the required fields
        let to_scheduler_msg = ToSchedulerMessage {
            worker_id: self.worker_id,
            memory_capacity: available_memory,
            job_type: job_type.clone(),
        };

        // Serialize the message to JSON
        let message_json = match serde_json::to_string(&to_scheduler_msg) {
            Ok(json) => json,
            Err(e) => {
                panic!(
                    "Worker {}: Failed to serialize ToSchedulerMessage: {}",
                    self.worker_id, e
                );
            }
        };

        // Create Message wrapper
        let message = Message {
            message: message_json,
            message_type: MessageType::ToSchedulerMessage,
        };

        // Send the message to the scheduler
        if let Err(e) = self
            .worker_channel_tx
            .send((MessageType::ToSchedulerMessage, message))
            .await
        {
            panic!(
                "Worker {}: Failed to send message to scheduler: {}",
                self.worker_id, e
            );
        }

        println!(
            "Worker {}: Sent task request to scheduler (job_type: {:?}, memory: {})",
            self.worker_id, job_type, available_memory
        );
    }

    pub async fn receive_tasks(&self) -> (bool, Vec<Job>) {
        let mut tasks = Vec::new();
        let mut rx = self.worker_channel_rx.lock().await;
        let mut termination_flag = false;
        // Wait for a message from the scheduler
        match rx.recv().await {
            Some((message_type, message)) => {
                match message_type {
                    MessageType::ToWorkerMessage => {
                        match serde_json::from_str::<ToWorkerMessage>(&message.message) {
                            Ok(to_worker_msg) => match to_worker_msg.status {
                                JobAskStatus::Found => {
                                    tasks.push(to_worker_msg.job);
                                    println!(
                                        "Worker {}: Received job from scheduler ({} of {})",
                                        self.worker_id,
                                        tasks.len(),
                                        self.num_concurrent_tasks
                                    );
                                }
                                JobAskStatus::Terminate => {
                                    println!(
                                        "Worker {}: Received terminate message from scheduler",
                                        self.worker_id
                                    );
                                    // self.shutdown().await;
                                    termination_flag=true;
                                }
                                JobAskStatus::NotFound => {
                                    println!(
                                        "Worker {}: Job not found from scheduler",
                                        self.worker_id
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(1))
                                        .await;
                                }
                                JobAskStatus::OutofBoundsJob => {
                                    println!(
                                        "Worker {}: Out of bounds job from scheduler",
                                        self.worker_id
                                    );
                                }
                            },
                            Err(e) => {
                                eprintln!(
                                    "Worker {}: Failed to deserialize ToWorkerMessage: {}",
                                    self.worker_id, e
                                )
                            }
                        }
                    }
                    MessageType::ToSchedulerMessage => {
                        // This shouldn't happen - worker shouldn't receive ToSchedulerMessage
                        println!(
                            "Worker {}: Received unexpected ToSchedulerMessage",
                            self.worker_id
                        );
                    }
                }
            }
            None => {
                println!(
                    "Worker {}: Channel closed while waiting for scheduler response",
                    self.worker_id
                )
            }
        }

        drop(rx); // Release the lock

        println!(
            "Worker {}: Collected {} tasks from scheduler",
            self.worker_id,
            tasks.len()
        );
        return (termination_flag, tasks);
    }

    pub async fn start(&self) {
        println!(
            "Worker: {:?} started on core id : {:?}",
            self.worker_id, self.core_id
        );
        
        // Wait for signal to start processing (from execute_jobs endpoint)
        self.workers_notification_channel.notified().await;

        println!("Worker {}: Starting to process tasks", self.worker_id);
        let mut request_flag = false;
        let mut tasks_to_process: Vec<Job> = Vec::new();

        let mut termination_flag=false;
        loop {
            // Check for shutdown signal
            let mut flag = self.shutdown_flag.lock().await;
            if *flag {
                println!("Worker: {:?} shutting down", self.worker_id);
                *flag = false;
                return;
            }

            // If terminated, immediately go idle and wait for notification
            if termination_flag {
                // Process any remaining tasks first
                if !tasks_to_process.is_empty() {
                    // Process remaining tasks before going idle
                    match tasks_to_process.len() {
                        1 => {
                            let task = tasks_to_process.pop().unwrap();
                            let available_memory = get_memory_current_kb();
                            let task_memory = task.memory_prediction.unwrap_or(0.0) as usize;
                            println!("Before running check: task memory: {}, available memory {}", task_memory, available_memory);
                            if task_memory <= available_memory {
                                self.run_task(task).await;
                            }
                        }
                        2 => {
                            let available_memory = get_memory_current_kb();
                            let task_2 = tasks_to_process.pop().unwrap();
                            let task_1 = tasks_to_process.pop().unwrap();

                            
                            let task_memory1 = task_1.memory_prediction.unwrap_or(0.0) as usize;
                            let task_memory2 = task_2.memory_prediction.unwrap_or(0.0) as usize;
                            println!("Before running check: task memory1: {}, available memory {}",task_memory1, available_memory);
                            println!("Before running check: task memory2: {}, available memory {}",task_memory2, available_memory);
                            
                            // Create futures for tasks that can run
                            let mut handlers = Vec::new();
                            
                            // if task_memory1 <= available_memory {
                            if true{
                                handlers.push(self.run_task(task_1));
                            }
                            
                            // if task_memory2 <= available_memory {
                            if true{
                                handlers.push(self.run_task(task_2));
                            }
                            
                            // Run all eligible tasks concurrently
                            if handlers.is_empty() {
                                println!("Worker {}: Both tasks skipped due to insufficient memory", self.worker_id);
                            } else {
                                futures::future::join_all(handlers).await;
                            }
                        }
                        _ => {
                            for _ in 0..tasks_to_process.len() {
                                let task = tasks_to_process.pop().unwrap();
                                self.run_task(task).await;
                            }
                        }
                    }
                }
                
                // Now go idle and wait for notification
                println!("Worker {}: Idle, waiting for notification to resume", self.worker_id);
                self.workers_notification_channel.notified().await;
                println!("Worker {}: Resuming task processing", self.worker_id);
                termination_flag = false;
                request_flag = false; // Reset request flag when resuming
                continue; // Skip task requests and go back to top of loop
            }

            // Only request tasks if not terminated
            if !termination_flag {
                // Request tasks based on num_concurrent_tasks
                if self.num_concurrent_tasks == 1
                    && request_flag == false
                    && tasks_to_process.is_empty()
                {
                    self.request_tasks(JobType::Mixed).await;
                    request_flag = true;
                } else if self.num_concurrent_tasks == 2
                    && request_flag == false
                    && tasks_to_process.is_empty()
                {
                    self.request_tasks(JobType::IOBound).await;
                    self.request_tasks(JobType::CPUBound).await;
                    request_flag = true;
                }
            }

            // Receive responses - need to receive as many as we sent
            // For num_concurrent_tasks == 2, we sent 2 requests, so receive 2 responses
            let mut received_count = 0;
            let expected_responses = if self.num_concurrent_tasks == 2 { 2 } else { 1 };
            
            while received_count < expected_responses && !termination_flag {
                let (new_termination_flag, new_tasks) = self.receive_tasks().await;
                termination_flag = termination_flag || new_termination_flag;
                tasks_to_process.extend(new_tasks);
                received_count += 1;
                
                // If we received termination, stop receiving immediately
                if termination_flag {
                    println!("Worker {}: Received termination, stopping further requests", self.worker_id);
                    break;
                }
            }

            // Handle empty tasks
            if tasks_to_process.is_empty() {
                request_flag = false;
                if self.evaluation_metrics.are_all_tasks_completed().await {
                    println!("Worker {}: All tasks completed, exiting", self.worker_id);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                continue;
            }

            // Process tasks based on count
            match tasks_to_process.len() {
                1 => {
                    {
                        let task = tasks_to_process.pop().unwrap();
                        let available_memory = get_memory_current_kb();
                        let task_memory = task.memory_prediction.unwrap_or(0.0) as usize;
                        println!("Before running check: task memory: {}, available memory {}", task_memory, available_memory);
                        // if task_memory <= available_memory {
                        if true{
                            self.run_task(task).await;
                            request_flag = false;
                        }
                    }                    
                }
                2 => {
                    {
                        let available_memory = get_memory_current_kb();
                        let task_2 = tasks_to_process.pop().unwrap();
                        let task_1 = tasks_to_process.pop().unwrap();

                        
                        let task_memory1 = task_1.memory_prediction.unwrap_or(0.0) as usize;
                        let task_memory2 = task_2.memory_prediction.unwrap_or(0.0) as usize;
                        println!("Before running check: task memory1: {}, available memory {}",task_memory1, available_memory);
                        println!("Before running check: task memory2: {}, available memory {}",task_memory2, available_memory);
                        
                        // Create futures for tasks that can run
                        let mut handlers = Vec::new();
                        
                        // if task_memory1 <= available_memory {
                        if true{
                            handlers.push(self.run_task(task_1));
                        }
                        
                        // if task_memory2 <= available_memory {
                        if true{
                            handlers.push(self.run_task(task_2));
                        }
                        
                        // Run all eligible tasks concurrently
                        if handlers.is_empty() {
                            println!("Worker {}: Both tasks skipped due to insufficient memory", self.worker_id);
                        } else {
                            futures::future::join_all(handlers).await;
                        }
                    }
                    request_flag = false;
                }
                _ => {
                    for _ in 0..tasks_to_process.len() {
                        let task = tasks_to_process.pop().unwrap();
                        self.run_task(task).await;
                    }
                    request_flag = false;
                }
            }
        }
    }
}
