use crate::api::api_objects::Job;
use crate::channel_objects::{JobAskStatus, JobType, Message, Status, TaskStatusMessage, ToSchedulerMessage, ToWorkerMessage};
use crate::memory_monitoring::get_available_memory_kb;
use crate::wasm_loaders::{WasmComponentLoader, ComponentCache};
use crate::{channel_objects::MessageType, evaluation_metrics::EvaluationMetrics};
use base64::{Engine, engine::general_purpose};
use core_affinity::CoreId;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
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

pub struct Worker {
    pub worker_id: usize,
    pub core_id: CoreId,
    worker_channel_tx: mpsc::Sender<(MessageType, Message)>,
    worker_channel_rx: Arc<Mutex<mpsc::Receiver<(MessageType, Message)>>>,
    shutdown_flag: Arc<Mutex<bool>>,
    wasm_loader: Arc<Mutex<WasmComponentLoader>>,
    component_cache: Arc<ComponentCache>, // Shared component cache (for compilation)
    workers_notification_channel: Arc<Notify>, 
    evaluation_metrics: Arc<EvaluationMetrics>, // Reference to evaluation metrics
    num_concurrent_tasks: Arc<Mutex<usize>>, // Mutable to allow changing in sequential mode
    task_memory_map: Arc<HashMap<String, usize>>, // HashMap of task_id -> memory_kb
}

/// Converts the input to the wasm component from string to wasmtime::component::Val
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
        component_cache: Arc<ComponentCache>,
        worker_channel_tx: mpsc::Sender<(MessageType, Message)>,
        worker_channel_rx: mpsc::Receiver<(MessageType, Message)>,
        workers_notification_channel: Arc<Notify>,
        evaluation_metrics: Arc<EvaluationMetrics>,
        num_concurrent_tasks: usize,
    ) -> Self {
        let shutdown_flag = Arc::new(Mutex::new(false));
        let worker_channel_rx = Arc::new(Mutex::new(worker_channel_rx));
        
        // Load task_to_memory_kb.csv into HashMap
        let task_memory_map = Arc::new(Worker::load_task_memory_map());
        
        Worker {
            worker_id,
            core_id,
            worker_channel_tx,
            worker_channel_rx,
            shutdown_flag,
            wasm_loader,
            component_cache,
            workers_notification_channel,
            evaluation_metrics,
            num_concurrent_tasks: Arc::new(Mutex::new(num_concurrent_tasks)),
            task_memory_map,
        }
    }

    /// Set num_concurrent_tasks (used in sequential mode)
    pub async fn set_num_concurrent_tasks(&self, num: usize) {
        let mut num_tasks = self.num_concurrent_tasks.lock().await;
        *num_tasks = num;
    }

    /// Load task_to_memory_kb.csv into a HashMap
    fn load_task_memory_map() -> HashMap<String, usize> {
        let mut map = HashMap::new();
        let csv_path = "task_to_memory_kb.csv";
        
        match std::fs::read_to_string(csv_path) {
            Ok(contents) => {
                for line in contents.lines() {
                    let parts: Vec<&str> = line.split(',').collect();
                    if parts.len() == 2 {
                        let task_id = parts[0].trim().to_string();
                        if let Ok(memory_kb) = parts[1].trim().parse::<usize>() {
                            map.insert(task_id, memory_kb);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Warning: Failed to load {}: {}. Task memory checks will be skipped.", csv_path, e);
            }
        }
        map
    }

    /// Send TaskStatusMessage to scheduler
    async fn send_task_status(&self, task_id: String, status: Status) {
        let task_status_msg = TaskStatusMessage {
            worker_id: self.worker_id,
            status: status.clone(),
            job_id: task_id.clone(),
        };

        let message_json = serde_json::to_string(&task_status_msg)
            .unwrap_or_else(|e| {
                eprintln!("Worker {}: Failed to serialize TaskStatusMessage: {}", self.worker_id, e);
                return String::new();
            });

        if message_json.is_empty() {
            return;
        }

        let message = Message {
            message: message_json,
            message_type: MessageType::TaskStatusMessage,
        };

        if let Err(e) = self.worker_channel_tx.send((MessageType::TaskStatusMessage, message)).await {
            eprintln!("Worker {}: Failed to send TaskStatusMessage: {}", self.worker_id, e);
        }
    }

    /// Store result of wasm component execution for the user to retrieve
    pub fn store_result_uncompressed<T: std::fmt::Display>(task_id: &str, result: &T) {
        let file_name = format!("results/result_{}.txt", task_id);
        let result_string = format!("Result: {}", result);
        std::fs::write(&file_name, &result_string)
            .expect("Failed to write uncompressed result to file");
    }

    /// Store compressed result of wasm component execution for the user to retrieve 
    pub fn _store_compressed_result<T: std::fmt::Display>(task_id: &str, result: &T) {
        let file_name = format!("results/result_{}.gz", task_id);

        let result_string = format!("Result: {}", result);
        let mut compressed_data = Vec::new();
        {
            let mut encoder = GzEncoder::new(&mut compressed_data, Compression::default());
            std::io::Write::write_all(&mut encoder, result_string.as_bytes())
                .expect("Failed to compress result");
            encoder.finish().expect("Failed to finish compression");
        }
        std::fs::write(&file_name, &compressed_data)
            .expect("Failed to write compressed result to file");
    }


    pub async fn run_wasm_job_component(
        _core_id: CoreId,
        task_id: String,
        component_name: String,
        func_name: String,
        payload: String,
        shared_wasm_loader: Arc<Mutex<WasmComponentLoader>>,
        component_cache: Arc<ComponentCache>,
        evaluation_metrics: Arc<EvaluationMetrics>,
        start_time: std::time::Instant,
    ) -> Result<(String, Result<Vec<Val>, anyhow::Error>), anyhow::Error> {

        // Load function - only the component loading needs the lock (for cache access)
        // The actual instantiation happens in the worker's own store (parallel)
        let func_to_run = {
            let mut loader = shared_wasm_loader.lock().await;
            loader.load_func(&component_cache, component_name, func_name).await
        };

        // Prepare input
        let input = vec![input_to_wasm_event_val(payload)];

        // Run function - uses worker's own store (no lock contention with other workers)
        let result = {
            let mut loader = shared_wasm_loader.lock().await;
            loader.run_func(input, func_to_run).await
        };

        match &result {
            Ok(val) => Self::store_result_uncompressed(&task_id, &format!("{:?}", val)),
            Err(e) => Self::store_result_uncompressed(&task_id, &format!("Error: {:?}", e)),
        }

        // Set response time
        let end_time = std::time::Instant::now();
        let response_time = end_time.duration_since(start_time);
        evaluation_metrics.set_response_time(task_id.clone(), response_time);

        Ok((task_id, result))
    }

    async fn run_task(&self, task: Job)->Status {
        let task_id = task.id.clone();

        // Check memory requirement before executing
        let current_memory = get_available_memory_kb();
        let required_memory = self.task_memory_map.get(&task_id).copied();
        
        if let Some(required_mem) = required_memory {
            if current_memory <= required_mem {
                eprintln!(
                    "Worker {}: Insufficient memory for task {}. Required: {} KB, Available: {} KB",
                    self.worker_id, task_id, required_mem, current_memory
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                // Send failure status and return early
                self.send_task_status(task_id.clone(), Status::Failed).await;
                return Status::Failed;
            }
        }

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
        let component_cache = self.component_cache.clone();
        let evaluation_metrics = self.evaluation_metrics.clone();
        let core_id = self.core_id;

        // Get start_time from evaluation metrics
        let start_time = evaluation_metrics.get_execution_start_time();
        let start_time = match start_time {
            Some(t) => t,
            None => {
                eprintln!(
                    "Worker {}: Execution not started yet, skipping task {}",
                    self.worker_id, task_id
                );
                // Send failure status
                self.send_task_status(task_id.clone(), Status::Failed).await;
                return Status::Failed;
            }
        };

        let handler = Worker::run_wasm_job_component(
            core_id,
            task_id.clone(),
            binary_path,
            func_name,
            payload,
            wasm_loader,
            component_cache,
            evaluation_metrics.clone(),
            start_time,
        )
        .await;

        let completed_task_id = match handler {
            Ok((task_id_result, _result)) => task_id_result,
            Err(_) => {
                // Task execution failed
                self.send_task_status(task_id.clone(), Status::Failed).await;
                return Status::Failed;
            }
        };

        // Send success status
        self.send_task_status(completed_task_id.clone(), Status::Success).await;
        Status::Success
    }

    pub async fn shutdown(&self){
        let mut flag = self.shutdown_flag.lock().await;
        *flag = true;
    }


    /// Send a job request to scheduler with parameters worker_id, job_type, memory_capacity
    pub async fn request_tasks(&self, job_type: JobType, num_jobs:usize) {
        let available_memory = get_available_memory_kb();
        // Create ToSchedulerMessage with the required fields
        let to_scheduler_msg = ToSchedulerMessage {
            worker_id: self.worker_id,
            memory_capacity: available_memory,
            job_type: job_type.clone(),
            num_jobs: num_jobs
        };

        // Serialize the message to JSON
        let message_json = serde_json::to_string(&to_scheduler_msg)
            .unwrap_or_else(|e| panic!("Worker {}: Failed to serialize ToSchedulerMessage: {}", self.worker_id, e));

        // Create Message wrapper
        let message = Message {
            message: message_json,
            message_type: MessageType::ToSchedulerMessage,
        };

        // Send the message to the scheduler
        self.worker_channel_tx
        .send((MessageType::ToSchedulerMessage, message))
        .await.unwrap_or_else(|e| {panic!(
            "Worker {}: Failed to send message to scheduler: {}",
            self.worker_id, e
        )});
    }

    /// Receive messages from Scheduler
    /// If termination message is received return immediately!
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
                                }
                                JobAskStatus::Terminate => {
                                    termination_flag=true;
                                    return (termination_flag, tasks);
                                }
                                JobAskStatus::NotFound => {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                        .await;
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
                    },
                    MessageType::TaskStatusMessage =>{
                        // This shouldn't happen - worker shouldn't receive TaskStatusMessage
                    }
                }
            }
            None => {
                // Channel closed
            }
        }

        drop(rx); // Release the lock

        return (termination_flag, tasks);
    }

    // Works only when executed on linux.
    // Prints available memory a task predicted memory
    pub fn print_memort_status(&self, _task: &Job){
        // Memory status printing removed
    }

    pub async fn process_task(&self, tasks_to_process: &mut Vec<Job> ){
        if !tasks_to_process.is_empty() {
            match tasks_to_process.len() {
                1 => {
                    let task = tasks_to_process.pop().unwrap();
                    self.print_memort_status(&task);
                    self.run_task(task).await;
                    
                    // Clear store after single task completes
                    // All Func objects from this task are already dropped
                    {
                        let mut loader = self.wasm_loader.lock().await;
                        loader.clear_store("models");
                    }
                    
                }
                2 => {
                    let task_2 = tasks_to_process.pop().unwrap();
                    let task_1 = tasks_to_process.pop().unwrap();
                    self.print_memort_status(&task_1);
                    self.print_memort_status(&task_2);

                    let mut handlers = Vec::new();
                    handlers.push(self.run_task(task_1));
                    handlers.push(self.run_task(task_2));
                    
                    // Wait for all concurrent tasks to complete
                    futures::future::join_all(handlers).await;
                    
                    // Clear store AFTER all concurrent tasks complete
                    // This ensures no Func objects from the old store are still in use
                    {
                        let mut loader = self.wasm_loader.lock().await;
                        loader.clear_store("models");
                    }
                }
                _ => {
                    for _ in 0..tasks_to_process.len() {
                        let task = tasks_to_process.pop().unwrap();
                        self.run_task(task).await;
                    }
                    
                    // Clear store after all tasks complete
                    {
                        let mut loader = self.wasm_loader.lock().await;
                        loader.clear_store("models");
                    }
                }
            }
        }

    }

    /// The higher level logic on receiving tasks
    pub async fn receive_responses_handler(&self, tasks_to_process: &mut Vec<Job>, expected_responses: usize)->bool{
        let mut received_count = 0;
        let mut termination_flag_local = false;
        while received_count < expected_responses {
            let (new_termination_flag, new_tasks) = self.receive_tasks().await;
            tasks_to_process.extend(new_tasks);
            received_count += 1;
            termination_flag_local = new_termination_flag;
            
            // If we received termination, stop receiving immediately
            if termination_flag_local {
                break;
            }
        }
        return termination_flag_local;
    }

    /// Worker background loop -> sends job request messages to scheduler and receives them
    /// When terminating it stops 
    pub async fn start(&self) {
        // Wait for signal to start processing (from execute_jobs endpoint)
        self.workers_notification_channel.notified().await;
        let mut request_flag = false;
        let mut tasks_to_process: Vec<Job> = Vec::new();
        let mut termination_flag=false;
        loop {
            // Check for shutdown signal
            {
                let mut flag = self.shutdown_flag.lock().await;
                if *flag {
                    *flag = false;
                    return;
                }
            } // Lock is dropped here

            // If terminated, process remaining tasks and go idle waiting for notification to restart
            if termination_flag {
                // Process any remaining tasks first
                self.process_task(&mut tasks_to_process).await;

                // Aggressively clear WASM store and linker to free all memory
                {
                    let mut loader = self.wasm_loader.lock().await;
                    // Use aggressive clear which recreates both store and linker
                    loader.clear_memory("models");
                }
                
                // Drop the tasks vector to free memory
                tasks_to_process.clear();
                tasks_to_process.shrink_to_fit();
                
                // Small delay to allow OS to reclaim memory
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Now go idle and wait for notification
                self.workers_notification_channel.notified().await;
                termination_flag = false;
                request_flag = false; // Reset request flag when resuming
                continue; 
            }

            // Track if we actually sent requests in this iteration
            let mut sent_requests = 0;

            // If no termination flag -> Send request for jobs message to scheduler
            if !termination_flag && tasks_to_process.is_empty() {
                // Request tasks based on num_concurrent_tasks
                let num_tasks = *self.num_concurrent_tasks.lock().await;
                if num_tasks == 1 && !request_flag {
                    self.request_tasks(JobType::Mixed, 1).await;
                    sent_requests = 1;
                    request_flag = true;
                } else if num_tasks == 2 && !request_flag {
                    self.request_tasks(JobType::Mixed, 2).await;
                    sent_requests = 2;
                    request_flag = true;
                }
            }

            // Only receive responses if we actually sent requests in this iteration
            if sent_requests > 0 && !termination_flag {
                termination_flag = self.receive_responses_handler(&mut tasks_to_process, sent_requests).await;
            }

            // If tasks are empty -> wait a bit and continue
            if tasks_to_process.is_empty() {
                request_flag = false;
                // tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                continue;
            }
            else{
                self.process_task(&mut tasks_to_process).await;
            }
        }
    }
}
