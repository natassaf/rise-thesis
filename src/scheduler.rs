use std::sync::Arc;
use std::collections::HashSet;
use std::time::Duration;
use anyhow::Error;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::api::api_objects::{Job, SubmittedJobs};
use crate::channel_objects::{JobAskStatus, JobType, Message, TaskStatusMessage, ToSchedulerMessage, ToWorkerMessage};
use crate::evaluation_metrics::EvaluationMetrics;
use crate::utils::load_config;
use crate::{
    channel_objects::MessageType,
    worker::Worker,
};
use actix_web::web;
use core_affinity::*;



// ========== SCHEDULER ==========
pub struct SchedulerEngine {
    submitted_jobs: web::Data<SubmittedJobs>,
    workers: Vec<Arc<Worker>>,
    scheduler_channel_rx: Arc<Mutex<mpsc::Receiver<(MessageType, Message)>>>, // Channel for workers messages to send to scheduler. Worker channels are cloned
    scheduler_txs: Arc<Mutex<Vec<mpsc::Sender<(MessageType, Message)>>>>, // Senders to send messages to workers
    evaluation_metrics: Arc<EvaluationMetrics>, // Evaluation metrics storage
    component_cache: Arc<crate::wasm_loaders::ComponentCache>, // Shared component cache
    pin_cores: bool,
    worker_requests: Arc<Mutex<HashSet<usize>>>, // Track which workers have sent job requests
    sequential_run_flag: Arc<Mutex<bool>>, // when true run all remaining jobs on one worker
    active_workers: Arc<Mutex<Vec<usize>>>, // Track which workers are active
    worker_thread_handles: Arc<Mutex<Vec<Option<std::thread::JoinHandle<()>>>>>, // Store worker thread handles to kill them when needed
}


impl SchedulerEngine {

    pub fn new(
        core_ids: Vec<CoreId>,
        submitted_jobs: web::Data<SubmittedJobs>,
        num_workers_to_start: usize,
        pin_cores: bool,
        num_concurrent_tasks: usize,
        workers_notification_channel:Arc<Notify>,
        evaluation_metrics:Arc<EvaluationMetrics>,
    ) -> Self {
        
        let (scheduler_txs, mut  worker_rxs, scheduler_channel_rx , worker_txs ) = SchedulerEngine::create_channels(num_workers_to_start);

        // Create shared component cache (handles compilation, thread-safe)
        let component_cache = Arc::new(crate::wasm_loaders::ComponentCache::new("models".to_string()));

        // Create workers - each worker gets its own WasmComponentLoader instance which allows parallel execution without mutex contention
        // We also mount the models directory so ONNX models can be accessed
        let workers: Vec<Arc<Worker>> = core_ids[0..num_workers_to_start]
            .iter()
            .enumerate()
            .map(|(i, core_id)| {
                // Each worker gets its own WasmComponentLoader instance with its own Store (for parallelism)
                let worker_wasm_loader = Arc::new(Mutex::new(
                    crate::wasm_loaders::WasmComponentLoader::new("models".to_string(), &component_cache),
                ));
                Arc::new(Worker::new(
                    core_id.id,
                    *core_id,
                    worker_wasm_loader,
                    component_cache.clone(), // Share the cache with workers
                    worker_txs[i].clone(),
                    worker_rxs.remove(0),
                    workers_notification_channel.clone(),
                    evaluation_metrics.clone(),
                    num_concurrent_tasks,
                ))
            })
            .collect();

        SchedulerEngine {
            submitted_jobs,
            workers,
            scheduler_channel_rx,
            scheduler_txs: Arc::new(Mutex::new(scheduler_txs)),
            evaluation_metrics,
            component_cache: component_cache.clone(),
            pin_cores,
            worker_requests: Arc::new(Mutex::new(HashSet::new())),
            sequential_run_flag: Arc::new(Mutex::new(false)) ,
            active_workers: Arc::new(Mutex::new(Vec::new())),
            worker_thread_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }


    pub fn create_channels(num_workers_to_start: usize) -> (Vec<mpsc::Sender<(MessageType, Message)>>, Vec<mpsc::Receiver<(MessageType, Message)>>, Arc<Mutex<mpsc::Receiver<(MessageType, Message)>>>, Vec<mpsc::Sender<(MessageType, Message)>>) {
        // Create channels with 4 producers (workers) , 1 consumer (scheduler) to send messages from worker to scheduler
        // Create 4 channels woth scheduler as producer and workers as consumers
        let (worker_tx, scheduler_rx) = mpsc::channel::<(MessageType, Message)>(10000);
        let scheduler_channel_rx: Arc<Mutex<mpsc::Receiver<(MessageType, Message)>>> = Arc::new(Mutex::new(scheduler_rx));


        let mut scheduler_txs = Vec::new();
        let mut worker_rxs = Vec::new();

        for _ in 0..num_workers_to_start {
            let (tx, rx) = mpsc::channel::<(MessageType, Message)>(1000);
            scheduler_txs.push(tx);
            worker_rxs.push(rx);
        }

        // Create vector of cloned senders for workers (each worker gets a clone)
        let worker_txs: Vec<mpsc::Sender<(MessageType, Message)>> = (0..num_workers_to_start)
            .map(|_| worker_tx.clone())
            .collect();

        return (scheduler_txs, worker_rxs,  scheduler_channel_rx , worker_txs )

    }

    pub async fn start_workers(&mut self) -> Result<Vec<std::thread::JoinHandle<()>>, Error> {
        // This function is used to start the workers 
        // It pins the worker's main thread to its assigned core if pin_cores is true.
        // It creates a local runtime just for this thread and runs the worker's async logic on this pinned thread.
        // Finally, it pushes the handler to the handlers vector and returns the handlers vector.

        let pin_cores = self.pin_cores.clone();

        let mut handlers: Vec<std::thread::JoinHandle<()>> = vec![];
        // start all workers
        for worker in &self.workers {
            let worker = worker.clone();
            let pin_cores_clone = pin_cores.clone();
            let mut w = self.active_workers.lock().await;
            w.push(worker.worker_id);
            // This blocks the thread and pins it to the core
            let handler: std::thread::JoinHandle<()> = std::thread::spawn(move || {
                // Pin worker thread to its assigned core
                if pin_cores_clone {
                    let res = core_affinity::set_for_current(worker.core_id);
                    if res {
                        // Worker pinned to core
                    } else {
                        eprintln!(
                            "Failed to pin worker {} to core {}",
                            worker.core_id.id, worker.core_id.id
                        );
                    }
                }

                // Create a local runtime just for this thread
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                // Run the worker's async logic on this pinned thread
                rt.block_on(async {
                    worker.start().await;
                });
            });
            handlers.push(handler);
        }

        // Store the handles in the scheduler for later use
        // We need to move them, so we'll store them and return a reference
        // Actually, we can't return moved values. Let's store them and return empty vec
        // But main.rs might need them... Let's check if we can change the approach
        let mut stored_handles = self.worker_thread_handles.lock().await;
        for handler in handlers {
            stored_handles.push(Some(handler));
        }
        drop(stored_handles);

        // Return empty vec since we stored them internally
        // Main.rs doesn't seem to use the return value anyway
        Ok(vec![])
    }

    pub async fn decode_scheduler_message(&self, message: &Message) -> Result<ToSchedulerMessage, anyhow::Error> {
        let message_from_worker = serde_json::from_str::<ToSchedulerMessage>(&message.message);
        let message_from_worker:ToSchedulerMessage = match message_from_worker{
            Ok(msg)=> {
                msg
            },
            Err(_) => {
                return Err(anyhow::Error::msg("failed to deserialize worker message"));
            }
        };
        Ok(message_from_worker)
    }

    pub async fn decode_task_status_message(&self, message: &Message)-> Result<TaskStatusMessage, anyhow::Error>{
        let task_status = serde_json::from_str::<TaskStatusMessage>(&message.message);
        let task_status: TaskStatusMessage = match task_status {
            Ok(msg) => {
                msg
            },
            Err(e) => {
                return Err(anyhow::Error::msg(format!("failed to deserialize task status message: {}", e)));
            }
        };
        Ok(task_status)
    }

    pub async fn  find_compatible_job(&self, msg: &ToSchedulerMessage)->Option<Job>{
        let sequential_run_flag = *self.sequential_run_flag.lock().await;
        let memory_capacity = msg.memory_capacity;
        let job = match msg.job_type {
            JobType::IOBound => {
                self.submitted_jobs.get_next_io_bounded_job(memory_capacity, sequential_run_flag).await
            }
            JobType::CPUBound => {
                self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity, sequential_run_flag).await
            }
            JobType::Mixed => {
                // Try IO-bound first, then CPU-bound, then any job
                if let Some(job) =
                    self.submitted_jobs.get_next_io_bounded_job(memory_capacity, sequential_run_flag).await
                {
                    Some(job)
                } else if let Some(job) =
                    self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity, sequential_run_flag).await
                {
                    Some(job)
                } else {
                    self.submitted_jobs.get_next_job(memory_capacity, sequential_run_flag).await
                }
            }
            JobType::None=> None
        };
        job
    }

    /// Find a compatible job with fallback logic:
    /// - If IOBound requested but not found, try CPUBound
    /// - If CPUBound requested but not found, try IOBound
    /// - For Mixed, use the existing logic (try both types)
    /// - For None, return None
    pub async fn find_compatible_job_with_fallback(&self, msg: &ToSchedulerMessage) -> Option<Job> {
        let sequential_run_flag = *self.sequential_run_flag.lock().await;
        let memory_capacity = msg.memory_capacity;
        
        match msg.job_type {
            JobType::IOBound => {
                // Try IOBound first
                if let Some(job) = self.submitted_jobs.get_next_io_bounded_job(memory_capacity, sequential_run_flag).await {
                    Some(job)
                } else {
                    // Fallback to CPUBound
                    self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity, sequential_run_flag).await
                }
            }
            JobType::CPUBound => {
                // Try CPUBound first
                if let Some(job) = self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity, sequential_run_flag).await {
                    Some(job)
                } else {
                    // Fallback to IOBound
                    self.submitted_jobs.get_next_io_bounded_job(memory_capacity, sequential_run_flag).await
                }
            }
            JobType::Mixed => {
                // Try IO-bound first, then CPU-bound, then any job
                if let Some(job) = self.submitted_jobs.get_next_io_bounded_job(memory_capacity, sequential_run_flag).await {
                    Some(job)
                } else if let Some(job) = self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity, sequential_run_flag).await {
                    Some(job)
                } else {
                    self.submitted_jobs.get_next_job(memory_capacity, sequential_run_flag).await
                }
            }
            JobType::None => None
        }
    }

    pub async fn encode_response(&self, job:Option<Job>, to_scheduler_msg:&ToSchedulerMessage)-> Message{
        match job{
            Some(this_job)=> {
                // Only send job if execution has started
                if self.evaluation_metrics.get_execution_start_time().is_none() {
                    // Execution not started yet, don't send job - it would be skipped and lost
                    // Re-add job to the list (it was already removed, so we need to add it back)
                    self.submitted_jobs.add_task(this_job.clone()).await;
                    // Return NotFound so worker can retry later
                    let to_worker_msg = ToWorkerMessage {
                        status: JobAskStatus::NotFound,
                        job: Job {
                            id: String::new(),
                            binary_path: String::new(),
                            func_name: String::new(),
                            payload: String::new(),
                            payload_compressed: false,
                            folder_to_mount: String::new(),
                            status: String::new(),
                            arrival_time: std::time::SystemTime::now(),
                            memory_prediction: None,
                            execution_time_prediction: None,
                            task_bound_type: None,
                        },
                        job_type: to_scheduler_msg.job_type.clone(),
                    };
                    let response_json = serde_json::to_string(&to_worker_msg).unwrap();
                    Message {
                        message: response_json,
                        message_type: MessageType::ToWorkerMessage,
                    }
                } else {
                    let to_worker_msg = ToWorkerMessage{
                                status: JobAskStatus::Found,
                                job:this_job,
                                job_type: to_scheduler_msg.job_type.clone()};
                    let response_json = serde_json::to_string(&to_worker_msg).unwrap();
                    let response_message = Message {
                        message: response_json,
                        message_type: MessageType::ToWorkerMessage,
                    };
                    response_message
                }
            },
            None=>{
                {                    
                    let to_worker_msg = ToWorkerMessage {
                        status: JobAskStatus::NotFound,
                        job: Job {
                            id: String::new(),
                            binary_path: String::new(),
                            func_name: String::new(),
                            payload: String::new(),
                            payload_compressed: false,
                            folder_to_mount: String::new(),
                            status: String::new(),
                            arrival_time: std::time::SystemTime::now(),
                            memory_prediction: None,
                            execution_time_prediction: None,
                            task_bound_type: None,
                        },
                        job_type: to_scheduler_msg.job_type.clone(),
                    };
                    let response_json = serde_json::to_string(&to_worker_msg).unwrap();
                    let response_message = Message {
                    message: response_json,
                    message_type: MessageType::ToWorkerMessage,
                    };
                    response_message
                }
            }
        }
    }

    pub async fn send_message(&self, worker_ids:&[usize], response_message:&Message)->Result<(), Error>{

        let scheduler_txs = self.scheduler_txs.lock().await;
        for id in worker_ids.iter(){
            if let Err(e) = scheduler_txs[*id].send((MessageType::ToWorkerMessage, response_message.clone())).await {
                eprintln!("Scheduler: Unable to send to worker {}: {}", id, e);
            }
        }
        // Lock guard will be dropped naturally when it goes out of scope
        Ok(())
    }


    pub async fn check_for_termination(&self)->bool{
        
        // Check SubmittedJobs termination condition
        // This handles both cases:
        // 1. Jobs list is empty and all jobs completed (removed from list)
        // 2. Jobs list is not empty but all jobs are in successful or failed sets
        if !self.submitted_jobs.check_for_termination().await {
            return false;
        }

        // Also verify evaluation metrics indicate completion
        // Evaluation metrics are reset when a new batch starts, so they're always fresh
        let all_completed = self.evaluation_metrics.are_all_tasks_completed().await;
        all_completed
    }

    pub async fn create_termination_message(&self)->Result<Message, anyhow::Error>{
        let terminate_msg = ToWorkerMessage {
            status: JobAskStatus::Terminate,
            job: Job {
                id: String::new(),
                binary_path: String::new(),
                func_name: String::new(),
                payload: String::new(),
                payload_compressed: false,
                folder_to_mount: String::new(),
                status: String::new(),
                arrival_time: std::time::SystemTime::now(),
                memory_prediction: None,
                execution_time_prediction: None,
                task_bound_type: None,
            },
            job_type: JobType::None,
        };
        
        let terminate_json = match serde_json::to_string(&terminate_msg) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Scheduler: Failed to serialize termination message: {}", e);
                return Err(anyhow::Error::msg("Serialization error"));
            }
        };
        let terminate_message = Message {
            message: terminate_json,
            message_type: MessageType::ToWorkerMessage,
        };
        Ok(terminate_message)
    }

    pub async fn send_termination_message(&self, worker_ids:Vec<usize>)->Result<(), Error>{
        let terminate_message = self.create_termination_message().await;
        if let Ok(tm) = terminate_message{
            let _res = self.send_message(&worker_ids, &tm).await;
        }
        Ok(())
    }

    pub async fn broadcast_termination_message(&self)->Result<(), Error>{
        let terminate_message = self.create_termination_message().await;
        if let Ok(tm) = terminate_message{
            let worker_ids:Vec<usize> = self.active_workers.lock().await.iter().map(|w| *w).collect();
            let _res = self.send_message(&worker_ids, &tm).await;
        }
        Ok(())
    }

    pub async fn handle_incoming_message(&self, message_type: MessageType, message: Message) -> Result<(), Error> {
        match message_type {
            MessageType::ToSchedulerMessage => {
            
                // self.submitted_jobs.print_status().await;
                let from_worker_message:ToSchedulerMessage = match self.decode_scheduler_message(&message).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Scheduler: Failed to decode scheduler message: {}", e);
                        return Err(e);
                    }
                };
                
                // In sequential mode, only allow active workers to get jobs
                let sequential_run_flag = *self.sequential_run_flag.lock().await;
                if sequential_run_flag {
                    let active_workers = self.active_workers.lock().await;
                    if !active_workers.contains(&from_worker_message.worker_id) {
                        // Worker is not active, send NotFound response so worker doesn't hang
                        let response_message = self.encode_response(None, &from_worker_message).await;
                        self.send_message(&[from_worker_message.worker_id], &response_message).await?;
                        return Ok(());
                    }
                    drop(active_workers);
                }
                let job_to_send = self.find_compatible_job_with_fallback(&from_worker_message).await;
                
                // If a job was found, add it to pending_job_ids and remove it from reschedule_job_ids
                if let Some(ref job) = job_to_send {
                    self.submitted_jobs.add_to_pending(&job.id).await;
                    let job_id = job.id.clone();
                    
                    // Remove from reschedule_job_ids if it's there (we're retrying it)
                    let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                    reschedule.remove(&job_id);
                    drop(reschedule);
                }
                else {
                    // No job found - move next job that is not in pending to reschedule
                    self.submitted_jobs.move_next_job_to_reschedule().await;
                }
                
                // Always send a response to the worker (either Found or NotFound)
                let response_message = self.encode_response(job_to_send, &from_worker_message).await;
                self.send_message(&[from_worker_message.worker_id], &response_message).await?;
            }
            MessageType::TaskStatusMessage=> {
                let task_status_message = self.decode_task_status_message(&message).await.unwrap();
                let sequential_run_flag = *self.sequential_run_flag.lock().await;
                match task_status_message.status {
                    crate::channel_objects::Status::Success => {
                        // Using status=1 to indicate success
                        self.evaluation_metrics.set_task_status(task_status_message.job_id.clone(), 1).await;
                        
                        // If sequential mode is active, increment the sequential successful count
                        if sequential_run_flag {
                            self.evaluation_metrics.increment_sequential_successful().await;
                        }
                        
                        // Remove from pending_job_ids and add to successfull_job_ids
                        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                        pending.remove(&task_status_message.job_id);
                        drop(pending);
                        self.submitted_jobs.add_to_succesfull(&task_status_message.job_id).await;
                        self.submitted_jobs.remove_job(task_status_message.job_id.clone()).await;
                        self.remove_from_reschedule(&task_status_message.job_id).await;
                    },
                    crate::channel_objects::Status::Failed => {
                        // Update evaluation metrics: mark task as completed (failed)
                        // Use status=2 to indicate failure (1=success, 2=failed)
                        self.evaluation_metrics.set_task_status(task_status_message.job_id.clone(), 2).await;
                        
                        // Remove from pending_job_ids first
                        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                        pending.remove(&task_status_message.job_id);
                        drop(pending);
                        
                        // Remove from reschedule
                        // Check if job is in reschedule_job_ids before removing it
                        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                        let was_in_reschedule = reschedule.contains(&task_status_message.job_id);
                        reschedule.remove(&task_status_message.job_id);
                        drop(reschedule);
                        
                        if sequential_run_flag {
                            // If sequential_run_flag is true, only add to failed_job_ids if it's NOT in reschedule
                            // If it's in reschedule, skip adding to failed because it will be retried
                            if !was_in_reschedule {
                                self.submitted_jobs.add_to_failed(&task_status_message.job_id).await;
                                // Remove from jobs list when it goes to failed_job_ids
                                self.submitted_jobs.remove_job(task_status_message.job_id.clone()).await;
                            }
                        } else {
                            // If sequential_run_flag is false, add to reschedule_job_ids
                            let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                            reschedule.insert(task_status_message.job_id.clone());
                            drop(reschedule);
                        }
                    }
                }
            },
            MessageType::ToWorkerMessage => {
                // Unexpected message type
            }
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) {
        for worker in &self.workers {
            worker.shutdown().await;
        }
    }

    async fn remove_from_reschedule(&self, job_id: &str) {
        // Check if the job ID is in reschedule_job_ids and remove it if present
        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
        reschedule.remove(job_id);
    }


    pub async fn reset_for_new_batch(&self) {
        self.evaluation_metrics.reset().await;
        
        // Reset sequential run flag
        let mut sequential_flag = self.sequential_run_flag.lock().await;
        *sequential_flag = false;
        drop(sequential_flag);
        
        // Reset active workers to include only workers that are still alive (have thread handles)
        // Workers that were killed in sequential mode won't have thread handles anymore
        let mut active_workers = self.active_workers.lock().await;
        let handles = self.worker_thread_handles.lock().await;
        active_workers.clear();
        for (idx, worker) in self.workers.iter().enumerate() {
            // Only add worker if it still has a thread handle (thread is still running)
            if handles.get(idx).and_then(|h| h.as_ref()).is_some() {
                active_workers.push(worker.worker_id);
            }
        }
        drop(handles);
        drop(active_workers);
        
        // Reset worker requests
        let mut worker_requests = self.worker_requests.lock().await;
        worker_requests.clear();
        drop(worker_requests);
        
        // Reset SubmittedJobs state
        self.submitted_jobs.reset_submitted_jobs().await;

        for worker in self.workers.iter() {
            let config = load_config();
            let num_consurrent_tasks = config.num_concurrent_tasks.unwrap_or(1);
            worker.set_num_concurrent_tasks(num_consurrent_tasks).await;
        }
    }

    async fn move_pending_back_to_reschedule(&self){
        // Move all jobs from pending_job_ids back to reschedule_job_ids
        // This happens when sequential mode is activated, so these jobs can be retried sequentially
        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
        
        let jobs_to_move: Vec<String> = pending.iter().cloned().collect();
        
        // Remove from pending and add to reschedule
        for job_id in &jobs_to_move {
            pending.remove(job_id);
            reschedule.insert(job_id.clone());
        }
        
        drop(pending);
        drop(reschedule);
    }

    async fn move_all_jobs_to_reschedule(&self) {
        // Move all jobs on submitted jobs to reschedule
        // Only moves jobs that are not already successful or failed
        let jobs = self.submitted_jobs.get_jobs().await;
        
        // Collect job IDs that need to be moved (checking exclusion sets one at a time)
        let mut job_ids_to_move = Vec::new();
        for job in &jobs {
            let job_id = &job.id;
            
            // Check if job is successful (acquire lock, check, release)
            let in_successful = {
                let successfull = self.submitted_jobs.successfull_job_ids.lock().await;
                successfull.contains(job_id)
            };
            if in_successful {
                continue;
            }
            
            // Check if job has failed (acquire lock, check, release)
            let in_failed = {
                let failed = self.submitted_jobs.failed_job_ids.lock().await;
                failed.contains(job_id)
            };
            if in_failed {
                continue;
            }
            
            // Job is eligible to be moved
            job_ids_to_move.push(job_id.clone());
        }
        
        // Now perform the moves, acquiring locks only when needed
        for job_id in &job_ids_to_move {
            // Remove from pending if present
            {
                let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                pending.remove(job_id);
            }
            
            // Add to reschedule if not already there
            {
                let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                reschedule.insert(job_id.clone());
            }
        }
    }

    async fn activate_sequential_mode(&self) {
        // Check if sequential mode is already activated to prevent multiple calls
        let sequential_flag = *self.sequential_run_flag.lock().await;
        if sequential_flag {
            return;
        }
        
        println!("=== Transitioning to sequential run mode ===");
        
        // Kill workers 1, 2, 3 completely
        let workers_to_kill: Vec<usize> = (1..self.workers.len()).map(|i| self.workers[i].worker_id).collect();
        
        // First, send shutdown signal to workers
        for &worker_id in &workers_to_kill {
            if let Some(worker) = self.workers.get(worker_id) {
                worker.shutdown().await;
            }
        }
        
        // Wait a bit for workers to process shutdown
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // Now kill the worker threads completely by detaching them
        // We detach instead of joining because workers might be stuck in async operations (like notified().await)
        // Detaching allows the threads to finish on their own when they check the shutdown flag
        // The threads will exit when they reach the shutdown check in their loop
        let mut handles = self.worker_thread_handles.lock().await;
        for &worker_id in &workers_to_kill {
            handles.get_mut(worker_id).and_then(|h| h.take());
        }
        drop(handles);
        
        // Remove killed workers from active_workers
        let mut w = self.active_workers.lock().await;
        // Remove killed workers by value, not by index
        for worker_id in &workers_to_kill {
            w.retain(|&id| id != *worker_id);
        }
        // Ensure worker 0 is in active_workers (it's the only active worker in sequential mode)
        if !w.contains(&0) {
            w.push(0);
        }
        drop(w);

        // Set worker 0's num_concurrent_tasks to 1 for sequential execution
        if let Some(worker_0) = self.workers.get(0) {
            worker_0.set_num_concurrent_tasks(1).await;
        }

        self.move_pending_back_to_reschedule().await;

        let mut flag = self.sequential_run_flag.lock().await;
        *flag = true;
    }

    pub async fn start(&mut self) {
        // Start loop to receive messages from workers
        let mut rx = self.scheduler_channel_rx.lock().await;
        let mut counter = 0;
        loop {   
            // Receive message from a worker
            counter += 1;
            match rx.recv().await {
                Some((message_type, message)) => {
                    if let Err(e) = self.handle_incoming_message(message_type, message).await {
                        eprintln!("Scheduler: Error handling incoming message: {}", e);
                    }
                } 
                None => {
                    break;
                }
            }

            // Print periodic status every 100 iterations
            if counter % 100 == 0 {
                self.submitted_jobs.print_status_summary().await;
            }

            let all_in_reschedule = self.submitted_jobs.are_all_jobs_in_reschedule().await;
            let sequential_run_flag = *self.sequential_run_flag.lock().await;
        
            // If all jobs in reschedule activate sequential mode
            if !sequential_run_flag && all_in_reschedule {
                self.activate_sequential_mode().await;
            }

            // check for termination
            if self.check_for_termination().await {
                // Get failed and successful job counts to store in evaluation metrics
                let jobs_failed = self.submitted_jobs.get_failed_count().await;
                let jobs_succeeded = self.submitted_jobs.get_successful_count().await;
                let jobs_pending = self.submitted_jobs.get_pending_count().await;
                let jobs_rescheduled = self.submitted_jobs.get_reschedule_count().await;
                
                // Print final status
                println!("=== Final Status ===");
                println!("Successful: {}, Failed: {}, Pending: {}, Rescheduled: {}", 
                    jobs_succeeded, jobs_failed, jobs_pending, jobs_rescheduled);
                
                // Print evaluation metrics
                self.evaluation_metrics.all_tasks_completed_callback(jobs_failed, jobs_succeeded).await;
                
                self.broadcast_termination_message().await.unwrap();
                
                // Reset scheduler state for next batch
                self.reset_for_new_batch().await;
            }
        }
    }
}