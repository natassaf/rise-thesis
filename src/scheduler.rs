use std::ops::Add;
use std::sync::Arc;
use std::collections::{HashSet, HashMap};
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
    pin_cores: bool,
    worker_requests: Arc<Mutex<HashSet<usize>>>, // Track which workers have sent job requests
    sequential_run_flag: Arc<Mutex<bool>>, // when true run all remaining jobs on one worker
    active_workers: Arc<Mutex<Vec<usize>>>,
    consecutive_not_found_count: Arc<Mutex<HashMap<usize, usize>>>, // Track consecutive NotFound responses per worker
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
            pin_cores,
            worker_requests: Arc::new(Mutex::new(HashSet::new())),
            sequential_run_flag: Arc::new(Mutex::new(false)) ,
            active_workers: Arc::new(Mutex::new(Vec::new())),
            consecutive_not_found_count: Arc::new(Mutex::new(HashMap::new())),
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
                        println!(
                            "Worker {} main thread pinned to core {}",
                            worker.core_id.id, worker.core_id.id
                        );
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

        Ok(handlers)
    }

    pub async fn decode_scheduler_message(&self, message: &Message) -> Result<ToSchedulerMessage, anyhow::Error> {
        let message_from_worker = serde_json::from_str::<ToSchedulerMessage>(&message.message);
        let message_from_worker:ToSchedulerMessage = match message_from_worker{
            Ok(msg)=> {
                println!(
                    "Scheduler: Received task request from worker {} (job_type: {:?}, memory: {})",
                    msg.worker_id,
                    msg.job_type,
                    msg.memory_capacity
                );
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
                println!(
                    "Scheduler: Received task status from worker {} for job {} (status: {:?})",
                    msg.worker_id,
                    msg.job_id,
                    msg.status
                );
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

    pub async fn encode_response(&self, job:Option<Job>, to_scheduler_msg:&ToSchedulerMessage)-> Message{
        match job{
            Some(this_job)=> {
                // Only send job if execution has started
                if self.evaluation_metrics.get_execution_start_time().is_none() {
                    // Execution not started yet, don't send job - it would be skipped and lost
                    println!("Scheduler: Execution not started yet, re-adding job {} to avoid loss", this_job.id);
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
                    // Job found - reset consecutive NotFound count for this worker
                    let worker_id = to_scheduler_msg.worker_id;
                    let mut not_found_count = self.consecutive_not_found_count.lock().await;
                    not_found_count.insert(worker_id, 0); // Reset count
                    drop(not_found_count);
                    
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
                    // No job found - just return NotFound
                    // Increment consecutive NotFound count for this worker
                    let worker_id = to_scheduler_msg.worker_id;
                    let mut not_found_count = self.consecutive_not_found_count.lock().await;
                    let count = not_found_count.entry(worker_id).or_insert(0);
                    *count += 1;
                    println!("Scheduler: Worker {} consecutive NotFound count: {}", worker_id, *count);
                    drop(not_found_count);
                    
                    // Don't check for termination here because jobs might still be in-flight with workers
                    // Termination will be checked after task completion in handle_incoming_message
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
            println!("Scheduler: Send message from worker: {}", id);
            scheduler_txs[*id].send((MessageType::ToWorkerMessage, response_message.clone())).await.unwrap_or_else(|e| eprintln!("Unable to send to {}", id));
        }
        drop(self.scheduler_txs.clone());
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
                self.submitted_jobs.print_status().await;
                let from_worker_message:ToSchedulerMessage = self.decode_scheduler_message(&message).await.unwrap();
                
                // In sequential mode, only allow active workers to get jobs
                let sequential_run_flag = *self.sequential_run_flag.lock().await;
                if sequential_run_flag {
                    let active_workers = self.active_workers.lock().await;
                    if !active_workers.contains(&from_worker_message.worker_id) {
                        // Worker is not active, don't send job
                        println!("Scheduler: Worker {} is not active (sequential mode), ignoring job request", from_worker_message.worker_id);
                        let response_message = self.encode_response(None, &from_worker_message).await;
                        self.send_message(&[from_worker_message.worker_id], &response_message).await?;
                        return Ok(());
                    }
                    drop(active_workers);
                }
                
                let job_to_send = self.find_compatible_job(&from_worker_message).await;
                
                // If a job was found, add it to pending_job_ids and remove it from reschedule_job_ids
                if let Some(ref job) = job_to_send {
                    let job_id = job.id.clone();
                    let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                    pending.insert(job_id.clone());
                    drop(pending);
                    
                    // Remove from reschedule_job_ids if it's there (we're retrying it)
                    let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                    if reschedule.remove(&job_id) {
                        println!("Scheduler: Removed job {} from reschedule_job_ids (retrying)", job_id);
                    }
                    drop(reschedule);
                    
                    println!("Scheduler: Added job {} to pending_job_ids (sending to worker {})", job_id, from_worker_message.worker_id);
                }
                
                let response_message = self.encode_response(job_to_send, &from_worker_message).await;
                self.send_message(&[from_worker_message.worker_id], &response_message).await?;
            }
            MessageType::TaskStatusMessage=> {
                let task_status_message = self.decode_task_status_message(&message).await.unwrap();
                let sequential_run_flag = *self.sequential_run_flag.lock().await;
                match task_status_message.status {
                    crate::channel_objects::Status::Success => {
                        // Update evaluation metrics: mark task as completed (success)
                        // Note: Worker also sets this, but we set it here for consistency with failed jobs
                        // Using status=1 to indicate success
                        self.evaluation_metrics.set_task_status(task_status_message.job_id.clone(), 1).await;
                        
                        // Remove from pending_job_ids and add to successfull_job_ids
                        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                        let was_in_pending = pending.remove(&task_status_message.job_id);
                        drop(pending);
                        if was_in_pending {
                            println!("Scheduler: Task {} removed from pending_job_ids (success)", task_status_message.job_id);
                        } else {
                            println!("Scheduler: WARNING - Task {} was not in pending_job_ids when succeeded", task_status_message.job_id);
                        }
                        self.submitted_jobs.add_to_succesfull(&task_status_message.job_id).await;
                        self.submitted_jobs.remove_job(task_status_message.job_id.clone()).await;
                        self.remove_from_reschedule(&task_status_message.job_id).await;
                        println!("Scheduler: Task {} succeeded, moved to successful", task_status_message.job_id);
                    },
                    crate::channel_objects::Status::Failed => {
                        // Update evaluation metrics: mark task as completed (failed)
                        // Use status=2 to indicate failure (1=success, 2=failed)
                        self.evaluation_metrics.set_task_status(task_status_message.job_id.clone(), 2).await;
                        
                        // Remove from pending_job_ids first
                        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
                        let was_in_pending = pending.remove(&task_status_message.job_id);
                        drop(pending);
                        if was_in_pending {
                            println!("Scheduler: Task {} removed from pending_job_ids (failure)", task_status_message.job_id);
                        } else {
                            println!("Scheduler: WARNING - Task {} was not in pending_job_ids when failed", task_status_message.job_id);
                        }
                        
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
                                println!("Scheduler: Task {} failed, moved to failed_job_ids and removed from jobs list (sequential mode, not in reschedule)", task_status_message.job_id);
                            } else {
                                println!("Scheduler: Task {} failed but is in reschedule, will be retried (not added to failed_job_ids)", task_status_message.job_id);
                            }
                        } else {
                            // If sequential_run_flag is false, add to reschedule_job_ids
                            let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
                            reschedule.insert(task_status_message.job_id.clone());
                            drop(reschedule);
                            println!("Scheduler: Task {} failed, moved to reschedule_job_ids", task_status_message.job_id);
                        }
                    }
                }
            },
            MessageType::ToWorkerMessage => {
                println!("Scheduler: Received unexpected ToWorkerMessage");

            }
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) {
        println!("Shutting down scheduler and all workers...");
        for worker in &self.workers {
            worker.shutdown().await;
        }
    }

    async fn remove_from_reschedule(&self, job_id: &str) {
        // Check if the job ID is in reschedule_job_ids and remove it if present
        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
        if reschedule.remove(job_id) {
            println!("Scheduler: Removed job {} from reschedule_job_ids (job succeeded)", job_id);
        }
    }

    pub async fn reset_for_new_batch(&self) {
        // Reset all scheduler state variables for a new batch of tasks
        println!("Scheduler: Resetting state for new batch");
        
        // Completely reinitialize evaluation metrics to avoid stale state
        self.evaluation_metrics.reset().await;
        println!("Scheduler: Reset evaluation metrics");
        
        // Clear consecutive NotFound counts
        let mut not_found_count = self.consecutive_not_found_count.lock().await;
        not_found_count.clear();
        drop(not_found_count);
        
        // Reset sequential run flag
        let mut sequential_flag = self.sequential_run_flag.lock().await;
        *sequential_flag = false;
        drop(sequential_flag);
        
        // Reset active workers to include all workers
        let mut active_workers = self.active_workers.lock().await;
        active_workers.clear();
        for worker in &self.workers {
            active_workers.push(worker.worker_id);
        }
        println!("Scheduler: Reset active_workers to include all {} workers", active_workers.len());
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
        
        println!("Scheduler: State reset complete for new batch");
    }

    async fn move_pending_back_to_reschedule(&self){
        // Move all jobs from pending_job_ids back to reschedule_job_ids
        // This happens when sequential mode is activated, so these jobs can be retried sequentially
        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;
        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
        
        let jobs_to_move: Vec<String> = pending.iter().cloned().collect();
        let moved_count = jobs_to_move.len();
        
        // Remove from pending and add to reschedule
        for job_id in &jobs_to_move {
            pending.remove(job_id);
            reschedule.insert(job_id.clone());
        }
        
        drop(pending);
        drop(reschedule);
        
        if moved_count > 0 {
            println!("Scheduler: Moved {} job(s) from pending_job_ids back to reschedule_job_ids for sequential retry", moved_count);
        }
    }

    async fn check_job_not_found_status(&self, threshold: usize) -> bool {
        // Check if all active workers have consecutive_not_found_count greater than threshold
        let active_workers = self.active_workers.lock().await;
        let not_found_count = self.consecutive_not_found_count.lock().await;
        
        if active_workers.is_empty() {
            drop(active_workers);
            drop(not_found_count);
            return false;
        }
        
        let all_received_no_job = active_workers.iter().all(|&worker_id| {
            not_found_count.get(&worker_id).copied().unwrap_or(0) > threshold
        });
        
        drop(active_workers);
        drop(not_found_count);
        
        all_received_no_job
    }

    async fn move_all_jobs_to_reschedule(&self) {
        // Move all jobs on submitted jobs to reschedule
        // Only moves jobs that are not already successful or failed
        let jobs = self.submitted_jobs.get_jobs().await;
        let mut reschedule = self.submitted_jobs.reschedule_job_ids.lock().await;
        let successfull = self.submitted_jobs.successfull_job_ids.lock().await;
        let failed = self.submitted_jobs.failed_job_ids.lock().await;
        let mut pending = self.submitted_jobs.pending_job_ids.lock().await;

        let mut moved_count = 0;
        for job in &jobs {
            let job_id = &job.id;
            // Only move jobs that are not already successful or failed
            if !successfull.contains(job_id) && !failed.contains(job_id) {
                // Remove from pending if present
                pending.remove(job_id);
                // Add to reschedule if not already there
                if reschedule.insert(job_id.clone()) {
                    moved_count += 1;
                }
            }
        }

        drop(reschedule);
        drop(successfull);
        drop(failed);
        drop(pending);

        if moved_count > 0 {
            println!("Scheduler: Moved {} job(s) to reschedule_job_ids (all workers received no job)", moved_count);
        }
    }

    async fn activate_sequential_mode(&self) {
        // Terminate all workers except worker 0
        let workers_to_terminate: Vec<usize> = (1..self.workers.len()).map(|i| self.workers[i].worker_id).collect();
        if let Err(e) = self.send_termination_message(workers_to_terminate.clone()).await {
            eprintln!("Scheduler: Error sending termination message: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        // Remove terminated workers from active_workers
        let mut w = self.active_workers.lock().await;
        // Remove terminated workers by value, not by index
        for worker_id in &workers_to_terminate {
            w.retain(|&id| id != *worker_id);
        }
        // Ensure worker 0 is in active_workers (it's the only active worker in sequential mode)
        if !w.contains(&0) {
            w.push(0);
            println!("Scheduler: Added worker 0 to active_workers for sequential mode");
        }
        drop(w);

        // Set worker 0's num_concurrent_tasks to 1 for sequential execution
        if let Some(worker_0) = self.workers.get(0) {
            worker_0.set_num_concurrent_tasks(1).await;
            println!("Scheduler: Set worker 0's num_concurrent_tasks to 1 for sequential execution");
        }

        self.move_pending_back_to_reschedule().await;
        
        self.submitted_jobs.print_status().await;

        let mut flag = self.sequential_run_flag.lock().await;
        println!("Sequential flag set to true");
        *flag = true;
    }

    pub async fn start(&mut self) {
        // Start loop to receive messages from workers
        let mut rx = self.scheduler_channel_rx.lock().await;
        let mut counter = 0;
        let mut prepare_for_sequential_execution_flag = false;
        loop {   
            // Receive message from a worker
            println!("Iteration number: {}", counter);
            counter += 1;
            match rx.recv().await {
                Some((message_type, message)) => {
                    let _res = self.handle_incoming_message(message_type, message).await;
                } 
                None => {
                    println!("Scheduler: Channel closed, exiting message loop");
                    break;
                }
            }
            let all_in_reschedule = self.submitted_jobs.are_all_jobs_in_reschedule().await;
            let sequential_run_flag = *self.sequential_run_flag.lock().await;
        
            // Check if all workers received no job
            let all_workers_received_no_job = self.check_job_not_found_status(1).await;
            if all_workers_received_no_job {
                self.move_all_jobs_to_reschedule().await;
            }
            
            // Activate sequential mode if all jobs are in reschedule OR all workers received no job
            if (all_in_reschedule && sequential_run_flag == false) || all_workers_received_no_job {
                self.activate_sequential_mode().await;
            }
            

            // check for termination
            // Note: check_for_termination handles both cases: jobs in queue and jobs completed (removed from queue)
            if self.check_for_termination().await {
                let completed_count = self.evaluation_metrics.get_completed_count().await;
                let total_tasks = self.evaluation_metrics.get_total_tasks().await;
                println!("Scheduler: Termination check - completed: {}, total: {}, match: {}", 
                        completed_count, total_tasks, completed_count == total_tasks);
                
                // Print termination report
                self.submitted_jobs.termination_report().await;
                
                // Print evaluation metrics before resetting
                self.evaluation_metrics.all_tasks_completed_callback().await;
                
                // Print job status summary
                self.submitted_jobs.print_status().await;
                
                println!("Scheduler: All tasks completed! Sending termination messages to all workers...");
                self.broadcast_termination_message().await.unwrap();
                
                // Reset scheduler state for next batch
                self.reset_for_new_batch().await;
                
                // Don't break - continue the loop to wait for new tasks
                // The loop will continue and process new tasks when they arrive
                println!("Scheduler: Waiting for new tasks...");
                // Continue the loop instead of breaking
            }
        }
    }
}