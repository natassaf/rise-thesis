use std::sync::Arc;
use anyhow::Error;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::api::api_objects::{Job, SubmittedJobs};
use crate::channel_objects::{JobAskStatus, JobType, Message, ToSchedulerMessage, ToWorkerMessage};
use crate::evaluation_metrics::EvaluationMetrics;
use crate::{
    channel_objects::MessageType,
    optimized_scheduling_preprocessing::scheduler_algorithms::{
        BaselineStaticSchedulerAlgorithm, MemoryTimeAwareSchedulerAlgorithm, SchedulerAlgorithm,
    },
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
    workers_notification_channel: Arc<Notify>, // Signal to workers to start processing
    evaluation_metrics: Arc<EvaluationMetrics>, // Evaluation metrics storage
    pin_cores: bool,
}

impl SchedulerEngine {
    pub fn new(
        core_ids: Vec<CoreId>,
        submitted_jobs: web::Data<SubmittedJobs>,
        num_workers_to_start: usize,
        pin_cores: bool,
        num_concurrent_tasks: usize,
        workers_notification_channel:Arc<Notify>,
        evaluation_metrics:Arc<EvaluationMetrics>
    ) -> Self {
        // Create 4 producer channels to send ask message from worker to scheduler
        let (worker_tx, scheduler_rx) = mpsc::channel::<(MessageType, Message)>(10000);
        let scheduler_channel_rx = Arc::new(Mutex::new(scheduler_rx));

        // Create receiving channels to send the jobs and messages from scheduler to workers
        let mut scheduler_txs = Vec::new();
        let mut worker_rxs = Vec::new();

        for _ in 0..num_workers_to_start {
            let (tx, rx) = mpsc::channel::<(MessageType, Message)>(1000);
            scheduler_txs.push(tx);
            worker_rxs.push(rx);
        }

        // Create vector of cloned senders for workers (each worker gets a clone)
        let worker_txs: Vec<_> = (0..num_workers_to_start)
            .map(|_| worker_tx.clone())
            .collect();

        // Create workers - each worker gets its own WasmComponentLoader instance
        // This allows parallel execution without mutex contention
        // Mount the model_1 directory so ONNX models can be accessed
        let workers: Vec<Arc<Worker>> = core_ids[0..num_workers_to_start]
            .iter()
            .enumerate()
            .map(|(i, core_id)| {
                // Each worker gets its own WasmComponentLoader instance
                let worker_wasm_loader = Arc::new(Mutex::new(
                    crate::wasm_loaders::WasmComponentLoader::new("models".to_string()),
                ));
                Arc::new(Worker::new(
                    core_id.id,
                    *core_id,
                    worker_wasm_loader,
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
            workers_notification_channel,
            evaluation_metrics,
            pin_cores,
        }
    }

    pub async fn start_workers(&mut self) -> Result<Vec<std::thread::JoinHandle<()>>, Error> {
        // This function is used to start the scheduler and is called by the main function.
        // It pins the worker's main thread to its assigned core if pin_cores is true.
        // It creates a local runtime just for this thread and runs the worker's async logic on this pinned thread.
        // Finally, it pushes the handler to the handlers vector and returns the handlers vector.

        let pin_cores = self.pin_cores.clone();

        let mut handlers: Vec<std::thread::JoinHandle<()>> = vec![];
        // start all workers
        for worker in &self.workers {
            let worker = worker.clone();
            let pin_cores_clone = pin_cores.clone();

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
        // self.execute_jobs_continuously().await;
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

    // fix this
    pub async fn  find_compatible_job(&self, msg: &ToSchedulerMessage)->Option<Job>{
        let memory_capacity = msg.memory_capacity;
        let job = match msg.job_type {
            JobType::IOBound => {
                self.submitted_jobs.get_next_io_bounded_job(memory_capacity).await
            }
            JobType::CPUBound => {
                self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity).await
            }
            JobType::Mixed => {
                // Try IO-bound first, then CPU-bound, then any job
                if let Some(job) =
                    self.submitted_jobs.get_next_io_bounded_job(memory_capacity).await
                {
                    Some(job)
                } else if let Some(job) =
                    self.submitted_jobs.get_next_cpu_bounded_job(memory_capacity).await
                {
                    Some(job)
                } else {
                    self.submitted_jobs.get_next_job(memory_capacity).await
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
        let all_completed = self.evaluation_metrics.are_all_tasks_completed().await;
        all_completed
    }

    pub async fn send_termination_message(&self)->Result<(), Error>{
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
        let worker_ids:Vec<usize> = self.workers.iter().map(|w| w.worker_id).collect();
        let _res = self.send_message(&worker_ids, &terminate_message).await;
        Ok(())
    }

    pub async fn handle_incoming_message(&self, message_type: MessageType, message: Message) -> Result<(), Error> {
        match message_type {
            MessageType::ToSchedulerMessage => {
                let from_worker_message:ToSchedulerMessage = self.decode_scheduler_message(&message).await.unwrap();
                let job_to_send:Option<Job> = self.find_compatible_job(&from_worker_message).await;
                let response_message = self.encode_response(job_to_send, &from_worker_message).await;
                self.send_message(&[from_worker_message.worker_id], &response_message).await?;
            }
            MessageType::ToWorkerMessage => {
                println!("Scheduler: Received unexpected ToWorkerMessage");

            }
        }
        // Only check for termination after handling a message (not when no job is found)
        // This ensures we don't terminate prematurely when jobs are still in-flight with workers
        let should_terminate = self.check_for_termination().await;
        if should_terminate {
            let completed_count = self.evaluation_metrics.get_completed_count().await;
            let total_tasks = self.evaluation_metrics.get_total_tasks().await;
            println!("Scheduler: Termination check - completed: {}, total: {}, match: {}", 
                     completed_count, total_tasks, completed_count == total_tasks);
            println!("Scheduler: All tasks completed! Sending termination messages to all workers...");


            self.send_termination_message().await?;
            
            self.evaluation_metrics.reset().await;
        }
        Ok(())
    }

    pub async fn initialize_execution(&self) {
        // Record start time
        let start_time = std::time::Instant::now();
        self.evaluation_metrics.set_execution_start_time(start_time);

        let jobs = self.submitted_jobs.get_jobs().await;



        // Verify initialization worked
        let total_after_init = self.evaluation_metrics.get_total_tasks().await;
        println!(
            "=== Execution started at {:?} with {} tasks (verified: {} in metrics) ===",
            start_time,
            jobs.len(),
            total_after_init
        );
    }

    pub async fn start(&mut self) {
        // Initialize execution (will be called again when handle_execute_tasks is called with new jobs)
        self.initialize_execution().await;

        // Start loop to receive messages from workers
        let mut rx = self.scheduler_channel_rx.lock().await;
        let mut counter = 0;
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
        }
    }

    pub fn get_evaluation_metrics(&self) -> Arc<EvaluationMetrics> {
        self.evaluation_metrics.clone()
    }

    pub async fn shutdown(&mut self) {
        println!("Shutting down scheduler and all workers...");
        for worker in &self.workers {
            worker.shutdown().await;
        }
    }

    // pub async fn execute_jobs_continuously(&mut self)->Result<(), Error>{
    //     loop {
    //         println!("Checking for new tasks");
    //         println!("Num of submitted tasks: {:?}", self.submitted_jobs.get_num_tasks().await);
    //         self.scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;
    //         tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    //     }
    // }

}