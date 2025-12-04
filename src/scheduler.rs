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
    execution_notify: Arc<Notify>, // Signal to workers to start processing
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
    ) -> Self {
        // Create a notification mechanism to signal workers when to process tasks
        let execution_notify = Arc::new(Notify::new());

        // Create shared evaluation metrics
        let evaluation_metrics = Arc::new(EvaluationMetrics::new());

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
                    execution_notify.clone(),
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
            execution_notify,
            evaluation_metrics,
            pin_cores,
        }
    }

    pub async fn start_scheduler(&mut self) -> Result<Vec<std::thread::JoinHandle<()>>, Error> {
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

    // async fn store_task_ids_to_self(&mut self, io_bound_task_ids: Vec<String>, cpu_bound_task_ids: Vec<String>){
    //     *self.cpu_bound_task_ids.lock().await = cpu_bound_task_ids;
    //     *self.io_bound_task_ids.lock().await = io_bound_task_ids;
    // }

    pub async fn predict_and_sort(&mut self, scheduling_algorithm: String) {
        // Get the jobs from the submitted jobs vector and add them to the two channels for the workers to pull from and process
        // Create the appropriate scheduler algorithm based on the request
        let scheduler_algo: Box<dyn SchedulerAlgorithm> = match scheduling_algorithm.as_str() {
            "memory_time_aware" => Box::new(MemoryTimeAwareSchedulerAlgorithm::new()),
            "baseline" => Box::new(BaselineStaticSchedulerAlgorithm::new()),
            _ => panic!(
                "Invalid scheduling algorithm: {}. Must be 'memory_time_aware' or 'baseline'",
                scheduling_algorithm
            ),
        };

        println!(
            "=== Starting predictions and sorting with {} algorithm ===",
            scheduling_algorithm
        );

        // Schedule tasks using the selected algorithm (does predictions and sorting)
        let (io_bound_task_ids, cpu_bound_task_ids) =
            scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;

        // // Store the task ID sets in SchedulerEngine)
        // self.store_task_ids_to_self(io_bound_task_ids, cpu_bound_task_ids).await;

        // // Also store in SubmittedJobs so workers can access them
        self.submitted_jobs
            .set_cpu_bound_task_ids(cpu_bound_task_ids)
            .await;
        self.submitted_jobs
            .set_io_bound_task_ids(io_bound_task_ids)
            .await;

        // println!("=== Predictions and sorting completed ===");
        // println!("=== Separated tasks: {} CPU-bound, {} I/O-bound ===", self.cpu_bound_task_ids.lock().await.len(), self.io_bound_task_ids.lock().await.len());
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
        let job = match msg.job_type {
            JobType::IOBound => {
                self.submitted_jobs.get_next_io_bounded_job().await
            }
            JobType::CPUBound => {
                self.submitted_jobs.get_next_cpu_bounded_job().await
            }
            JobType::Mixed => {
                // Try IO-bound first, then CPU-bound, then any job
                if let Some(job) =
                    self.submitted_jobs.get_next_io_bounded_job().await
                {
                    Some(job)
                } else if let Some(job) =
                    self.submitted_jobs.get_next_cpu_bounded_job().await
                {
                    Some(job)
                } else {
                    self.submitted_jobs.get_next_job().await
                }
            }
            JobType::None=> None
        };
        job
    }

    pub async fn encode_response(&self, job:Option<Job>, to_scheduler_msg:&ToSchedulerMessage)-> Message{
        match job{
            Some(this_job)=> {
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
                },
            None=>{
                {
                    // Check if all tasks are completed
                    let to_worker_msg = if self.evaluation_metrics.are_all_tasks_completed().await{
                        println!("Sending termination message");
                        ToWorkerMessage {
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
                            job_type: to_scheduler_msg.job_type.clone(),
                        }
                    } else {
                        ToWorkerMessage {
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
                        }
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
        let should_terminate = self.check_for_termination().await;
        println!("Should terminate {}", should_terminate);
        if should_terminate{
            println!("Scheduler: All tasks completed! Sending termination messages to all workers...");
            // drop(scheduler_txs);
            // let scheduler_txs = self.scheduler_txs.lock().await;

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
            self.evaluation_metrics.reset().await;
        }
        Ok(())
    }

    pub async fn start(&mut self) {
        // Record start time
        let start_time = std::time::Instant::now();
        self.evaluation_metrics.set_execution_start_time(start_time);

        let jobs = self.submitted_jobs.get_jobs().await;
        let task_ids: Vec<String> = jobs.iter().map(|j| j.id.clone()).collect();
        println!(
            "Scheduler: Initializing task status with {} task IDs",
            task_ids.len()
        );
        self.evaluation_metrics
            .initialize_task_status(task_ids)
            .await;

        // Verify initialization worked
        let total_after_init = self.evaluation_metrics.get_total_tasks().await;
        println!(
            "=== Execution started at {:?} with {} tasks (verified: {} in metrics) ===",
            start_time,
            jobs.len(),
            total_after_init
        );

        // Notify all workers to start asking for jobs
        self.execution_notify.notify_waiters();
        println!("=== Notified workers to start requesting tasks ===");

        // Start loop to receive messages from workers
        let mut rx = self.scheduler_channel_rx.lock().await;
        let mut counter = 0;
        loop {
            // Receive message from a worker
            println!("Iteration number: {}", counter);
            counter += 1;
            match rx.recv().await {
                Some((message_type, message)) => {
                    // message_count += 1;
                    // if message_count % 10 == 0 {
                    //     let total_tasks = self.evaluation_metrics.get_total_tasks().await;
                    //     let completed_count = self.evaluation_metrics.get_completed_count().await;
                    //     let jobs_in_submitted = self.submitted_jobs.get_num_tasks().await;
                    //     println!(
                    //         "Scheduler: Total tasks in metrics: {}, Completed: {}, Jobs in submitted_jobs: {}",
                    //         total_tasks, completed_count, jobs_in_submitted
                    //     );
                    // }
                    let _res = self.handle_incoming_message(message_type, message).await;
                } 
                None => {
                    println!("Scheduler: Channel closed, exiting message loop");
                    break;
                }
            }
        }
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