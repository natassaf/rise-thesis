use std::sync::Arc;
use tokio::sync::{Mutex, Notify, mpsc};

use actix_web::web;
use tokio::time::error::Error;
use crate::{optimized_scheduling_preprocessing::scheduler_algorithms::{BaselineStaticSchedulerAlgorithm, MemoryTimeAwareSchedulerAlgorithm, SchedulerAlgorithm},worker::Worker};
use core_affinity::*;
use crate::api::api_objects::{SubmittedJobs, Job};
use crate::evaluation_metrics::EvaluationMetrics;

    // ========== SCHEDULER ==========
    pub struct SchedulerEngine{
        submitted_jobs: web::Data<SubmittedJobs>,
        workers: Vec<Arc<Worker>>,
        // Shared channels: all workers pull from these
        io_bound_tx: mpsc::Sender<Job>, // Shared IO-bound channel sender
        cpu_bound_tx: mpsc::Sender<Job>, // Shared CPU-bound channel sender
        execution_notify: Arc<Notify>, // Signal to workers to start processing
        evaluation_metrics: Arc<EvaluationMetrics>, // Evaluation metrics storage
        pin_cores: bool,
        cpu_bound_task_ids: Arc<Mutex<Vec<String>>>, // CPU-bound task IDs
        io_bound_task_ids: Arc<Mutex<Vec<String>>>,  // I/O-bound task IDs
    }

    impl SchedulerEngine{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>,  num_workers_to_start: usize, pin_cores: bool)->Self{
            
            // Create a notification mechanism to signal workers when to process tasks
            let execution_notify = Arc::new(Notify::new());
            
            // Create shared evaluation metrics
            let evaluation_metrics = Arc::new(EvaluationMetrics::new());
            
            // Create shared channels: one for IO-bound, one for CPU-bound tasks
            // All workers will pull from these shared channels
            let (io_bound_tx, io_bound_rx) = mpsc::channel::<Job>(10000); // Large buffer for many tasks
            let (cpu_bound_tx, cpu_bound_rx) = mpsc::channel::<Job>(10000);
            
            // Wrap receivers in Arc<Mutex> so all workers can share them
            let shared_io_rx = Arc::new(Mutex::new(io_bound_rx));
            let shared_cpu_rx = Arc::new(Mutex::new(cpu_bound_rx));
            
            // Create workers - each worker gets its own WasmComponentLoader instance
            // This allows parallel execution without mutex contention
            // Mount the model_1 directory so ONNX models can be accessed
            let workers: Vec<Arc<Worker>> = core_ids[0..num_workers_to_start].iter().map(|core_id| {
                // Each worker gets its own WasmComponentLoader instance
                let worker_wasm_loader = Arc::new(Mutex::new(crate::wasm_loaders::WasmComponentLoader::new("/home/pi/rise-thesis/models".to_string())));
                
                Arc::new(Worker::new(
                    core_id.id, 
                    *core_id, 
                    worker_wasm_loader, 
                    shared_io_rx.clone(),
                    shared_cpu_rx.clone(),
                    execution_notify.clone(),
                    evaluation_metrics.clone()
                ))
            }).collect();
            
            SchedulerEngine{
                submitted_jobs, 
                workers,
                io_bound_tx,
                cpu_bound_tx,
                execution_notify,
                evaluation_metrics,
                pin_cores,
                cpu_bound_task_ids: Arc::new(Mutex::new(Vec::new())),
                io_bound_task_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub async fn start_scheduler(&mut self)-> Result<Vec<std::thread::JoinHandle<()>>, Error>{
            // Capture baseline value before the closure
            let pin_cores = self.pin_cores.clone();
            
            let mut handlers:Vec<std::thread::JoinHandle<()>> = vec![];
            // start all workers
            for worker in &self.workers {
                let worker = worker.clone();
                let pin_cores_clone = pin_cores.clone();
                
                // This blocks the thread and pins it to the core
                let handler: std::thread::JoinHandle<()> = std::thread::spawn(move || {
                    
                    // Pin worker thread to its assigned core
                    if pin_cores_clone{
                        let res = core_affinity::set_for_current(worker.core_id);
                        if res {
                            println!("Worker {} main thread pinned to core {}", worker.core_id.id, worker.core_id.id);
                        } else {
                            eprintln!("Failed to pin worker {} to core {}", worker.core_id.id, worker.core_id.id);
                        }
                    }
                    
                    // Create a local runtime just for this thread
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    // Run the worker's async logic on this pinned thread
                    rt.block_on(async move {
                        worker.start().await;
                    });
                });
                handlers.push(handler);
            }
            
            Ok(handlers)
            // self.execute_jobs_continuously().await;
        }
        pub async fn predict_and_sort(&mut self, scheduling_algorithm: String){
            // Create the appropriate scheduler algorithm based on the request
            let scheduler_algo: Box<dyn SchedulerAlgorithm> = match scheduling_algorithm.as_str() {
                "memory_time_aware" => Box::new(MemoryTimeAwareSchedulerAlgorithm::new()),
                "baseline" => Box::new(BaselineStaticSchedulerAlgorithm::new()),
                _ => panic!("Invalid scheduling algorithm: {}. Must be 'memory_time_aware' or 'baseline'", scheduling_algorithm),
            };
            
            println!("=== Starting predictions and sorting with {} algorithm ===", scheduling_algorithm);
            
            // Schedule tasks using the selected algorithm (does predictions and sorting)
            let (io_bound_task_ids, cpu_bound_task_ids) = scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;
            
            // Store the task ID sets in SchedulerEngine
            let mut stored_cpu_bound = self.cpu_bound_task_ids.lock().await;
            let mut stored_io_bound = self.io_bound_task_ids.lock().await;
            *stored_cpu_bound = cpu_bound_task_ids.clone();
            *stored_io_bound = io_bound_task_ids.clone();
            
            // Also store in SubmittedJobs so workers can access them
            self.submitted_jobs.set_cpu_bound_task_ids(cpu_bound_task_ids.clone()).await;
            self.submitted_jobs.set_io_bound_task_ids(io_bound_task_ids.clone()).await;
            
            println!("=== Predictions and sorting completed ===");
            println!("=== Separated tasks: {} CPU-bound, {} I/O-bound ===", cpu_bound_task_ids.len(), io_bound_task_ids.len());
        }

        pub async fn execute_jobs(&mut self){
            // Record start time
            let start_time = std::time::Instant::now();
            self.evaluation_metrics.set_execution_start_time(start_time);
            
            // Get all current tasks and initialize HashMap (all set to 0 = not processed)
            let jobs = self.submitted_jobs.get_jobs().await;

            // Initialize task status in evaluation metrics
            let task_ids: Vec<String> = jobs.iter().map(|j| j.id.clone()).collect();
            self.evaluation_metrics.initialize_task_status(task_ids).await;
            
            println!("=== Execution started at {:?} with {} tasks ===", start_time, jobs.len());
            
            // Distribute tasks to shared channels - workers will pull when free
            let io_bound_ids = self.io_bound_task_ids.lock().await.clone();
            let cpu_bound_ids = self.cpu_bound_task_ids.lock().await.clone();
            
            let mut io_count = 0;
            let mut cpu_count = 0;
            
            for job in jobs.iter() {
                let is_io_bound = io_bound_ids.contains(&job.id);
                let is_cpu_bound = cpu_bound_ids.contains(&job.id);
                
                if is_io_bound {
                    // Send to shared IO-bound channel - any free worker will pull it
                    if let Err(e) = self.io_bound_tx.send(job.clone()).await {
                        eprintln!("Failed to send IO-bound job to channel: {:?}", e);
                    } else {
                        io_count += 1;
                    }
                } else if is_cpu_bound {
                    // Send to shared CPU-bound channel - any free worker will pull it
                    if let Err(e) = self.cpu_bound_tx.send(job.clone()).await {
                        eprintln!("Failed to send CPU-bound job to channel: {:?}", e);
                    } else {
                        cpu_count += 1;
                    }
                } else {
                    // Default: send to IO-bound channel
                    if let Err(e) = self.io_bound_tx.send(job.clone()).await {
                        eprintln!("Failed to send job to channel: {:?}", e);
                    } else {
                        io_count += 1;
                    }
                }
            }
            
            println!("=== Published {} tasks to channels ({} IO-bound, {} CPU-bound) ===", 
                     jobs.len(), io_count, cpu_count);
            
            // Notify all workers to start processing tasks
            self.execution_notify.notify_waiters();
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


