use std::sync::{Arc, Mutex as StdMutex};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, mpsc};

use actix_web::web;
use tokio::time::error::Error;
use crate::{optimized_scheduling_preprocessing::scheduler_algorithms::{BaselineStaticSchedulerAlgorithm, MemoryTimeAwareSchedulerAlgorithm, SchedulerAlgorithm},worker::Worker};
use core_affinity::*;
use crate::api::api_objects::{SubmittedJobs, Job};


    // Standalone function to store evaluation metrics to file
    fn store_evaluation_metrics(total_tasks: usize, total_time_secs: f64, total_time_ms: f64, avg_time_ms: f64, throughput: f64) {
        let metrics_content = format!(
            "Total tasks processed: {}\nTotal execution time: {:.2} seconds ({:.2} ms)\nAverage time per task: {:.2} ms\nThroughput: {:.2} tasks/second\n",
            total_tasks, total_time_secs, total_time_ms, avg_time_ms, throughput
        );
        
        if let Err(e) = std::fs::write("results/evaluation_metrics.txt", metrics_content) {
            eprintln!("Failed to write evaluation metrics to file: {}", e);
        } else {
            println!("Evaluation metrics written to results/evaluation_metrics.txt");
        }
    }

    // ========== SCHEDULER ==========
    pub struct SchedulerEngine{
        submitted_jobs: web::Data<SubmittedJobs>,
        workers: Vec<Arc<Worker>>,
        // Shared channels: all workers pull from these
        io_bound_tx: mpsc::Sender<Job>, // Shared IO-bound channel sender
        cpu_bound_tx: mpsc::Sender<Job>, // Shared CPU-bound channel sender
        execution_notify: Arc<Notify>, // Signal to workers to start processing
        task_status: Arc<Mutex<HashMap<String, u8>>>, // HashMap: task_id -> 0 (not processed) or 1 (processed)
        completed_count: Arc<Mutex<usize>>, // Counter for completed tasks
        execution_start_time: Arc<Mutex<Option<std::time::Instant>>>, // Track when execution starts
        pin_cores: bool,
        cpu_bound_task_ids: Arc<Mutex<Vec<String>>>, // CPU-bound task IDs
        io_bound_task_ids: Arc<Mutex<Vec<String>>>,  // I/O-bound task IDs
        response_time_per_task: Arc<StdMutex<HashMap<String, Duration>>>, // Track response time for each task
    }

    impl SchedulerEngine{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>,  num_workers_to_start: usize, pin_cores: bool)->Self{
            
            // Create a notification mechanism to signal workers when to process tasks
            let execution_notify = Arc::new(Notify::new());
            
            // Create shared channels: one for IO-bound, one for CPU-bound tasks
            // All workers will pull from these shared channels
            let (io_bound_tx, io_bound_rx) = mpsc::channel::<Job>(10000); // Large buffer for many tasks
            let (cpu_bound_tx, cpu_bound_rx) = mpsc::channel::<Job>(10000);
            
            // Wrap receivers in Arc<Mutex> so all workers can share them
            let shared_io_rx = Arc::new(Mutex::new(io_bound_rx));
            let shared_cpu_rx = Arc::new(Mutex::new(cpu_bound_rx));
            
            // Create a single shared WasmComponentLoader instance wrapped in Arc<Mutex>
            // Mount the model_1 directory so ONNX models can be accessed
            let shared_wasm_loader = Arc::new(Mutex::new(crate::wasm_loaders::WasmComponentLoader::new("/home/pi/rise-thesis/models".to_string())));
            
            // Create workers - all share the same channel receivers
            let workers: Vec<Arc<Worker>> = core_ids[0..num_workers_to_start].iter().map(|core_id| {
                Arc::new(Worker::new(
                    core_id.id, 
                    *core_id, 
                    shared_wasm_loader.clone(), 
                    shared_io_rx.clone(),
                    shared_cpu_rx.clone(),
                    execution_notify.clone()
                ))
            }).collect();
            
            SchedulerEngine{
                submitted_jobs, 
                workers,
                io_bound_tx,
                cpu_bound_tx,
                execution_notify,
                task_status: Arc::new(Mutex::new(HashMap::new())),
                completed_count: Arc::new(Mutex::new(0)),
                execution_start_time: Arc::new(Mutex::new(None)),
                response_time_per_task: Arc::new(StdMutex::new(HashMap::new())),
                pin_cores,
                cpu_bound_task_ids: Arc::new(Mutex::new(Vec::new())),
                io_bound_task_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub async fn get_execution_start_time(&self) -> Option<std::time::Instant> {
            self.execution_start_time.lock().await.clone()
        }

        pub async fn start_scheduler(&mut self, scheduler_arc: Arc<Mutex<SchedulerEngine>>)-> Result<Vec<std::thread::JoinHandle<()>>, Error>{
            // Set scheduler reference in workers so they can notify on completion
            for worker in &self.workers {
                worker.set_scheduler(scheduler_arc.clone()).await;
            }
            
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

        async fn initialize_task_status(&mut self, jobs:&Vec<Job>){
            let mut task_status = self.task_status.lock().await;
            task_status.clear();
            for job in jobs.iter() {
                task_status.insert(job.id.clone(), 0); // 0 = not processed
            }
            drop(task_status);

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
            *self.execution_start_time.lock().await = Some(start_time);
            
            // Get all current tasks and initialize HashMap (all set to 0 = not processed)
            let jobs = self.submitted_jobs.get_jobs().await;

            self.initialize_task_status(&jobs).await;

            // Initialize completed counter
            *self.completed_count.lock().await = 0;
            
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

        // Get response_time_per_task Arc (for passing to workers)
        pub fn get_response_time_per_task(&self) -> Arc<StdMutex<HashMap<String, Duration>>> {
            self.response_time_per_task.clone()
        }

        // Synchronous method to set response time for a task (can be called from blocking context)
        pub fn set_response_time(&self, task_id: String, response_time: Duration) {
            let mut response_times = self.response_time_per_task.lock().unwrap();
            response_times.insert(task_id, response_time);
        }

        async fn set_task_status(&self, task_id: String, status: u8){
            let mut task_status = self.task_status.lock().await;
            task_status.insert(task_id, status);
            *self.completed_count.lock().await += 1;
        }

        pub async fn calculate_average_response_time(&self) -> f64 {
            let response_times = self.get_response_time_per_task();
            let response_times_map = response_times.lock().unwrap();
            if !response_times_map.is_empty() {
                response_times_map.values().sum::<Duration>().as_millis() as f64 / response_times_map.len() as f64
            } else {
                0.0
            }
        }

        async fn calculate_throughput(&self, completion_time: std::time::Instant, total_tasks: usize) -> f64 {
            let total_time_secs = completion_time - self.get_execution_start_time().await.unwrap();
            total_tasks as f64 / total_time_secs.as_secs_f64()
        }

        pub async fn on_task_completed(&self, task_id: String, completion_time: std::time::Instant) {
            // Called by workers when a task completes (push notification)
            self.set_task_status(task_id.clone(), 1).await;
            let completed_tasks_count = *self.completed_count.lock().await;
            let total_tasks = {
                let task_status = self.task_status.lock().await;
                task_status.len()
            };
            if completed_tasks_count>0 && completed_tasks_count == total_tasks {
                let throughput = self.calculate_throughput(completion_time, total_tasks).await;
                let average_response_time_millis = self.calculate_average_response_time().await;
                let total_time_duration: Duration = completion_time - self.get_execution_start_time().await.unwrap();
                println!("=== Execution completed ===");
                println!("Total tasks processed: {}", completed_tasks_count);
                println!("Total execution time: {:.2} seconds ({:.2} ms)", total_time_duration.as_secs(), total_time_duration.as_millis());
                println!("Average response time per task: {:.2} ms", average_response_time_millis);
                println!("Throughput: {:.2} tasks/second", throughput);
                
                // Store metrics to file
                store_evaluation_metrics(completed_tasks_count, total_time_duration.as_secs_f64(), total_time_duration.as_millis() as f64, average_response_time_millis, throughput);
                
                // Reset timing for next execution
                *self.execution_start_time.lock().await = None;
                self.task_status.lock().await.clear();
                *self.completed_count.lock().await = 0;
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


