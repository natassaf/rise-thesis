    use std::sync::Arc;
    use std::collections::HashMap;
    use tokio::sync::{Mutex, Notify};

    use actix_web::web;
    use tokio::{time::error::Error, task};
    use crate::{various::{Job, SubmittedJobs}, worker::Worker};
    use core_affinity::*;

    pub struct BaselineStaticSchedulerAlgorithm{
    }

    impl BaselineStaticSchedulerAlgorithm{
        pub fn new()->Self{
            Self{}
        }
        
        pub async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
            // Sort jobs by arrival time (oldest first) so workers process them in order
            let job_ids_before: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
            println!("Sorting jobs by arrival time before: {:?}", job_ids_before);

            self.sort_by_arrival_time(submitted_jobs).await;

            let job_ids_after: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
            println!("Sorting jobs by arrival time after: {:?}", job_ids_after);
        }

        // Sort jobs by arrival time (oldest first)
        async fn sort_by_arrival_time(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
            let mut jobs = submitted_jobs.jobs.lock().await;
            jobs.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
        }

    }

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
        scheduler_algo: BaselineStaticSchedulerAlgorithm,
        execution_notify: Arc<Notify>, // Signal to workers to start processing
        task_status: Arc<Mutex<HashMap<String, u8>>>, // HashMap: task_id -> 0 (not processed) or 1 (processed)
        completed_count: Arc<Mutex<usize>>, // Counter for completed tasks
        execution_start_time: Arc<Mutex<Option<std::time::Instant>>>, // Track when execution starts
        baseline: String, // Baseline scheduler type: "fifo" or "linux"
    }

    impl SchedulerEngine{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>,  num_workers_to_start: usize, baseline: String)->Self{
            
            // Create a notification mechanism to signal workers when to process tasks
            let execution_notify = Arc::new(Notify::new());
            
            // Create a single shared WasmComponentLoader instance wrapped in Arc<Mutex>
            // Mount the model_1 directory so ONNX models can be accessed
            let shared_wasm_loader = Arc::new(Mutex::new(crate::wasm_loaders::WasmComponentLoader::new("models".to_string())));
            
            // Create workers with the shared wasm_loader, reference to submitted_jobs, and execution_notify
            let workers: Vec<Arc<Worker>> = core_ids[0..num_workers_to_start].iter().map(|core_id| {
                Arc::new(Worker::new(core_id.id, *core_id, shared_wasm_loader.clone(), submitted_jobs.clone(), execution_notify.clone()))
            }).collect();
            
            let scheduler_algo = BaselineStaticSchedulerAlgorithm::new();
            SchedulerEngine{
                submitted_jobs, 
                workers, 
                scheduler_algo, 
                execution_notify,
                task_status: Arc::new(Mutex::new(HashMap::new())),
                completed_count: Arc::new(Mutex::new(0)),
                execution_start_time: Arc::new(Mutex::new(None)),
                baseline,
            }
        }

        pub async fn start_scheduler(&mut self, scheduler_arc: Arc<Mutex<SchedulerEngine>>)-> Result<Vec<tokio::task::JoinHandle<()>>, Error>{
            // Set scheduler reference in workers so they can notify on completion
            for worker in &self.workers {
                worker.set_scheduler(scheduler_arc.clone()).await;
            }
            
            // Capture baseline value before the closure
            let baseline = self.baseline.clone();
            
            let mut handlers:Vec<tokio::task::JoinHandle<()>> = vec![];
            // start all workers
            for worker in &self.workers {
                let worker = worker.clone();
                let baseline_clone = baseline.clone();
                
                // This blocks the thread and pins it to the core
                let handler = task::spawn_blocking(move || {
                    
                    // Pin to the correct core (unless baseline is "linux")
                    if baseline_clone != "linux" {
                        println!("Pinning worker");
                        core_affinity::set_for_current(worker.core_id);
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
            
            // Schedule tasks: sort by arrival time
            self.scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;
            // Notify all workers to start processing tasks
            self.execution_notify.notify_waiters();
        }

        // Called by workers when a task completes (push notification)
        pub async fn on_task_completed(&self, task_id: String) {
            let mut task_status = self.task_status.lock().await;
            
            // Check if task was already marked as completed (avoid double counting)
            if let Some(status) = task_status.get(&task_id) {
                if *status == 0 {
                    // Mark as processed
                    task_status.insert(task_id.clone(), 1);
                    drop(task_status);
                    
                    // Increment completed counter
                    let mut completed = self.completed_count.lock().await;
                    *completed += 1;
                    let completed_count = *completed;
                    drop(completed);
                    
                    // Get total tasks
                    let total_tasks = {
                        let status = self.task_status.lock().await;
                        status.len()
                    };
                    
                    // Check if all tasks are complete
                    if completed_count >= total_tasks {
                        // All tasks completed - calculate and print timing
                        let start_time_opt = {
                            *self.execution_start_time.lock().await
                        };
                        
                        if let Some(start_time) = start_time_opt {
                            let elapsed = start_time.elapsed();
                            let total_time_secs = elapsed.as_secs_f64();
                            let total_time_ms = elapsed.as_millis() as f64;
                            let avg_time_ms = if total_tasks > 0 {
                                total_time_ms / total_tasks as f64
                            } else {
                                0.0
                            };
                            let throughput = if total_time_secs > 0.0 {
                                total_tasks as f64 / total_time_secs
                            } else {
                                0.0
                            };
                            
                            println!("=== Execution completed ===");
                            println!("Total tasks processed: {}", total_tasks);
                            println!("Total execution time: {:.2} seconds ({:.2} ms)", total_time_secs, total_time_ms);
                            if total_tasks > 0 {
                                println!("Average time per task: {:.2} ms", avg_time_ms);
                            }
                            println!("Throughput: {:.2} tasks/second", throughput);
                            
                            // Store metrics to file
                            store_evaluation_metrics(total_tasks, total_time_secs, total_time_ms, avg_time_ms, throughput);
                            
                            // Reset timing for next execution
                            *self.execution_start_time.lock().await = None;
                            self.task_status.lock().await.clear();
                            *self.completed_count.lock().await = 0;
                        }
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



        pub async fn execute_jobs_continuously(&mut self)->Result<(), Error>{
            loop {
                println!("Checking for new tasks");
                println!("Num of submitted tasks: {:?}", self.submitted_jobs.get_num_tasks().await);
                self.scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }

    }


