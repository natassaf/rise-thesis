    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify};

    use actix_web::web;
    use tokio::{time::error::Error, task};
    use crate::{various::SubmittedJobs, worker::Worker};
    use core_affinity::*;

    pub struct BaselineStaticSchedulerAlgorithm{
    }

    impl BaselineStaticSchedulerAlgorithm{
        pub fn new()->Self{
            Self{}
        }
        
        pub async fn schedule_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
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


    // ========== SCHEDULER ==========
    pub struct SchedulerEngine{
        submitted_jobs: web::Data<SubmittedJobs>,
        workers: Vec<Arc<Worker>>,
        scheduler_algo: BaselineStaticSchedulerAlgorithm,
        execution_notify: Arc<Notify>, // Signal to workers to start processing
    }

    impl SchedulerEngine{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>,  num_workers_to_start: usize)->Self{
            
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
            SchedulerEngine{submitted_jobs, workers, scheduler_algo, execution_notify}
        }

        pub async fn start_scheduler(&mut self)-> Result<Vec<tokio::task::JoinHandle<()>>, Error>{
            let mut handlers:Vec<tokio::task::JoinHandle<()>> = vec![];
            // start all workers
            for worker in &self.workers {
                let worker = worker.clone();
                
                // This blocks the thread and pins it to the core
                let handler = task::spawn_blocking(move || {
                    // Pin to the correct core
                    core_affinity::set_for_current(worker.core_id);

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

        pub async fn execute_jobs(&mut self){
            // Schedule tasks: sort by arrival time
            self.scheduler_algo.schedule_tasks(&self.submitted_jobs).await;
            // Notify all workers to start processing tasks
            self.execution_notify.notify_waiters();
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
                self.scheduler_algo.schedule_tasks(&self.submitted_jobs).await;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }

    }


