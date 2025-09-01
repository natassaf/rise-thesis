    use std::{collections::{HashMap}, sync::Arc, time::Duration};
    use tokio::sync::Mutex;

    use actix_web::web;
    use tokio::{time::sleep};
    use tokio::{time::error::Error, task};
    use crate::{various::{Job, SubmittedJobs, WasmJobRequest}, worker::Worker};
    use core_affinity::*;


    // ========== SCHEDULER ==========
    pub struct JobsScheduler{
        submitted_jobs: web::Data<SubmittedJobs>,
        assigned_jobs: HashMap<usize, Vec<Job>>,
        workers: Vec<Arc<Worker>>
    }

    impl JobsScheduler{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>)->Self{
            let assigned_jobs: HashMap<usize, Vec<Job>> = core_ids[0..2].iter().map(|&val| (val.id, vec![])).collect();
            
            // Create a single shared WasmComponentLoader instance wrapped in Arc<Mutex>
            // Mount the model_1 directory so ONNX models can be accessed
            let shared_wasm_loader = Arc::new(Mutex::new(crate::wasm_loaders::WasmComponentLoader::new("models".to_string())));
            
            // Create workers with the shared wasm_loader
            let workers: Vec<Arc<Worker>> = core_ids[0..2].iter().map(|core_id| {
                Arc::new(Worker::new(core_id.id, *core_id, shared_wasm_loader.clone()))
            }).collect();
            
            JobsScheduler{submitted_jobs, assigned_jobs, workers}
        }

        pub async fn start_scheduler(&mut self)-> Result<Vec<tokio::task::JoinHandle<()>>, Error>{
            let mut handlers:Vec<tokio::task::JoinHandle<()>> = vec![];
            // start all workers
            for worker in &self.workers {
                let worker = Arc::clone(worker);
                
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
            Self::submitted_to_assigned_jobs(&self.submitted_jobs, &mut self.assigned_jobs).await;
                // println!("Jobs to assign: {:?}", self.assigned_jobs);
                for (k,v) in self.assigned_jobs.iter(){
                    self.workers[*k].add_to_queue(v.clone()).await;
                }
                
                for jobs in self.assigned_jobs.values_mut() {
                    jobs.clear();
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
                Self::submitted_to_assigned_jobs(&self.submitted_jobs, &mut self.assigned_jobs).await;
                // println!("Jobs to assign: {:?}", self.assigned_jobs);
                for (k,v) in self.assigned_jobs.iter(){
                    self.workers[*k].add_to_queue(v.clone()).await;
                }
                
                for jobs in self.assigned_jobs.values_mut() {
                    jobs.clear();
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }

        pub async fn submitted_to_assigned_jobs(
            submitted_jobs: &web::Data<SubmittedJobs>, 
            assigned_jobs: &mut HashMap<usize, Vec<Job>>
        ) {
            // 1. Get all submitted jobs (pending tasks)
            let mut pending_jobs = submitted_jobs.get_jobs().await;

            // 2. Sort keys of assigned_jobs ascending to break ties by lowest key
            let mut worker_keys: Vec<usize> = assigned_jobs.keys().cloned().collect();
            worker_keys.sort();

            // 3. Helper function to find best worker key to assign a job
            fn find_worker_key(assigned_jobs: &HashMap<usize, Vec<Job>>, keys: &[usize]) -> Option<usize> {
                // Prefer empty workers
                if let Some(empty_key) = keys.iter()
                    .find(|&&key| assigned_jobs.get(&key).map(|v| v.is_empty()).unwrap_or(true))
                {
                    return Some(*empty_key);
                }

                // Otherwise find worker with least jobs
                keys.iter()
                    .min_by_key(|&&key| assigned_jobs.get(&key).map(|v| v.len()).unwrap_or(usize::MAX))
                    .copied()
            }

            // 4. Assign each pending job to a worker and remove it from submitted jobs
            for job in pending_jobs.drain(..) {
                if let Some(worker_key) = find_worker_key(&assigned_jobs, &worker_keys) {
                    assigned_jobs.entry(worker_key).or_default().push(job.clone());

                    // Remove the job from submitted_jobs storage
                    // Assuming you have a method `remove_job` that accepts job ID and is async
                    submitted_jobs.remove_job(job.id).await;
                } else {
                    // No workers available? Could log or break here
                    break;
                }
            }
        }


    }


