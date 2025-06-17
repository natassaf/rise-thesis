    use std::{collections::{HashMap}, sync::Arc, time::Duration};

    use actix_web::web;
    use tokio::{time::sleep};
    use tokio::{time::error::Error};
    use crate::{various::{Job, SubmittedJobs, WasmJob}, worker::Worker};
    use core_affinity::*;


    // ========== SCHEDULER ==========
    pub struct JobsScheduler{
        submitted_jobs: web::Data<SubmittedJobs>,
        assigned_jobs: HashMap<usize, Vec<WasmJob>>,
        workers: Vec<Arc<Worker>>
    }

    impl JobsScheduler{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>)->Self{
            let assigned_jobs: HashMap<usize, Vec<WasmJob>> = core_ids.iter().map(|&val| (val.id, vec![])).collect();
            // let workers = core_ids.iter().map(|core_id| Worker::new(core_id.id, *core_id)).collect();
            let workers: Vec<Arc<Worker>> = core_ids.iter().map(|core_id| Arc::new(Worker::new(core_id.id, *core_id))).collect();
            JobsScheduler{submitted_jobs, assigned_jobs, workers}
        }

        pub async fn start_scheduler(&mut self) -> Result<(), Error> {
            // start all workers
            for worker in &self.workers {
                let worker = Arc::clone(worker);
                tokio::spawn(async move {
                    worker.start().await;
                    sleep(Duration::from_millis(100)).await;
                });
            }

            loop {
                println!("Checking for new tasks");
                println!("Num of submitted tasks: {:?}", self.submitted_jobs.get_num_tasks().await);
                Self::submitted_to_assigned_jobs(&self.submitted_jobs, &mut self.assigned_jobs).await;
                println!("Jobs to assign: {:?}", self.assigned_jobs);
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
            assigned_jobs: &mut HashMap<usize, Vec<WasmJob>>
        ) {
            // 1. Get all submitted jobs (pending tasks)
            let mut pending_jobs = submitted_jobs.get_jobs().await;

            // 2. Sort keys of assigned_jobs ascending to break ties by lowest key
            let mut worker_keys: Vec<usize> = assigned_jobs.keys().cloned().collect();
            worker_keys.sort();

            // 3. Helper function to find best worker key to assign a job
            fn find_worker_key(assigned_jobs: &HashMap<usize, Vec<WasmJob>>, keys: &[usize]) -> Option<usize> {
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
                    submitted_jobs.remove_job(job.job_input.id).await;
                } else {
                    // No workers available? Could log or break here
                    break;
                }
            }
        }


    }


