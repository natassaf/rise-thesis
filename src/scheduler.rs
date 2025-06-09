    use std::{collections::{vec_deque, HashMap, VecDeque}, rc::Weak, sync::Arc, thread::{self, JoinHandle}, time::Duration};

    use actix_web::web;
    use futures::future::join_all;
    use tokio::{sync::Mutex, time::sleep};
    use rand::Rng;
    use tokio::{task, time::error::Error};
    use crate::{all_tasks::fibonacci, various::{Job, SubmittedJobs}};
    use core_affinity::*;


    pub struct Worker{
        worker_id:usize,
        core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<Job>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            // let core = core_ids[i % num_cores];
            Worker{worker_id, core_id, thread_queue}
        }

        pub async fn start(&self){
            println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
            loop{
                // Retrieves the next task from the queue runs it buy awaiting and returns result
                let mut queue = self.thread_queue.lock().await; // lock the mutex
                let my_task = queue.pop_front();
                match my_task{
                    None=> (),
                    Some(my_task_1)=>{
                        let my_task = my_task_1.clone();
                        let core_id = self.core_id.clone();
                        // Spawn a blocking task to map the worker to the core 
                        let handle = task::spawn_blocking(move || {     
                            core_affinity::set_for_current(core_id);
                            println!("Running task {} on core {:?}", my_task.name, core_id);
                            let result = fibonacci(my_task.n);
                            
                            // Store result in a file named after the task
                            let file_name = format!("result_{}.txt", my_task.id);
                            std::fs::write(&file_name, format!("Result: {}", result)).expect("Failed to write result to file");
                            println!("Finished task {}", my_task.name);
                            result
                        });
                        let result = match handle.await {
                            Ok(result) => Some(result),
                            Err(e) => None,
                        };
                        ()
                        }
                    }
                }
            }
    }

    // ========== SCHEDULER ==========
    pub struct JobsScheduler{
        submitted_jobs: web::Data<SubmittedJobs>,
        assigned_jobs: HashMap<usize, Vec<Job>>,
        core_ids: Vec<CoreId>,
        num_cores: usize,
        workers: Vec<Arc<Worker>>
    }

    impl JobsScheduler{

        pub fn new(core_ids: Vec<CoreId>, submitted_jobs: web::Data<SubmittedJobs>)->Self{
            let num_cores = &core_ids.len();
            let assigned_jobs: HashMap<usize, Vec<Job>> = core_ids.iter().map(|(&val)| (val.id, vec![])).collect();
            // let workers = core_ids.iter().map(|core_id| Worker::new(core_id.id, *core_id)).collect();
            let workers: Vec<Arc<Worker>> = core_ids.iter().map(|core_id| Arc::new(Worker::new(core_id.id, *core_id))).collect();
            JobsScheduler{submitted_jobs, assigned_jobs, core_ids, num_cores: *num_cores, workers}
        }

        pub async fn start_scheduler(&self) -> Result<(), Error> {
            // start all workers
            // let results  = self.assigned_jobs.iter().map(|(k, v)| self.workers[*k].start()).collect();
            for worker in &self.workers {
                let worker = Arc::clone(worker);
                tokio::spawn(async move {
                    worker.start().await;
                    sleep(Duration::from_millis(100)).await;
                });
            }


            loop {
                // iterate through assigned jobs  and spawn worker
                println!("Checking for new tasks");
                println!("Num of submitted tasks: {:?}", self.submitted_jobs.get_num_tasks().await);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }

        pub async fn calculate_task_priorities(&self){
            self.assign_random_priorities().await;
        }

        pub async fn assign_random_priorities(&self) {
            let mut jobs = self.submitted_jobs.get_jobs().await;

            let mut rng = rand::rng();

            for job in jobs.iter_mut() {
                job.set_priority(rng.random_range(1..4)); 
            }

            println!("Updated task priorities: {:?}", jobs);
        }

        // pub async fn assign_tasks_to_workers(){

        // }

        pub async fn run_tasks_parallel(&self)->Vec<task::JoinHandle<usize>> {
            let tasks = {
                let guard = self.submitted_jobs.get_jobs().await;
                guard.clone()
            };
            let core_ids: Vec<CoreId> = core_affinity::get_core_ids().expect("Failed to get core IDs");
            let num_cores = core_ids.len();

            println!("num_cores: {}", num_cores);
            println!("core_ids: {:?}", core_ids);
            let handles:Vec<task::JoinHandle<usize>> = tasks.into_iter().enumerate().map(|(i, my_task)| {
                let core = core_ids[i % num_cores];

                // Spawn a blocking task, safe to use thread-affinity
                let handle = task::spawn_blocking(move || {
                    core_affinity::set_for_current(core);
                    println!("Running task {} on core {}", my_task.name, core.id);

                    let result = fibonacci(my_task.n);

                    // Store result in a file named after the task
                    let file_name = format!("result_{}.txt", my_task.id);
                    std::fs::write(&file_name, format!("Result: {}", result))
                        .expect("Failed to write result to file");
                    println!("Finished task {}", my_task.name);

                    result
                });
                handle
            }).collect();
            handles
        }
        
    pub async fn run_tasks(&self)->Vec<usize> {
            let tasks = {
                let guard = self.submitted_jobs.jobs.lock().await;
                guard.clone()
            };
            let result = tasks.into_iter().enumerate().map(|(i, my_task)| fibonacci(my_task.n)).collect();
            println!("task executed result: {:?}", result);
            result
        }
    }


    // // Read full job queue (append-only log)
    // fn read_job_log() -> Result<Vec<Job>, Error> {
    //     // - Open shared log
    //     // - Deserialize all jobs
    // }

    // // Decide which jobs to schedule and assign worker IDs & priorities
    // fn schedule_jobs(jobs: &mut [Job]) -> Result<(), Error> {
    //     // - Filter queued jobs
    //     // - Sort by priority
    //     // - Assign target_worker and update job.status = Scheduled
    //     // - Write back updated job metadata (status, assignment)
    // }

    // // Monitor job queue for new jobs and trigger scheduling loop
    // fn scheduler_loop() -> Result<(), Error> {
    //     loop {
    //         // - Wait for new job arrival (poll or event)
    //         // - read_job_log()
    //         // - schedule_jobs()
    //         // - sleep or wait
    //     }
    // }


    