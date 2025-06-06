    use std::{collections::{vec_deque, HashMap, VecDeque}, sync::Arc, thread::{self, JoinHandle}, time::Duration};

    use actix_web::web;
    use tokio::sync::Mutex;
    use rand::Rng;
    use tokio::{task, time::error::Error};
    use crate::{all_tasks::fibonacci, various::{Job, SubmittedJobs}};
    use core_affinity::*;

    type worker = usize;

    pub struct Worker{
        worker_id:usize,
        core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<Job>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            Worker{worker_id, core_id, thread_queue}
        }

        pub async fn start(&self, core_id:CoreId){
            // Run worker
            thread::spawn(move || {
                // Pin thread to the core
                core_affinity::set_for_current(core_id);
                println!("[Core {}] Thread started", core_id.id);

                loop {
                    let task_opt = {
                        let mut q = self.thread_queue.lock().await;
                        q.pop_front()
                    };

                    if let Some(task) = task_opt {
                        task();
                    } else {
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            });
        }
    }

    // ========== SCHEDULER ==========
    pub struct JobsScheduler{
        submitted_jobs: web::Data<SubmittedJobs>,
        assigned_jobs: HashMap<worker, Vec<Job>>,
        core_ids: Vec<usize>,
        num_cores: usize
    }

    impl JobsScheduler{

        pub async fn new(core_ids: Vec<usize>, submitted_jobs: web::Data<SubmittedJobs>)->Self{
            let num_cores = &core_ids.len();
            let assigned_jobs: HashMap<worker, Vec<Job>> = core_ids.iter().map(|(&val)| (val, vec![])).collect();
            JobsScheduler{submitted_jobs: submitted_jobs, assigned_jobs, core_ids, num_cores: *num_cores}
        }

        pub async fn scheduler_loop() -> Result<(), Error> {
            // start all workers
            loop {
                // if any worker has empty queues: Add job to queue
                
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


    