use std::{thread, time::Duration};

use actix_web::web;
use rand::Rng;
use tokio::task;
use crate::various::{ SubmittedTasks};
use core_affinity::*;
// ========== SCHEDULER ==========
//  Reads the submitted tasks calculates priorities and when core is freed runs the next task
pub struct TaskScheduler{
    submitted_tasks: web::Data<SubmittedTasks>,
    num_cpus: u8
}

impl TaskScheduler{

    pub fn new(num_cpus: u8, submitted_tasks: web::Data<SubmittedTasks>)->Self{
        TaskScheduler{submitted_tasks, num_cpus}
    }

    pub async fn calculate_task_priorities(&self){
        self.assign_random_priorities().await;
    }

    pub async fn assign_random_priorities(&self) {
        let mut task_list = self.submitted_tasks.tasks.lock().await;

        let mut rng = rand::rng();

        for task in task_list.iter_mut() {
            task.priority = rng.random_range(1..4); 
        }

        println!("Updated task priorities: {:?}", *task_list);
    }
    pub async fn run_tasks(&self) {
        let tasks = {
            let guard = self.submitted_tasks.tasks.lock().await;
            guard.clone()
        };

        let core_ids = core_affinity::get_core_ids().expect("Failed to get core IDs");
        let num_cores = core_ids.len();

        for (i, task) in tasks.into_iter().enumerate() {
            let core = core_ids[i % num_cores];

            // Spawn a blocking task, safe to use thread-affinity
            task::spawn_blocking(move || {
                core_affinity::set_for_current(core);
                println!("Running task {} on core {}", task.name, core.id);

                // Simulated work
                std::thread::sleep(Duration::from_secs(2));
                println!("Finished task {}", task.name);
            });
        }
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