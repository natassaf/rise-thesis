use actix_web::web;
use async_trait::async_trait;

use crate::various::SubmittedJobs;


#[async_trait]
pub trait SchedulerAlgorithm{
    fn new()->Self;
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>);
}


pub struct BaselineStaticSchedulerAlgorithm{
}

impl BaselineStaticSchedulerAlgorithm{
    // Sort jobs by arrival time (oldest first)
    async fn sort_by_arrival_time(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        let mut jobs = submitted_jobs.jobs.lock().await;
        jobs.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
    }
}

#[async_trait]
impl SchedulerAlgorithm for BaselineStaticSchedulerAlgorithm{
    fn new()->Self{
        Self{}
    }

    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        // Sort jobs by arrival time (oldest first) so workers process them in order
        let job_ids_before: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
        println!("Sorting jobs by arrival time before: {:?}", job_ids_before);

        self.sort_by_arrival_time(submitted_jobs).await;

        let job_ids_after: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
        println!("Sorting jobs by arrival time after: {:?}", job_ids_after);
    }
}

pub struct MemoryTimeAwareSchedulerAlgorithm{
}

#[async_trait]
impl SchedulerAlgorithm for MemoryTimeAwareSchedulerAlgorithm{
    fn new()->Self{
        Self{}
    }
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        // for each job predict memory and time requirements
        // Create 3 arrays of tasks with memory ranges
        // Sort tasks by execution time from shortest to longest
    }
}
