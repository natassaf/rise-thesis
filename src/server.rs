use actix_web::http::Error;
use serde::{Deserialize, Serialize};



pub struct JobRequest{
    task: String,
    number: u64
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Job {
    id: u64,
    data: String,
    priority: u8,
    status: JobStatus,
    target_worker: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
enum JobStatus {
    Queued,
    Scheduled,
    Running,
    Done,
}
// ========== SERVER SIDE ==========

// Accept new jobs and append to shared queue/log
async fn accept_job(request: JobRequest) -> Result<Job, Error> {
    // - Validate request
    // - Create Job struct with status=Queued
    // - Serialize job and append to shared log (file or shared memory)
}
