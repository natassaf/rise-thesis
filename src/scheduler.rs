
// ========== SCHEDULER ==========

// Read full job queue (append-only log)
fn read_job_log() -> Result<Vec<Job>, Error> {
    // - Open shared log
    // - Deserialize all jobs
}

// Decide which jobs to schedule and assign worker IDs & priorities
fn schedule_jobs(jobs: &mut [Job]) -> Result<(), Error> {
    // - Filter queued jobs
    // - Sort by priority
    // - Assign target_worker and update job.status = Scheduled
    // - Write back updated job metadata (status, assignment)
}

// Monitor job queue for new jobs and trigger scheduling loop
fn scheduler_loop() -> Result<(), Error> {
    loop {
        // - Wait for new job arrival (poll or event)
        // - read_job_log()
        // - schedule_jobs()
        // - sleep or wait
    }
}