// ========== WORKER ==========

// Read job queue and pick jobs assigned to this worker
fn fetch_assigned_jobs(worker_id: u8) -> Result<Vec<Job>, Error> {
    // - read_job_log()
    // - filter jobs by target_worker == worker_id && status == Scheduled
}

// Run a WASM job using wasm-micro-runtime
fn run_wasm_job(job: &Job) -> Result<JobResult, Error> {
    // - Initialize WAMR runtime (if needed)
    // - Load wasm module (precompiled or per job)
    // - Pass job.data input
    // - Execute wasm function
    // - Capture output and/or status
}

// Update job status as Running/Done in shared metadata
fn update_job_status(job_id: Uuid, status: JobStatus) -> Result<(), Error> {
    // - Write to shared metadata region/file
}

// Worker process main loop
fn worker_loop(worker_id: u8) -> Result<(), Error> {
    loop {
        let jobs = fetch_assigned_jobs(worker_id)?;
        for job in jobs {
            update_job_status(job.id, JobStatus::Running)?;
            let result = run_wasm_job(&job)?;
            update_job_status(job.id, JobStatus::Done)?;
            // Optionally log or report result
        }
        // Sleep or wait before next iteration
    }
}

