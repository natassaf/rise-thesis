use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecuteTasksRequest {
    pub scheduling_algorithm: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WasmJobRequest {
    binary_name: String,
    func_name: String,
    payload: String,
    payload_compressed: bool,
    task_id: String,
    model_folder_name: String,
}

#[derive(Deserialize)]
pub struct TaskQuery {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Job {
    pub binary_path: String,
    pub func_name: String,
    pub payload: String,
    pub payload_compressed: bool, // Track if payload needs decompression
    pub id: String,
    pub folder_to_mount: String,
    pub status: String,
    pub arrival_time: std::time::SystemTime, // Track when job was submitted for sorting
    pub memory_prediction: Option<f64>,
    pub execution_time_prediction: Option<f64>,
    pub task_bound_type: Option<TaskBoundType>, // CPU bound, I/O bound, or Mixed
}

impl From<WasmJobRequest> for Job {
    fn from(request: WasmJobRequest) -> Self {
        // Construct the binary_path from binary_name
        let binary_path = format!("wasm-modules/{}", request.binary_name);
        // DON'T decompress here - do it in the worker to avoid blocking the handler
        // This prevents connection timeouts when handling large compressed payloads

        Job {
            binary_path,
            func_name: request.func_name,
            payload: request.payload, // Store compressed payload as-is
            payload_compressed: request.payload_compressed, // Remember if it needs decompression
            id: request.task_id,
            folder_to_mount: "models/".to_string() + &request.model_folder_name,
            status: "waiting".to_string(),
            arrival_time: std::time::SystemTime::now(), // Record arrival time for sorting
            memory_prediction: None,
            execution_time_prediction: None,
            task_bound_type: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<Job>>>,
    pub io_bound_task_ids: Arc<Mutex<std::collections::HashSet<String>>>, // I/O-bound task IDs set
    pub cpu_bound_task_ids: Arc<Mutex<std::collections::HashSet<String>>>, // CPU-bound task IDs set
    pub pending_job_ids: Arc<Mutex<HashSet<String>>>,
    pub reschedule_job_ids: Arc<Mutex<HashSet<String>>>,
    pub successfull_job_ids: Arc<Mutex<HashSet<String>>>,
    pub failed_job_ids: Arc<Mutex<HashSet<String>>>
}

impl SubmittedJobs {
    pub fn new() -> Self {
        let tasks = Arc::new(Mutex::new(vec![]));
        let io_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let cpu_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        Self {
            jobs: tasks,
            io_bound_task_ids: io_bound_set,
            cpu_bound_task_ids: cpu_bound_set,
            pending_job_ids: Arc::new(Mutex::new(HashSet::new())),
            reschedule_job_ids: Arc::new(Mutex::new(HashSet::new())),
            successfull_job_ids: Arc::new(Mutex::new(HashSet::new())),
            failed_job_ids: Arc::new(Mutex::new(HashSet::new()))
        }
    }

    pub async fn add_to_succesfull(&self, job_id:&str){
        self.successfull_job_ids.lock().await.insert(job_id.to_string());
    }

    pub async fn add_to_failed(&self, job_id:&str){
        self.failed_job_ids.lock().await.insert(job_id.to_string());
    }

    /// Set the I/O-bound task IDs set
    pub async fn set_io_bound_task_ids(&self, task_ids: Vec<String>) {
        let mut set = self.io_bound_task_ids.lock().await;
        *set = task_ids.into_iter().collect();
    }

    /// Set the CPU-bound task IDs set
    pub async fn set_cpu_bound_task_ids(&self, task_ids: Vec<String>) {
        let mut set = self.cpu_bound_task_ids.lock().await;
        *set = task_ids.into_iter().collect();
    }

    pub async fn remove_job(&self, job_id: String) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|job| job.id != job_id);
        drop(jobs); // Release the lock before acquiring the next one
        let mut pending = self.pending_job_ids.lock().await;
        pending.remove(&job_id);
    }

    /// Move a job_id from pending_job_ids to reschedule (for failed jobs)
    pub async fn move_to_reschedule(&self, job_id: String) {
        let mut pending = self.pending_job_ids.lock().await;
        if pending.remove(&job_id) {
            drop(pending); // Release the lock before acquiring the next one
            let mut reschedule = self.reschedule_job_ids.lock().await;
            reschedule.insert(job_id);
        }
    }

    /// Check if all jobs in the jobs vector are in the reschedule set
    pub async fn are_all_jobs_in_reschedule(&self) -> bool {
        let jobs = self.jobs.lock().await;
        let reschedule = self.reschedule_job_ids.lock().await;
        
        if jobs.is_empty() {
            return false;
        }
        
        // Check if all job IDs are in the reschedule set
        jobs.iter().all(|job| reschedule.contains(&job.id))
    }

    pub async fn get_num_tasks(&self) -> usize {
        let guard = self.jobs.lock().await;
        guard.len()
    }

    pub async fn get_jobs(&self) -> Vec<Job> {
        self.jobs.lock().await.to_vec()
    }

    // Gets the next job from the queue. The job will be removed when a success message is received
    pub async fn pop_next_job(&self) -> Option<Job> {
        let jobs = self.jobs.lock().await;
        // Get from the front (index 0) since jobs are sorted with oldest first
        if jobs.is_empty() {
            None
        } else {
            let job = jobs[0].clone();
            Some(job)
        }
    }

    /// Get the next I/O-bound job from the jobs list
    /// Returns a clone of the job without removing it from the queue
    /// The job will be removed when a success message is received
    pub async fn get_next_io_bounded_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        let io_bound_task_ids = self.io_bound_task_ids.lock().await;
        let pending_job_ids = self.pending_job_ids.lock().await;
        let reschedule_job_ids = self.reschedule_job_ids.lock().await;
        let failed_job_ids = self.failed_job_ids.lock().await;
        let jobs = self.jobs.lock().await;
        for i in 0..jobs.len() {
            if let Some((task_id, memory_pred)) = jobs.get(i).map(|job| (&job.id, &job.memory_prediction)) {
                // Skip jobs that are already pending (being executed)
                if pending_job_ids.contains(task_id) {
                    continue;
                }
                // Skip jobs that have already failed (in sequential mode)
                if failed_job_ids.contains(task_id) {
                    continue;
                }
                // If sequential_run_flag is false, skip jobs in reschedule_job_ids
                if !sequential_run_flag && reschedule_job_ids.contains(task_id) {
                    continue;
                }
                let job_memory = memory_pred.unwrap_or(0.0) as usize;
                if io_bound_task_ids.contains(task_id) && job_memory<=memory_capacity{
                    println!("job memore: {:?}, <=  memory_capacity: {:?}", job_memory, memory_capacity);
                    let job = jobs[i].clone();
                    drop(jobs); // releasing lock
                    drop(io_bound_task_ids); // releasing lock
                    drop(pending_job_ids); // releasing lock
                    drop(reschedule_job_ids); // releasing lock
                    drop(failed_job_ids); // releasing lock
                    return Some(job);
                }
            }
        }
        None
    }

    /// Get the next CPU-bound job from the jobs list
    /// Iterates from start (index 0) to end and returns the first job whose ID is in the cpu_bound_task_ids set
    /// Removes the job from the list when found
    /// Get the next CPU-bound job from the jobs list
    /// Returns a clone of the job without removing it from the queue
    /// The job will be removed when a success message is received
    pub async fn get_next_cpu_bounded_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        let cpu_bound_task_ids = self.cpu_bound_task_ids.lock().await;
        let pending_job_ids = self.pending_job_ids.lock().await;
        let reschedule_job_ids = self.reschedule_job_ids.lock().await;
        let failed_job_ids = self.failed_job_ids.lock().await;
        let jobs = self.jobs.lock().await;
        for i in 0..jobs.len() {
            if let Some((task_id, memory_pred)) = jobs.get(i).map(|job| (&job.id, &job.memory_prediction)) {
                // Skip jobs that are already pending (being executed)
                if pending_job_ids.contains(task_id) {
                    continue;
                }
                // Skip jobs that have already failed (in sequential mode)
                if failed_job_ids.contains(task_id) {
                    continue;
                }
                // If sequential_run_flag is false, skip jobs in reschedule_job_ids
                if !sequential_run_flag && reschedule_job_ids.contains(task_id) {
                    continue;
                }
                let job_memory = memory_pred.unwrap_or(0.0) as usize;
                if cpu_bound_task_ids.contains(task_id) && job_memory<=memory_capacity {
                    println!("job memore: {:?}, <=  memory_capacity: {:?}", job_memory, memory_capacity);
                    let job = jobs[i].clone();
                    drop(jobs); // releasing lock
                    drop(cpu_bound_task_ids); // releasing lock
                    drop(pending_job_ids); // releasing lock
                    drop(reschedule_job_ids); // releasing lock
                    drop(failed_job_ids); // releasing lock
                    return Some(job);
                }
            }
        }
        None
    }

     /// Get the next job from the jobs list
    /// The job will be removed when a success message is received
    pub async fn get_next_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        let pending_job_ids = self.pending_job_ids.lock().await;
        let reschedule_job_ids = self.reschedule_job_ids.lock().await;
        let failed_job_ids = self.failed_job_ids.lock().await;
        let jobs = self.jobs.lock().await;
        for i in 0..jobs.len() {
            let job_id = &jobs[i].id;
            // Skip jobs that are already pending (being executed)
            if pending_job_ids.contains(job_id) {
                continue;
            }
            // Skip jobs that have already failed (in sequential mode)
            if failed_job_ids.contains(job_id) {
                continue;
            }
            // If sequential_run_flag is false, skip jobs in reschedule_job_ids
            if !sequential_run_flag && reschedule_job_ids.contains(job_id) {
                continue;
            }
            let job_memory = jobs[i].memory_prediction.unwrap_or(0.0) as usize;
            if job_memory<=memory_capacity {
                println!("job memore: {:?}, <=  memory_capacity: {:?}", job_memory, memory_capacity);
                let job = jobs[i].clone();
                drop(jobs); // releasing lock
                drop(pending_job_ids); // releasing lock
                drop(reschedule_job_ids); // releasing lock
                drop(failed_job_ids); // releasing lock
                return Some(job);
            }
        }
        None
    }
    pub async fn add_task(&self, task: Job) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

    pub async fn print_status(&self) {
        let jobs = self.jobs.lock().await;
        let pending_job_ids = self.pending_job_ids.lock().await;
        let reschedule_job_ids = self.reschedule_job_ids.lock().await;
        let successfull_job_ids = self.successfull_job_ids.lock().await;
        let failed_job_ids = self.failed_job_ids.lock().await;

        println!("=== SubmittedJobs Status ===");
        println!("Total jobs: {}", jobs.len());
        println!("Pending job IDs ({}): {:?}", pending_job_ids.len(), pending_job_ids);
        println!("Reschedule job IDs ({}): {:?}", reschedule_job_ids.len(), reschedule_job_ids);
        println!("Successful job IDs ({}): {:?}", successfull_job_ids.len(), successfull_job_ids);
        println!("Failed job IDs ({}): {:?}", failed_job_ids.len(), failed_job_ids);
        
        // Print all job IDs for reference
        let all_job_ids: Vec<String> = jobs.iter().map(|job| job.id.clone()).collect();
        println!("All job IDs: {:?}", all_job_ids);
        println!("===========================");
    }
}
