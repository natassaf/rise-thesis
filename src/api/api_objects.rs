use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecuteTasksRequest{
    pub scheduling_algorithm: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WasmJobRequest{
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


#[derive(Debug, Clone)]
pub struct Job{
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
            status:"waiting".to_string(),
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
}

impl SubmittedJobs{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        let io_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let cpu_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        Self{
            jobs: tasks,
            io_bound_task_ids: io_bound_set,
            cpu_bound_task_ids: cpu_bound_set,
        }
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
    }

    pub async fn get_num_tasks(&self)->usize{
        let guard = self.jobs.lock().await;
        guard.len()
    }
    
    pub async fn get_jobs(&self)->Vec<Job>{
        self.jobs.lock().await.to_vec()
    }

    // Get and remove the next job from the queue (for workers to pull jobs)
    // Pops from the front (oldest first after sorting)
    pub async fn pop_next_job(&self) -> Option<Job> {
        let mut jobs = self.jobs.lock().await;
        // Pop from front (oldest first after sorting)
        if jobs.is_empty() {
            None
        } else {
            Some(jobs.pop().unwrap())
        }
    }

    /// Get the next I/O-bound job from the jobs list
    /// Iterates from start (index 0) to end and returns the first job whose ID is in the io_bound_task_ids set
    /// Removes the job from the list when found
    pub async fn get_next_io_bounded_job(&self) -> Option<Job> {
        let io_bound_task_ids = self.io_bound_task_ids.lock().await;
        let mut jobs = self.jobs.lock().await;
        for i in 0..jobs.len() {
            if let Some(task_id) = jobs.get(i).map(|job| &job.id) {
                if io_bound_task_ids.contains(task_id) {
                    return Some(jobs.remove(i));
                }
            }
        }
        None
    }

    /// Get the next CPU-bound job from the jobs list
    /// Iterates from start (index 0) to end and returns the first job whose ID is in the cpu_bound_task_ids set
    /// Removes the job from the list when found
    pub async fn get_next_cpu_bounded_job(&self) -> Option<Job> {
        let cpu_bound_task_ids = self.cpu_bound_task_ids.lock().await;
        let mut jobs = self.jobs.lock().await;
        for i in 0..jobs.len() {
            if let Some(task_id) = jobs.get(i).map(|job| &job.id) {
                if cpu_bound_task_ids.contains(task_id) {
                    return Some(jobs.remove(i));
                }
            }
        }
        None
    }

    pub async fn get_next_job(&self) -> Option<Job> {
        let mut jobs = self.jobs.lock().await;
        if jobs.is_empty() {
            None
        } else {
            Some(jobs.remove(0))
        }
    }

    pub async fn add_task(&self, task: Job) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

}

