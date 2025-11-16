use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

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
        }
    }
}


#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<Job>>>,
}

impl SubmittedJobs{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        Self{jobs: tasks}
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

    pub async fn add_task(&self, task: Job) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

}

