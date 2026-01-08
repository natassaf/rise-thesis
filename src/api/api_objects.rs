use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;
use crate::memory_monitoring::get_available_memory_kb;
use serde::{Deserialize, Serialize};
use std::{collections::{HashSet, HashMap}, sync::Arc};
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

/// Memory bucket: groups jobs by memory range for faster lookup
#[derive(Debug, Clone)]
struct MemoryBucket {
    min_memory: usize,
    max_memory: usize,
    job_ids: Vec<String>, // Job IDs (not indices, so they don't become stale)
}

#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<String>>>, // Job IDs vector
    job_map: Arc<Mutex<HashMap<String, Job>>>, // HashMap mapping job IDs to Job objects
    pub num_jobs: Arc<Mutex<usize>>,
    pub io_bound_task_ids: Arc<Mutex<std::collections::HashSet<String>>>, // I/O-bound task IDs set
    pub cpu_bound_task_ids: Arc<Mutex<std::collections::HashSet<String>>>, // CPU-bound task IDs set
    pub pending_job_ids: Arc<Mutex<HashSet<String>>>,
    pub reschedule_job_ids: Arc<Mutex<HashSet<String>>>,
    pub successfull_job_ids: Arc<Mutex<HashSet<String>>>,
    pub failed_job_ids: Arc<Mutex<HashSet<String>>>,
    memory_buckets: Arc<Mutex<Option<Vec<MemoryBucket>>>>, // Memory buckets for fast lookup (None if no memory predictions)
    io_memory_buckets: Arc<Mutex<Option<Vec<MemoryBucket>>>>, // IO-bound memory buckets (separate for faster IO job selection)
    cpu_memory_buckets: Arc<Mutex<Option<Vec<MemoryBucket>>>>, // CPU-bound memory buckets (separate for faster CPU job selection)
}

impl SubmittedJobs {
    pub fn new() -> Self {
        let tasks = Arc::new(Mutex::new(vec![]));
        let job_map = Arc::new(Mutex::new(HashMap::new()));
        let io_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let cpu_bound_set = Arc::new(Mutex::new(std::collections::HashSet::new()));
        Self {
            jobs: tasks,
            job_map,
            num_jobs: Arc::new(Mutex::new(0)),
            io_bound_task_ids: io_bound_set,
            cpu_bound_task_ids: cpu_bound_set,
            pending_job_ids: Arc::new(Mutex::new(HashSet::new())),
            reschedule_job_ids: Arc::new(Mutex::new(HashSet::new())),
            successfull_job_ids: Arc::new(Mutex::new(HashSet::new())),
            failed_job_ids: Arc::new(Mutex::new(HashSet::new())),
            memory_buckets: Arc::new(Mutex::new(None)),
            io_memory_buckets: Arc::new(Mutex::new(None)),
            cpu_memory_buckets: Arc::new(Mutex::new(None)),
        }
    }

    /// Get a job from the HashMap by its ID
    pub async fn get_job_by_id(&self, job_id: &str) -> Option<Job> {
        let job_map = self.job_map.lock().await;
        job_map.get(job_id).cloned()
    }

    /// Update a job's properties in the HashMap
    pub async fn update_job<F>(&self, job_id: &str, updater: F) -> bool
    where
        F: FnOnce(&mut Job),
    {
        let mut job_map = self.job_map.lock().await;
        if let Some(job) = job_map.get_mut(job_id) {
            updater(job);
            true
        } else {
            false
        }
    }

    /// Sort the jobs vector based on a comparison function that uses job data from HashMap
    pub async fn sort_jobs<F>(&self, compare: F)
    where
        F: Fn(&Job, &Job) -> std::cmp::Ordering,
    {
        let mut job_ids = self.jobs.lock().await;
        let job_map = self.job_map.lock().await;
        
        job_ids.sort_by(|id_a, id_b| {
            let job_a = job_map.get(id_a);
            let job_b = job_map.get(id_b);
            match (job_a, job_b) {
                (Some(a), Some(b)) => compare(a, b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
    }

    pub async fn add_to_succesfull(&self, job_id:&str){
        self.successfull_job_ids.lock().await.insert(job_id.to_string());
    }

    pub async fn add_to_failed(&self, job_id:&str){
        self.failed_job_ids.lock().await.insert(job_id.to_string());
    }

    pub async fn add_to_pending(&self, job_id: &str) {
        self.pending_job_ids.lock().await.insert(job_id.to_string());
    }

    pub async fn get_successful_count(&self) -> usize {
        self.successfull_job_ids.lock().await.len()
    }

    pub async fn get_failed_count(&self) -> usize {
        self.failed_job_ids.lock().await.len()
    }

    pub async fn get_pending_count(&self) -> usize {
        self.pending_job_ids.lock().await.len()
    }

    pub async fn get_reschedule_count(&self) -> usize {
        self.reschedule_job_ids.lock().await.len()
    }

    pub async fn print_status_summary(&self) {
        let pending = self.pending_job_ids.lock().await;
        let reschedule = self.reschedule_job_ids.lock().await;
        let successfull = self.successfull_job_ids.lock().await;
        let failed = self.failed_job_ids.lock().await;
        let available_memory = get_available_memory_kb();
        
        println!("Status - Successful: {}, Failed: {}, Pending: {}, Rescheduled: {}, Available Memory: {} KB", 
            successfull.len(), failed.len(), pending.len(), reschedule.len(), available_memory);
    }

    /// Sets the I/O-bound task IDs set
    pub async fn set_io_bound_task_ids(&self, task_ids: Vec<String>) {
        let mut set = self.io_bound_task_ids.lock().await;
        *set = task_ids.into_iter().collect();
    }

    /// Sets the CPU-bound task IDs set
    pub async fn set_cpu_bound_task_ids(&self, task_ids: Vec<String>) {
        let mut set = self.cpu_bound_task_ids.lock().await;
        *set = task_ids.into_iter().collect();
    }

    pub async fn remove_job(&self, job_id: String) {
        // Remove from jobs vector (which now contains IDs)
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|id| id != &job_id);
        drop(jobs);
        
        // Remove from job_map
        let mut job_map = self.job_map.lock().await;
        job_map.remove(&job_id);
        drop(job_map);
        
        // Remove from pending
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

    /// Move the next job from jobs list (that is not in pending) to reschedule_job_ids
    pub async fn move_next_job_to_reschedule(&self) -> bool {
        // First, collect all job IDs while holding only the jobs lock
        let job_ids: Vec<String> = {
            let jobs = self.jobs.lock().await;
            jobs.clone() // jobs is now Vec<String>, so we can clone it
        };
        
        // Now check each job ID against the other sets, acquiring locks only when needed
        for job_id in job_ids {
            // Check if job is in any of the exclusion sets
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(&job_id)
            };
            if in_pending {
                continue;
            }
            
            let in_reschedule = {
                let reschedule = self.reschedule_job_ids.lock().await;
                reschedule.contains(&job_id)
            };
            if in_reschedule {
                continue;
            }
            
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(&job_id)
            };
            if in_failed {
                continue;
            }
            
            let in_successful = {
                let successfull = self.successfull_job_ids.lock().await;
                successfull.contains(&job_id)
            };
            if in_successful {
                continue;
            }
            
            // Found a job to move - add to reschedule
            let mut reschedule = self.reschedule_job_ids.lock().await;
            reschedule.insert(job_id.clone());
            return true;
        }
        
        // No job found to move
        false
    }

    /// Check if all jobs in the jobs vector are in the reschedule set
    pub async fn are_all_jobs_in_reschedule(&self) -> bool {
        let jobs = self.jobs.lock().await;
        let reschedule = self.reschedule_job_ids.lock().await;
        
        if jobs.is_empty() {
            return false;
        }
        
        // Check if all job IDs are in the reschedule set
        let all_in_reschedule = jobs.iter().all(|job_id| reschedule.contains(job_id));
        
        all_in_reschedule
    }

    pub async fn get_num_tasks(&self) -> usize {
        let guard = self.jobs.lock().await;
        guard.len()
    }

    pub async fn get_jobs(&self) -> Vec<Job> {
        let job_ids = self.jobs.lock().await;
        let job_map = self.job_map.lock().await;
        job_ids.iter()
            .filter_map(|id| job_map.get(id).cloned())
            .collect()
    }

    // Gets the next job from the queue. The job will be removed when a success message is received
    pub async fn pop_next_job(&self) -> Option<Job> {
        let jobs = self.jobs.lock().await;
        // Get from the front (index 0) since jobs are sorted with oldest first
        if jobs.is_empty() {
            None
        } else {
            let job_id = jobs[0].clone();
            drop(jobs);
            self.get_job_by_id(&job_id).await
        }
    }

    /// Get the next I/O-bound job from the jobs list
    /// Returns a clone of the job without removing it from the queue
    /// The job will be removed when a success message is received
    pub async fn get_next_io_bounded_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        // Try using IO-bound memory buckets if available
        // OPTIMIZATION: Get reference instead of cloning to avoid expensive copy
        let io_buckets_guard = self.io_memory_buckets.lock().await;
        
        if let Some(io_buckets) = io_buckets_guard.as_ref() {
            // Search buckets from largest to smallest
            for bucket in io_buckets.iter().rev() {
                // Skip empty buckets
                if bucket.job_ids.is_empty() {
                    continue;
                }
                
                // Check if this bucket matches memory capacity
                if !sequential_run_flag && bucket.min_memory > memory_capacity {
                    // This bucket's jobs are too large, skip it
                    continue;
                }
                
                // Lock all data structures ONCE for the entire bucket iteration
                let job_map = self.job_map.lock().await;
                let pending = self.pending_job_ids.lock().await;
                let failed = self.failed_job_ids.lock().await;
                let reschedule = if !sequential_run_flag {
                    Some(self.reschedule_job_ids.lock().await)
                } else {
                    None
                };
                
                // Check each job ID in this bucket
                for job_id in &bucket.job_ids {
                    // OPTIMIZATION: Check cheap conditions FIRST (HashSet lookups)
                    // before doing expensive HashMap lookup
                    if pending.contains(job_id) {
                        continue; // Skip pending jobs immediately
                    }
                    
                    if failed.contains(job_id) {
                        continue; // Skip failed jobs immediately
                    }
                    
                    if !sequential_run_flag {
                        if let Some(ref reschedule_guard) = reschedule {
                            if reschedule_guard.contains(job_id) {
                                continue; // Skip rescheduled jobs immediately
                            }
                        }
                    }
                    
                    // Only do expensive HashMap lookup if job passed all cheap checks
                    // OPTIMIZATION: Check memory from reference before cloning
                    let job_memory = match job_map.get(job_id) {
                        Some(job) => {
                            if sequential_run_flag {
                                0
                            } else {
                                job.memory_prediction.unwrap_or(0.0) as usize
                            }
                        },
                        None => continue, // Job was removed from HashMap
                    };
                    
                    // Only clone job if it fits in memory capacity
                    if job_memory <= memory_capacity {
                        let job = job_map.get(job_id).unwrap().clone(); // Safe unwrap - we just checked it exists
                        // Drop all locks before returning
                        drop(job_map);
                        drop(pending);
                        drop(failed);
                        drop(reschedule);
                        return Some(job); // EARLY RETURN - found valid job!
                    }
                }
                // Locks are automatically dropped here when going to next bucket
                // If we get here, no valid job found in this bucket, try next bucket
            }
            return None;
        }
        
        // Fallback to linear search if no buckets
        let (job_ids, io_bound_task_ids): (Vec<String>, HashSet<String>) = {
            let jobs_guard = self.jobs.lock().await;
            let io_bound_guard = self.io_bound_task_ids.lock().await;
            (jobs_guard.clone(), io_bound_guard.clone())
        };
        
        for job_id in &job_ids {
            if !io_bound_task_ids.contains(job_id) {
                continue;
            }
            
            let job = match self.get_job_by_id(job_id).await {
                Some(job) => job,
                None => continue,
            };
            
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(job_id)
            };
            if in_pending {
                continue;
            }
            
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(job_id)
            };
            if in_failed {
                continue;
            }
            
            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(job_id)
                };
                if in_reschedule {
                    continue;
                }
            }
            
            let job_memory = if sequential_run_flag {
                0
            } else {
                job.memory_prediction.unwrap_or(0.0) as usize
            };
            
            if job_memory <= memory_capacity {
                return Some(job);
            }
        }
        None
    }

    /// Get the next CPU-bound job from the jobs list
    /// Returns a clone of the job without removing it from the queue
    /// The job will be removed when a success message is received
    pub async fn get_next_cpu_bounded_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        // Try using CPU-bound memory buckets if available
        // OPTIMIZATION: Get reference instead of cloning to avoid expensive copy
        let cpu_buckets_guard = self.cpu_memory_buckets.lock().await;
        
        if let Some(cpu_buckets) = cpu_buckets_guard.as_ref() {
            // Search buckets from largest to smallest
            for bucket in cpu_buckets.iter().rev() {
                // Skip empty buckets
                if bucket.job_ids.is_empty() {
                    continue;
                }
                
                // Check if this bucket matches memory capacity
                if !sequential_run_flag && bucket.min_memory > memory_capacity {
                    // This bucket's jobs are too large, skip it
                    continue;
                }
                
                // Lock all data structures ONCE for the entire bucket iteration
                let job_map = self.job_map.lock().await;
                let pending = self.pending_job_ids.lock().await;
                let failed = self.failed_job_ids.lock().await;
                let reschedule = if !sequential_run_flag {
                    Some(self.reschedule_job_ids.lock().await)
                } else {
                    None
                };
                
                // Check each job ID in this bucket
                for job_id in &bucket.job_ids {
                    // OPTIMIZATION: Check cheap conditions FIRST (HashSet lookups)
                    // before doing expensive HashMap lookup
                    if pending.contains(job_id) {
                        continue; // Skip pending jobs immediately
                    }
                    
                    if failed.contains(job_id) {
                        continue; // Skip failed jobs immediately
                    }
                    
                    if !sequential_run_flag {
                        if let Some(ref reschedule_guard) = reschedule {
                            if reschedule_guard.contains(job_id) {
                                continue; // Skip rescheduled jobs immediately
                            }
                        }
                    }
                    
                    // Only do expensive HashMap lookup if job passed all cheap checks
                    // OPTIMIZATION: Check memory from reference before cloning
                    let job_memory = match job_map.get(job_id) {
                        Some(job) => {
                            if sequential_run_flag {
                                0
                            } else {
                                job.memory_prediction.unwrap_or(0.0) as usize
                            }
                        },
                        None => continue, // Job was removed from HashMap
                    };
                    
                    // Only clone job if it fits in memory capacity
                    if job_memory <= memory_capacity {
                        let job = job_map.get(job_id).unwrap().clone(); // Safe unwrap - we just checked it exists
                        // Drop all locks before returning
                        drop(job_map);
                        drop(pending);
                        drop(failed);
                        drop(reschedule);
                        return Some(job); // EARLY RETURN - found valid job!
                    }
                }
                // Locks are automatically dropped here when going to next bucket
                // If we get here, no valid job found in this bucket, try next bucket
            }
            return None;
        }
        
        // Fallback to linear search if no buckets
        let (job_ids, cpu_bound_task_ids): (Vec<String>, HashSet<String>) = {
            let jobs_guard = self.jobs.lock().await;
            let cpu_bound_guard = self.cpu_bound_task_ids.lock().await;
            (jobs_guard.clone(), cpu_bound_guard.clone())
        };
        
        for job_id in &job_ids {
            if !cpu_bound_task_ids.contains(job_id) {
                continue;
            }
            
            let job = match self.get_job_by_id(job_id).await {
                Some(job) => job,
                None => continue,
            };
            
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(job_id)
            };
            if in_pending {
                continue;
            }
            
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(job_id)
            };
            if in_failed {
                continue;
            }
            
            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(job_id)
                };
                if in_reschedule {
                    continue;
                }
            }
            
            let job_memory = if sequential_run_flag {
                0
            } else {
                job.memory_prediction.unwrap_or(0.0) as usize
            };
            
            if job_memory <= memory_capacity {
                return Some(job);
            }
        }
        None
    }

    /// Get the next job from the jobs list
    /// The job will be removed when a success message is received
    pub async fn get_next_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        // Try using memory buckets if available
        // OPTIMIZATION: Get reference instead of cloning to avoid expensive copy
        let buckets_guard = self.memory_buckets.lock().await;
        
        if let Some(buckets) = buckets_guard.as_ref() {
            // Search buckets from largest to smallest
            for bucket in buckets.iter().rev() {
                // Skip empty buckets
                if bucket.job_ids.is_empty() {
                    continue;
                }
                
                // Check if this bucket matches memory capacity
                if !sequential_run_flag && bucket.min_memory > memory_capacity {
                    // This bucket's jobs are too large, skip it
                    continue;
                }
                
                // Lock all data structures ONCE for the entire bucket iteration
                let job_map = self.job_map.lock().await;
                let pending = self.pending_job_ids.lock().await;
                let failed = self.failed_job_ids.lock().await;
                let reschedule = if !sequential_run_flag {
                    Some(self.reschedule_job_ids.lock().await)
                } else {
                    None
                };
                
                // Check each job ID in this bucket
                for job_id in &bucket.job_ids {
                    // OPTIMIZATION: Check cheap conditions FIRST (HashSet lookups)
                    // before doing expensive HashMap lookup
                    if pending.contains(job_id) {
                        continue; // Skip pending jobs immediately
                    }
                    
                    if failed.contains(job_id) {
                        continue; // Skip failed jobs immediately
                    }
                    
                    if !sequential_run_flag {
                        if let Some(ref reschedule_guard) = reschedule {
                            if reschedule_guard.contains(job_id) {
                                continue; // Skip rescheduled jobs immediately
                            }
                        }
                    }
                    
                    // Only do expensive HashMap lookup if job passed all cheap checks
                    // OPTIMIZATION: Check memory from reference before cloning
                    let job_memory = match job_map.get(job_id) {
                        Some(job) => {
                            if sequential_run_flag {
                                0
                            } else {
                                job.memory_prediction.unwrap_or(0.0) as usize
                            }
                        },
                        None => continue, // Job was removed from HashMap
                    };
                    
                    // Only clone job if it fits in memory capacity
                    if job_memory <= memory_capacity {
                        let job = job_map.get(job_id).unwrap().clone(); // Safe unwrap - we just checked it exists
                        // Drop all locks before returning
                        drop(job_map);
                        drop(pending);
                        drop(failed);
                        drop(reschedule);
                        return Some(job); // EARLY RETURN - found valid job!
                    }
                }
                // Locks are automatically dropped here when going to next bucket
                // If we get here, no valid job found in this bucket, try next bucket
            }
            return None;
        }
        
        // Fallback to linear search if no buckets
        let job_ids: Vec<String> = {
            let jobs_guard = self.jobs.lock().await;
            jobs_guard.clone()
        };
        
        for job_id in &job_ids {
            let job = match self.get_job_by_id(job_id).await {
                Some(job) => job,
                None => continue,
            };
            
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(job_id)
            };
            if in_pending {
                continue;
            }
            
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(job_id)
            };
            if in_failed {
                continue;
            }

            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(job_id)
                };
                if in_reschedule {
                    continue;
                }
            }

            let job_memory = if sequential_run_flag {
                0
            } else {
                job.memory_prediction.unwrap_or(0.0) as usize
            };

            if job_memory <= memory_capacity {
                return Some(job);
            }
        }
        None
    }

    /// Check if a job is eligible (not in pending, failed, or reschedule)
    async fn is_job_eligible(&self, task_id: &str) -> bool {
        let in_pending = {
            let pending = self.pending_job_ids.lock().await;
            pending.contains(task_id)
        };
        if in_pending {
            return false;
        }
        
        let in_failed = {
            let failed = self.failed_job_ids.lock().await;
            failed.contains(task_id)
        };
        if in_failed {
            return false;
        }
        
        let in_reschedule = {
            let reschedule = self.reschedule_job_ids.lock().await;
            reschedule.contains(task_id)
        };
        if in_reschedule {
            return false;
        }
        
        true
    }

    /// Get memory requirement from memory prediction
    fn get_job_memory(memory_pred: &Option<f64>) -> usize {
        memory_pred.unwrap_or(0.0) as usize
    }

    /// Get two jobs: first job (job1) and first job of different type (job2)
    /// where job1 + job2 <= memory_capacity
    /// Returns immediately when found, otherwise None
    /// Note: This function is never called during sequential run mode
    pub async fn get_two_jobs(&self, memory_capacity: usize) -> Option<(Job, Job)> {
        println!("[get_two_jobs] Called with memory_capacity: {} KB", memory_capacity);
        
        // Collect job IDs and their types
        let (job_ids, io_bound_task_ids, cpu_bound_task_ids): (
            Vec<String>,
            HashSet<String>,
            HashSet<String>
        ) = {
            let jobs_guard = self.jobs.lock().await;
            let io_bound_guard = self.io_bound_task_ids.lock().await;
            let cpu_bound_guard = self.cpu_bound_task_ids.lock().await;
            (jobs_guard.clone(), io_bound_guard.clone(), cpu_bound_guard.clone())
        };

        println!("[get_two_jobs] Total jobs available: {}", job_ids.len());

        // Find the first job (job1) - any job, we don't care if it's IO-bound, CPU-bound, or mixed
        let mut job1_id: Option<String> = None;
        let mut job1_memory: usize = 0;
        let mut job1_is_io_bound: bool = false;
        let mut job1_is_cpu_bound: bool = false;
        
        for task_id in &job_ids {
            // Check if job is eligible
            if !self.is_job_eligible(task_id).await {
                continue;
            }
            
            // Get job from HashMap
            let job = match self.get_job_by_id(task_id).await {
                Some(job) => job,
                None => continue,
            };
            
            // Get memory requirement
            let job_memory = Self::get_job_memory(&job.memory_prediction);
            
            // Found first job - determine its type
            job1_id = Some(task_id.clone());
            job1_memory = job_memory;
            job1_is_io_bound = io_bound_task_ids.contains(task_id);
            job1_is_cpu_bound = cpu_bound_task_ids.contains(task_id);
            break;
        }
        
        // If no job found, return None
        let job1_id = match job1_id {
            Some(id) => {
                let job1_type = if job1_is_io_bound {
                    "IO-bound"
                } else if job1_is_cpu_bound {
                    "CPU-bound"
                } else {
                    "Mixed"
                };
                println!("[get_two_jobs] Found job1: id={}, type={}, memory={} KB", 
                    id, job1_type, job1_memory);
                id
            },
            None => {
                println!("[get_two_jobs] No eligible job1 found, returning None");
                return None;
            },
        };
        
        println!("[get_two_jobs] Searching for job2 (different type than job1)");
        
        // Now find the first job of different type than job1 that fits with job1
        for task_id in &job_ids {
            // Skip if it's the same job as job1
            if task_id == &job1_id {
                continue;
            }
            
            // Determine job2's type
            let job2_is_io_bound = io_bound_task_ids.contains(task_id);
            let job2_is_cpu_bound = cpu_bound_task_ids.contains(task_id);
            
            // Job2 must be of different type than job1
            // If job1 is IO-bound, job2 must be CPU-bound (or vice versa)
            // If job1 is neither (truly mixed), job2 can be either IO-bound or CPU-bound
            let is_different_type = if job1_is_io_bound {
                // job1 is IO-bound, job2 must be CPU-bound
                job2_is_cpu_bound
            } else if job1_is_cpu_bound {
                // job1 is CPU-bound, job2 must be IO-bound
                job2_is_io_bound
            } else {
                // job1 is mixed (neither), job2 can be either IO-bound or CPU-bound
                job2_is_io_bound || job2_is_cpu_bound
            };
            
            if !is_different_type {
                continue;
            }
            
            // Check if job is eligible
            if !self.is_job_eligible(task_id).await {
                continue;
            }
            
            // Get job from HashMap
            let job2 = match self.get_job_by_id(task_id).await {
                Some(job) => job,
                None => continue,
            };
            
            // Get memory requirement
            let job2_memory = Self::get_job_memory(&job2.memory_prediction);
            
            let combined_memory = job1_memory + job2_memory;
            let job2_type = if job2_is_io_bound {
                "IO-bound"
            } else if job2_is_cpu_bound {
                "CPU-bound"
            } else {
                "Mixed"
            };
            
            println!("[get_two_jobs] Checking job2: id={}, type={}, memory={} KB, combined={} KB (capacity={} KB)", 
                task_id, job2_type, job2_memory, combined_memory, memory_capacity);
            
            // Check if combined memory fits
            if combined_memory <= memory_capacity {
                // Found a pair! Return both jobs
                let job1 = match self.get_job_by_id(&job1_id).await {
                    Some(job) => job,
                    None => return None, // Should not happen, but safety check
                };
                println!("[get_two_jobs] ✓ Found pair: job1={} ({} KB), job2={} ({} KB), total={} KB", 
                    job1.id, job1_memory, job2.id, job2_memory, combined_memory);
                return Some((job1, job2));
            } else {
                println!("[get_two_jobs] ✗ Combined memory {} KB exceeds capacity {} KB", 
                    combined_memory, memory_capacity);
            }
        }
        
        println!("[get_two_jobs] No compatible job2 found, returning None");
        None
    }

    pub async fn add_task(&self, task: Job) {
        let job_id = task.id.clone();
        
        // Add to job_map
        let mut job_map = self.job_map.lock().await;
        job_map.insert(job_id.clone(), task);
        drop(job_map);
        
        // Add job ID to jobs vector
        let mut guard = self.jobs.lock().await;
        guard.push(job_id);
        drop(guard);
        
        let mut counter = self.num_jobs.lock().await;
        *counter += 1;
    }

    /// Build memory buckets: divide jobs into buckets with approximately 25 jobs per bucket
    /// Number of buckets = num_jobs / 25 (minimum 1 bucket)
    /// Each bucket contains jobs within a memory range [min, max]
    /// Returns None if no jobs have memory predictions
    /// This should be called after jobs are updated with memory predictions
    pub async fn build_memory_buckets(&self) {
        let job_ids = self.jobs.lock().await;
        let num_jobs = job_ids.len();
        
        if num_jobs == 0 {
            let mut buckets = self.memory_buckets.lock().await;
            *buckets = None;
            return;
        }
        
        // Collect jobs with their IDs, memory predictions, and execution times
        let mut jobs_with_memory: Vec<(String, usize, f64)> = {
            let job_map = self.job_map.lock().await;
            job_ids.iter()
                .filter_map(|job_id| {
                    job_map.get(job_id).and_then(|job| {
                        job.memory_prediction.map(|mem| {
                            let time = job.execution_time_prediction.unwrap_or(0.0);
                            (job_id.clone(), mem as usize, time)
                        })
                    })
                })
                .collect()
        };
        
        drop(job_ids); // Release lock early
        
        if jobs_with_memory.is_empty() {
            let mut buckets = self.memory_buckets.lock().await;
            *buckets = None;
            return;
        }
        
        // Sort by memory to create buckets
        jobs_with_memory.sort_by_key(|(_, mem, _)| *mem);
        
        // Calculate number of buckets: num_jobs / 25, minimum 1 bucket
        // Last bucket can have more than 25 jobs due to ceiling division
        const JOBS_PER_BUCKET: usize = 25;
        let num_buckets = std::cmp::max(1, jobs_with_memory.len() / JOBS_PER_BUCKET);
        let jobs_per_bucket = if num_buckets > 0 {
            (jobs_with_memory.len() + num_buckets - 1) / num_buckets // Ceiling division
        } else {
            jobs_with_memory.len()
        };
        
        // Create buckets
        let mut buckets = Vec::new();
        for i in 0..num_buckets {
            let start_idx = i * jobs_per_bucket;
            let end_idx = std::cmp::min(start_idx + jobs_per_bucket, jobs_with_memory.len());
            
            if start_idx >= jobs_with_memory.len() {
                break;
            }
            
            let mut bucket_data: Vec<(String, f64)> = jobs_with_memory[start_idx..end_idx]
                .iter()
                .map(|(job_id, _, time)| (job_id.clone(), *time))
                .collect();
            
            if !bucket_data.is_empty() {
                let min_memory = jobs_with_memory[start_idx].1;
                let max_memory = jobs_with_memory[end_idx - 1].1;
                
                // Sort job IDs within bucket by execution time (shortest first)
                bucket_data.sort_by(|(_, time_a), (_, time_b)| {
                    time_a.partial_cmp(time_b).unwrap_or(std::cmp::Ordering::Equal)
                });
                
                let bucket_job_ids: Vec<String> = bucket_data.iter().map(|(job_id, _)| job_id.clone()).collect();
                
                buckets.push(MemoryBucket {
                    min_memory,
                    max_memory,
                    job_ids: bucket_job_ids,
                });
            }
        }
        
        // Print buckets information
        if !buckets.is_empty() {
            println!("[Memory Buckets] Created {} buckets:", buckets.len());
            for (i, bucket) in buckets.iter().enumerate() {
                println!("  Bucket {}: min_memory={} KB, max_memory={} KB, num_jobs={}", 
                    i, bucket.min_memory, bucket.max_memory, bucket.job_ids.len());
            }
        }
        
        // Store general buckets
        let mut buckets_guard = self.memory_buckets.lock().await;
        if buckets.is_empty() {
            *buckets_guard = None;
        } else {
            *buckets_guard = Some(buckets);
        }
        drop(buckets_guard);
        
        // Build separate IO-bound and CPU-bound buckets for faster selection
        self.build_typed_memory_buckets().await;
    }

    /// Build separate IO-bound and CPU-bound memory buckets
    async fn build_typed_memory_buckets(&self) {
        let job_ids = self.jobs.lock().await;
        let io_bound_ids = self.io_bound_task_ids.lock().await;
        let cpu_bound_ids = self.cpu_bound_task_ids.lock().await;
        
        // Collect IO-bound jobs with memory
        let mut io_jobs_with_memory: Vec<(String, usize)> = {
            let job_map = self.job_map.lock().await;
            job_ids.iter()
                .filter_map(|job_id| {
                    if io_bound_ids.contains(job_id) {
                        job_map.get(job_id).and_then(|job| {
                            job.memory_prediction.map(|mem| (job_id.clone(), mem as usize))
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        // Collect CPU-bound jobs with memory
        let mut cpu_jobs_with_memory: Vec<(String, usize)> = {
            let job_map = self.job_map.lock().await;
            job_ids.iter()
                .filter_map(|job_id| {
                    if cpu_bound_ids.contains(job_id) {
                        job_map.get(job_id).and_then(|job| {
                            job.memory_prediction.map(|mem| (job_id.clone(), mem as usize))
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        drop(job_ids);
        drop(io_bound_ids);
        drop(cpu_bound_ids);
        
        // Sort by memory
        io_jobs_with_memory.sort_by_key(|(_, mem)| *mem);
        cpu_jobs_with_memory.sort_by_key(|(_, mem)| *mem);
        
        const JOBS_PER_BUCKET: usize = 25;
        
        // Build IO buckets
        let io_buckets = self.build_buckets_from_sorted(&io_jobs_with_memory, JOBS_PER_BUCKET).await;
        let mut io_buckets_guard = self.io_memory_buckets.lock().await;
        *io_buckets_guard = if io_buckets.is_empty() { None } else { Some(io_buckets) };
        drop(io_buckets_guard);
        
        // Build CPU buckets
        let cpu_buckets = self.build_buckets_from_sorted(&cpu_jobs_with_memory, JOBS_PER_BUCKET).await;
        let mut cpu_buckets_guard = self.cpu_memory_buckets.lock().await;
        *cpu_buckets_guard = if cpu_buckets.is_empty() { None } else { Some(cpu_buckets) };
    }

    /// Helper to build buckets from sorted job list
    async fn build_buckets_from_sorted(&self, sorted_jobs: &[(String, usize)], jobs_per_bucket: usize) -> Vec<MemoryBucket> {
        if sorted_jobs.is_empty() {
            return Vec::new();
        }
        
        // Collect execution times while holding the lock once, create a HashMap for O(1) lookup
        let job_times_map: std::collections::HashMap<String, f64> = {
            let job_map = self.job_map.lock().await;
            sorted_jobs.iter()
                .filter_map(|(job_id, _)| {
                    job_map.get(job_id).map(|job| (job_id.clone(), job.execution_time_prediction.unwrap_or(0.0)))
                })
                .collect()
        };
        
        let num_buckets = std::cmp::max(1, sorted_jobs.len() / jobs_per_bucket);
        let actual_jobs_per_bucket = if num_buckets > 0 {
            (sorted_jobs.len() + num_buckets - 1) / num_buckets
        } else {
            sorted_jobs.len()
        };
        
        let mut buckets = Vec::new();
        for i in 0..num_buckets {
            let start_idx = i * actual_jobs_per_bucket;
            let end_idx = std::cmp::min(start_idx + actual_jobs_per_bucket, sorted_jobs.len());
            
            if start_idx >= sorted_jobs.len() {
                break;
            }
            
            let mut bucket_data: Vec<(String, f64)> = sorted_jobs[start_idx..end_idx]
                .iter()
                .filter_map(|(job_id, _)| {
                    job_times_map.get(job_id).map(|&time| (job_id.clone(), time))
                })
                .collect();
            
            if !bucket_data.is_empty() {
                let min_memory = sorted_jobs[start_idx].1;
                let max_memory = sorted_jobs[end_idx - 1].1;
                
                // Sort job IDs within bucket by execution time (shortest first)
                bucket_data.sort_by(|(_, time_a), (_, time_b)| {
                    time_a.partial_cmp(time_b).unwrap_or(std::cmp::Ordering::Equal)
                });
                
                let bucket_job_ids: Vec<String> = bucket_data.iter().map(|(job_id, _)| job_id.clone()).collect();
                
                buckets.push(MemoryBucket {
                    min_memory,
                    max_memory,
                    job_ids: bucket_job_ids,
                });
            }
        }
        
        buckets
    }


    pub async fn print_status(&self) {
        // Status printing removed - use print_status_summary for periodic updates
    }

    pub async fn reset_submitted_jobs(&self) {
        let mut pending = self.pending_job_ids.lock().await;
        pending.clear();
        drop(pending);
        
        let mut reschedule = self.reschedule_job_ids.lock().await;
        reschedule.clear();
        drop(reschedule);
        
        let mut successfull = self.successfull_job_ids.lock().await;
        successfull.clear();
        drop(successfull);
        
        let mut failed = self.failed_job_ids.lock().await;
        failed.clear();
        drop(failed);
    }

    /// Check if all jobs are completed and can terminate
    /// Returns true if:
    /// - jobs list is empty AND
    /// - (successfull_job_ids.len() > 0 OR failed_job_ids.len() > 0) AND
    /// - pending_job_ids is empty
    /// OR
    /// - jobs list is not empty AND
    /// - all jobs in the list are either in successful_job_ids or failed_job_ids
    pub async fn check_for_termination(&self) -> bool {
        let jobs = self.jobs.lock().await;
        let pending = self.pending_job_ids.lock().await;
        let successfull = self.successfull_job_ids.lock().await;
        let failed = self.failed_job_ids.lock().await;
        
        // Case 1: Jobs list is empty - check if all jobs completed and were removed
        if jobs.is_empty() {
            // Check if pending is empty
            if !pending.is_empty() {
                return false;
            }
            
            // Check if we have any successful or failed jobs
            let has_completed_jobs = !successfull.is_empty() || !failed.is_empty();
            return has_completed_jobs;
        }
        
        // Case 2: Jobs list is not empty - check if all jobs are in successful or failed sets
        // Check if all job IDs are in either successful or failed sets
        let all_in_success_or_failed = jobs.iter().all(|job_id| {
            successfull.contains(job_id) || failed.contains(job_id)
        });
        
        all_in_success_or_failed
    }

}
