use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;
use crate::memory_monitoring::get_available_memory_kb;
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
    pub num_jobs: Arc<Mutex<usize>>,
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
            num_jobs: Arc::new(Mutex::new(0)),
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

    /// Move the next job from jobs list (that is not in pending) to reschedule_job_ids
    pub async fn move_next_job_to_reschedule(&self) -> bool {
        // First, collect all job IDs while holding only the jobs lock
        let job_ids: Vec<String> = {
            let jobs = self.jobs.lock().await;
            jobs.iter().map(|job| job.id.clone()).collect()
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
        let all_in_reschedule = jobs.iter().all(|job| reschedule.contains(&job.id));
        
        all_in_reschedule
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
        // Collect only minimal data needed: job indices, IDs, memory predictions (no large payloads)
        let (job_data, io_bound_task_ids): (Vec<(usize, String, Option<f64>)>, HashSet<String>) = {
            let jobs_guard = self.jobs.lock().await;
            let io_bound_guard = self.io_bound_task_ids.lock().await;
            let job_data: Vec<(usize, String, Option<f64>)> = jobs_guard.iter()
                .enumerate()
                .map(|(idx, job)| (idx, job.id.clone(), job.memory_prediction))
                .collect();
            (job_data, io_bound_guard.clone())
        };
        
        for (idx, task_id, memory_pred) in &job_data {
            // Check if job is IO-bound (quick check, no lock needed)
            if !io_bound_task_ids.contains(task_id) {
                continue;
            }
            
            // Check if job is in pending (acquire lock, check, release)
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(task_id)
            };
            if in_pending {
                continue;
            }
            
            // Check if job has failed (acquire lock, check, release)
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(task_id)
            };
            if in_failed {
                continue;
            }
            
            // Check if job is in reschedule to avoid picking it when we are not on 
            // the retry iteration (sequential_run_flag: true)
            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(task_id)
                };
                if in_reschedule {
                    continue;
                }
            }
            
            // Check memory capacity. 
            // In sequential run we are trying the task anyway
            let job_memory = if sequential_run_flag {
                0
            } else {
                memory_pred.unwrap_or(0.0) as usize
            };
            
            if job_memory <= memory_capacity {
                // Since we found a job we can clone it and send it to the worker
                let jobs_guard = self.jobs.lock().await;
                return Some(jobs_guard[*idx].clone());
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
        // Collect only minimal data needed: job indices, IDs, memory predictions (no large payloads)
        let (job_data, cpu_bound_task_ids): (Vec<(usize, String, Option<f64>)>, HashSet<String>) = {
            let jobs_guard = self.jobs.lock().await;
            let cpu_bound_guard = self.cpu_bound_task_ids.lock().await;
            let job_data: Vec<(usize, String, Option<f64>)> = jobs_guard.iter()
                .enumerate()
                .map(|(idx, job)| (idx, job.id.clone(), job.memory_prediction))
                .collect();
            (job_data, cpu_bound_guard.clone())
        };
        
        // Now iterate through job data (no large payloads in memory)
        for (idx, task_id, memory_pred) in &job_data {
            // Check if job is CPU-bound (quick check, no lock needed)
            if !cpu_bound_task_ids.contains(task_id) {
                continue;
            }
            
            // Check if job is in pending (acquire lock, check, release)
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(task_id)
            };
            if in_pending {
                continue;
            }
            
            // Check if job has failed (acquire lock, check, release)
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(task_id)
            };
            if in_failed {
                continue;
            }
            
            // Check if job is in reschedule to avoid picking it when we are not on 
            // the retry iteration (sequential_run_flag: true)
            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(task_id)
                };
                if in_reschedule {
                    continue;
                }
            }
            
            // Check memory capacity
            let job_memory = if sequential_run_flag {
                0
            } else {
                memory_pred.unwrap_or(0.0) as usize
            };
            
            if job_memory <= memory_capacity {
                // Only NOW clone the full Job object when we found a match
                let jobs_guard = self.jobs.lock().await;
                return Some(jobs_guard[*idx].clone());
            }
        }
        None
    }

     /// Get the next job from the jobs list
    /// The job will be removed when a success message is received
    pub async fn get_next_job(&self, memory_capacity: usize, sequential_run_flag:bool) -> Option<Job> {
        // Collect only minimal data needed: job indices, IDs, memory predictions (no large payloads)
        let job_data: Vec<(usize, String, Option<f64>)> = {
            let jobs_guard = self.jobs.lock().await;
            jobs_guard.iter()
                .enumerate()
                .map(|(idx, job)| (idx, job.id.clone(), job.memory_prediction))
                .collect()
        };
        
        // Now iterate through job data (no large payloads in memory)
        for (idx, job_id, memory_pred) in &job_data {
            // Check if job is in pending (acquire lock, check, release)
            let in_pending = {
                let pending = self.pending_job_ids.lock().await;
                pending.contains(job_id)
            };
            if in_pending {
                continue;
            }
            
            // Check if job has failed (acquire lock, check, release)
            let in_failed = {
                let failed = self.failed_job_ids.lock().await;
                failed.contains(job_id)
            };
            if in_failed {
                continue;
            }

            // Check if job is in reschedule to avoid picking it when we are not on 
            // the retry iteration (sequential_run_flag: true)
            if !sequential_run_flag {
                let in_reschedule = {
                    let reschedule = self.reschedule_job_ids.lock().await;
                    reschedule.contains(job_id)
                };
                if in_reschedule {
                    continue;
                }
            }

            // Check memory capacity
            let job_memory = if sequential_run_flag {
                0
            } else {
                memory_pred.unwrap_or(0.0) as usize
            };

            if job_memory <= memory_capacity {
                // Only NOW clone the full Job object when we found a match
                let jobs_guard = self.jobs.lock().await;
                return Some(jobs_guard[*idx].clone());
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
        
        // Collect job data with their types
        let (job_data, io_bound_task_ids, cpu_bound_task_ids): (
            Vec<(usize, String, Option<f64>)>,
            HashSet<String>,
            HashSet<String>
        ) = {
            let jobs_guard = self.jobs.lock().await;
            let io_bound_guard = self.io_bound_task_ids.lock().await;
            let cpu_bound_guard = self.cpu_bound_task_ids.lock().await;
            let job_data: Vec<(usize, String, Option<f64>)> = jobs_guard.iter()
                .enumerate()
                .map(|(idx, job)| (idx, job.id.clone(), job.memory_prediction))
                .collect();
            (job_data, io_bound_guard.clone(), cpu_bound_guard.clone())
        };

        println!("[get_two_jobs] Total jobs available: {}", job_data.len());

        // Find the first job (job1) - any job, we don't care if it's IO-bound, CPU-bound, or mixed
        let mut job1_idx: Option<usize> = None;
        let mut job1_memory: usize = 0;
        let mut job1_is_io_bound: bool = false;
        let mut job1_is_cpu_bound: bool = false;
        let mut job1_id: String = String::new();
        
        for (idx, task_id, memory_pred) in &job_data {
            // Check if job is eligible
            if !self.is_job_eligible(task_id).await {
                continue;
            }
            
            // Get memory requirement
            let job_memory = Self::get_job_memory(memory_pred);
            
            // Found first job - determine its type
            job1_idx = Some(*idx);
            job1_memory = job_memory;
            job1_id = task_id.clone();
            job1_is_io_bound = io_bound_task_ids.contains(task_id);
            job1_is_cpu_bound = cpu_bound_task_ids.contains(task_id);
            break;
        }
        
        // If no job found, return None
        let job1_idx = match job1_idx {
            Some(idx) => {
                let job1_type = if job1_is_io_bound {
                    "IO-bound"
                } else if job1_is_cpu_bound {
                    "CPU-bound"
                } else {
                    "Mixed"
                };
                println!("[get_two_jobs] Found job1: id={}, type={}, memory={} KB", 
                    job1_id, job1_type, job1_memory);
                idx
            },
            None => {
                println!("[get_two_jobs] No eligible job1 found, returning None");
                return None;
            },
        };
        
        println!("[get_two_jobs] Searching for job2 (different type than job1)");
        
        // Now find the first job of different type than job1 that fits with job1
        for (idx, task_id, memory_pred) in &job_data {
            // Skip if it's the same job as job1
            if *idx == job1_idx {
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
            
            // Get memory requirement
            let job2_memory = Self::get_job_memory(memory_pred);
            
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
                let jobs_guard = self.jobs.lock().await;
                let job1 = jobs_guard[job1_idx].clone();
                let job2 = jobs_guard[*idx].clone();
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
        let mut guard = self.jobs.lock().await;
        guard.push(task);
        let mut counter = self.num_jobs.lock().await;
        *counter += 1;
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
        let all_in_success_or_failed = jobs.iter().all(|job| {
            successfull.contains(&job.id) || failed.contains(&job.id)
        });
        
        all_in_success_or_failed
    }

}
