use std::collections::HashMap;

use actix_web::web;
use async_trait::async_trait;
use futures::future;

use crate::various::SubmittedJobs;
use crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction::predict_memory;
use crate::optimized_scheduling_preprocessing::features_extractor::build_memory_features;
use crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction_utils::MemoryFeatures;
use crate::optimized_scheduling_preprocessing::execution_time_prediction::time_prediction::predict_time;
use crate::optimized_scheduling_preprocessing::features_extractor::build_execution_time_features;

/// Save debug information about memory features and prediction to a file
fn save_debug_memory_prediction(job_id: &str, memory_features: &MemoryFeatures, memory_prediction: f64) {
    let debug_filename = format!("results/debug_memory_prediction_{}.txt", job_id);
    let memory_features_vec = memory_features.to_vec();
    let debug_content = format!(
        "Job ID: {}\n\
        Memory Features ({} values):\n{:?}\n\
        Memory Prediction: {}\n\
        Memory Features (as f32 vector):\n{:?}\n",
        job_id,
        memory_features_vec.len(),
        memory_features,
        memory_prediction,
        memory_features_vec
    );
    if let Err(e) = std::fs::write(&debug_filename, debug_content) {
        eprintln!("Failed to write debug file {}: {:?}", debug_filename, e);
    } else {
        println!("Debug info written to {}", debug_filename);
    }
}

#[async_trait]
pub trait SchedulerAlgorithm{
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>);
}


#[derive(Clone)]
pub struct BaselineStaticSchedulerAlgorithm{
}

impl BaselineStaticSchedulerAlgorithm{
    pub fn new() -> Self {
        Self{}
    }

    // Sort jobs by arrival time (oldest first)
    async fn sort_by_arrival_time(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        let mut jobs = submitted_jobs.jobs.lock().await;
        jobs.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
    }
}

#[async_trait]
impl SchedulerAlgorithm for BaselineStaticSchedulerAlgorithm{
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

impl MemoryTimeAwareSchedulerAlgorithm{
    pub fn new() -> Self {
        Self{}
    }

    async fn predict_memory(&self, cwasm_file: &str, wat_file: &str, payload: &str, folder_to_mount: &str)->f64{
        let memory_features = build_memory_features(&cwasm_file, &wat_file, &payload, &folder_to_mount).await;
        let memory_features_vec = memory_features.to_vec();
        let memory_prediction = predict_memory(&memory_features_vec).await;
        return memory_prediction
    }
    async fn predict_time(&self, cwasm_file: &str, wat_file: &str, payload: &str, folder_to_mount: &str)->f64{
        let time_features = build_execution_time_features(&cwasm_file, &wat_file, &payload, &folder_to_mount).await;
        let time_features_vec = time_features.to_vec();
        let time_prediction = predict_time(&time_features_vec).await;
        return time_prediction
    }
}

#[async_trait]
impl SchedulerAlgorithm for MemoryTimeAwareSchedulerAlgorithm{
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        // for each job predict memory and time requirements
        let jobs = submitted_jobs.get_jobs().await;
        let job_ids_before: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
        println!("Sorting jobs by execution time and memory before: {:?}", job_ids_before);
        
        let futures: Vec<_> = jobs.iter().map(|job| {
            let cwasm_file = job.binary_path.replace(".wasm", ".cwasm");
            let wat_file = job.binary_path.replace(".wasm", ".wat");
            let payload = job.payload.clone();
            let folder_to_mount = job.folder_to_mount.clone();
            let job_id = job.id.clone();
            
            // async move is needed here because:
            // 1. The closure is collected into a Vec and will outlive the iterator
            // 2. We need to take ownership of the cloned values (cwasm_file, wat_file, etc.)
            // 3. The future will be awaited later, so we can't borrow from the outer scope
            async move {
                let memory_prediction = self.predict_memory(&cwasm_file, &wat_file, &payload, &folder_to_mount).await;
                let time_prediction = self.predict_time(&cwasm_file, &wat_file, &payload, &folder_to_mount).await;
                // Debug: Store memory_features and memory_prediction to results directory
                // save_debug_memory_prediction(&job_id, &memory_features, memory_prediction);
                
                (job_id, memory_prediction, time_prediction)
            }
        }).collect();
        
        let predictions: Vec<(String, f64, f64)> = future::join_all(futures).await;
        let mut job_id_to_memory_prediction: HashMap<String, f64> = HashMap::new();
        let mut job_id_to_time_prediction: HashMap<String, f64> = HashMap::new();
        
        for (job_id, memory_pred, time_pred) in predictions {
            job_id_to_memory_prediction.insert(job_id.clone(), memory_pred);
            job_id_to_time_prediction.insert(job_id, time_pred);
        }
        
        println!("job_id_to_memory_prediction: {:?}", job_id_to_memory_prediction);
        println!("job_id_to_time_prediction: {:?}", job_id_to_time_prediction);
        
        // Update jobs with memory and time predictions
        let mut jobs = submitted_jobs.jobs.lock().await;
        for job in jobs.iter_mut() {
            if let Some(prediction) = job_id_to_memory_prediction.get(&job.id) {
                job.memory_prediction = Some(*prediction);
            }
            if let Some(prediction) = job_id_to_time_prediction.get(&job.id) {
                job.execution_time_prediction = Some(*prediction);
            }
        }
        
        // Sort jobs:
        // 1. First by execution time from largest to shortest (descending)
        // 2. Then by memory prediction from shortest to largest (ascending)
        // This way, when we pop() from the end, we get the job with shortest time and largest memory
        jobs.sort_by(|a, b| {
            let a_time = a.execution_time_prediction.unwrap_or(0.0);
            let b_time = b.execution_time_prediction.unwrap_or(0.0);
            let a_mem = a.memory_prediction.unwrap_or(0.0);
            let b_mem = b.memory_prediction.unwrap_or(0.0);
            
            // First sort by execution time: descending (largest first, shortest last)
            match b_time.partial_cmp(&a_time) {
                Some(std::cmp::Ordering::Equal) => {
                    // If execution times are equal, sort by memory: ascending (smallest first, largest last)
                    a_mem.partial_cmp(&b_mem).unwrap_or(std::cmp::Ordering::Equal)
                }
                Some(ordering) => ordering,
                None => std::cmp::Ordering::Equal,
            }
        });
        
        // Use the jobs we already have locked instead of calling get_jobs() again
        let job_ids_after: Vec<_> = jobs.iter().map(|job| job.id.clone()).collect();
        println!("Sorting jobs by execution time (desc) and memory (asc) after: {:?}", job_ids_after);
    }
    
}

