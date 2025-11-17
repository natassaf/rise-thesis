use std::collections::HashMap;

use actix_web::web;
use async_trait::async_trait;
use rayon::prelude::*;

use crate::api::api_objects::{SubmittedJobs, Job};
use crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction::predict_memory_batch;
use crate::optimized_scheduling_preprocessing::features_extractor::build_all_features;
use crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction_utils::MemoryFeatures;
use crate::optimized_scheduling_preprocessing::execution_time_prediction::time_prediction::predict_time_batch;

/// Check how many jobs have changed index after sorting
fn count_jobs_with_changed_index(job_ids_before: &[String], job_ids_after: &[String]) -> usize {
    let mut changed_count = 0;
    
    for (index, job_id) in job_ids_after.iter().enumerate() {
        if let Some(original_index) = job_ids_before.iter().position(|id| id == job_id) {
            if original_index != index {
                changed_count += 1;
            }
        }
    }
    
    changed_count
}

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

    /// Extract features for all jobs in parallel using rayon
    fn extract_features_parallel(jobs: &[Job]) -> Vec<(String, Vec<f32>, Vec<f32>)> {
        jobs
            .par_iter()
            .map(|job| {
                let cwasm_file = job.binary_path.replace(".wasm", ".cwasm");
                let wat_file = job.binary_path.replace(".wasm", ".wat");
                let payload = job.payload.clone();
                let folder_to_mount = job.folder_to_mount.clone();
                let job_id = job.id.clone();
                
                let (memory_features, time_features) = build_all_features(&cwasm_file, &wat_file, &payload, &folder_to_mount);
                (job_id, memory_features.to_vec(), time_features.to_vec())
            })
            .collect()
    }
}

#[async_trait]
impl SchedulerAlgorithm for MemoryTimeAwareSchedulerAlgorithm{
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        // Configuration: batch size for predictions
        const BATCH_SIZE: usize = 150;
        
        // for each job predict memory and time requirements
        let jobs = submitted_jobs.get_jobs().await;
        let job_ids_before: Vec<_> = submitted_jobs.get_jobs().await.iter().map(|job| job.id.clone()).collect();
        // println!("Sorting jobs by execution time and memory before: {:?}", job_ids_before);
        
        if jobs.is_empty() {
            return;
        }
        
        // Measure total time for feature extraction and prediction
        let total_start = std::time::Instant::now();
        
        // Extract features for all jobs in parallel
        let feature_results = Self::extract_features_parallel(&jobs);
        
        // Process predictions in batches
        let mut job_id_to_memory_prediction: HashMap<String, f64> = HashMap::new();
        let mut job_id_to_time_prediction: HashMap<String, f64> = HashMap::new();
        
        // Process in batches of BATCH_SIZE
        for batch_start in (0..feature_results.len()).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, feature_results.len());
            let batch = &feature_results[batch_start..batch_end];
            
            // Collect features for this batch
            let mut batch_job_ids: Vec<String> = Vec::new();
            let mut memory_features_batch: Vec<Vec<f32>> = Vec::new();
            let mut time_features_batch: Vec<Vec<f32>> = Vec::new();
            
            for (job_id, memory_features, time_features) in batch {
                batch_job_ids.push(job_id.clone());
                memory_features_batch.push(memory_features.clone());
                time_features_batch.push(time_features.clone());
            }
            
            println!("Processing prediction batch {}/{} ({} jobs)", 
                     batch_start / BATCH_SIZE + 1, 
                     (feature_results.len() + BATCH_SIZE - 1) / BATCH_SIZE,
                     batch_job_ids.len());
            
            // Run batch predictions
            let memory_predictions = predict_memory_batch(&memory_features_batch).await;
            let time_predictions = predict_time_batch(&time_features_batch).await;
            
            // Store results
            for (i, job_id) in batch_job_ids.iter().enumerate() {
                if i < memory_predictions.len() {
                    job_id_to_memory_prediction.insert(job_id.clone(), memory_predictions[i]);
                }
                if i < time_predictions.len() {
                    job_id_to_time_prediction.insert(job_id.clone(), time_predictions[i]);
                }
            }
        }
        
        let total_duration = total_start.elapsed();
        let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        std::fs::write(format!("results/timing_{}.txt", timestamp), format!("Total time: {:.2}ms\nJobs: {}", total_duration.as_secs_f64() * 1000.0, jobs.len())).unwrap_or_else(|e| eprintln!("Failed to write timing: {:?}", e));
        
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
        // println!("Sorting jobs by execution time (desc) and memory (asc) after: {:?}", job_ids_after);
        
        // Check how many jobs have changed index after sorting
        let changed_count = count_jobs_with_changed_index(&job_ids_before, &job_ids_after);
        println!("[ORDER CHECK] Jobs with changed index: {} / {}", changed_count, job_ids_before.len());
    }
    
}

