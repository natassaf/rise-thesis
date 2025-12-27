use std::collections::HashMap;
use actix_web::web;
use async_trait::async_trait;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::api::api_objects::{Job, SubmittedJobs};
use crate::optimized_scheduling_preprocessing::execution_time_prediction::time_prediction::predict_time_batch;
use crate::optimized_scheduling_preprocessing::features_extractor::{
    TaskBoundType, build_all_features,
};
use crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction::predict_memory_batch;
use crate::optimized_scheduling_preprocessing::task_type_prediction::task_type_prediction::predict_task_type_batch;
use crate::optimized_scheduling_preprocessing::task_type_prediction::task_type_prediction_utils::TaskTypeFeatures;

#[derive(Serialize, Deserialize)]
struct PredictionData {
    memory_prediction: f64,
    time_prediction: f64,
    task_bound_type: TaskBoundType,
}

/// Load predictions from JSON file - optimized to only load needed predictions
fn load_predictions_from_file(
    file_path: &str,
    needed_job_ids: &[String],
) -> Result<(HashMap<String, f64>, HashMap<String, f64>, HashMap<String, TaskBoundType>), String> {
    let content = std::fs::read_to_string(file_path)
        .map_err(|e| format!("Failed to read predictions file {}: {}", file_path, e))?;
    
    let all_predictions: HashMap<String, PredictionData> = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse predictions file: {}", e))?;
    
    // Only create HashMaps for the jobs we actually need
    let mut job_id_to_memory_prediction: HashMap<String, f64> = HashMap::with_capacity(needed_job_ids.len());
    let mut job_id_to_time_prediction: HashMap<String, f64> = HashMap::with_capacity(needed_job_ids.len());
    let mut job_id_to_task_bound_type: HashMap<String, TaskBoundType> = HashMap::with_capacity(needed_job_ids.len());
    
    // Only extract predictions for jobs we need
    for job_id in needed_job_ids {
        if let Some(pred_data) = all_predictions.get(job_id) {
            job_id_to_memory_prediction.insert(job_id.clone(), pred_data.memory_prediction);
            job_id_to_time_prediction.insert(job_id.clone(), pred_data.time_prediction);
            job_id_to_task_bound_type.insert(job_id.clone(), pred_data.task_bound_type);
        }
    }
    
    // Explicitly drop the large all_predictions HashMap to free memory immediately
    drop(all_predictions);
    drop(content);
    
    Ok((job_id_to_memory_prediction, job_id_to_time_prediction, job_id_to_task_bound_type))
}

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


#[derive(Debug, Clone)]
pub struct SchedulerAlgorithmUtils{}

impl SchedulerAlgorithmUtils{

    pub fn new()->Self{
        Self{}
    }

    /// Extract features for all jobs in parallel using rayon
    pub fn extract_features_parallel(&self, jobs: &[Job]) -> Vec<(String, Vec<f32>, Vec<f32>, Vec<f32>)> {
        jobs.par_iter()
            .map(|job| {
                let cwasm_file = job.binary_path.replace(".wasm", ".cwasm");
                let wat_file = job.binary_path.replace(".wasm", ".wat");
                let payload = job.payload.clone();
                let folder_to_mount = job.folder_to_mount.clone();
                let job_id = job.id.clone();

                let (memory_features, time_features) =
                    build_all_features(&cwasm_file, &wat_file, &payload, &folder_to_mount);
                let task_type_features = TaskTypeFeatures::from_memory_features(&memory_features);
                (
                    job_id,
                    memory_features.to_vec(),
                    time_features.to_vec(),
                    task_type_features.to_vec(),
                )
            })
            .collect()
    }
    
    /// Process predictions in batches and return memory and time predictions
    pub async fn process_predictions_in_batches(&self,
        feature_results: &[(String, Vec<f32>, Vec<f32>, Vec<f32>)],
        batch_size: usize,
    ) -> (
        HashMap<String, f64>,
        HashMap<String, f64>,
        HashMap<String, TaskBoundType>,
    ) {
        let mut job_id_to_memory_prediction: HashMap<String, f64> = HashMap::new();
        let mut job_id_to_time_prediction: HashMap<String, f64> = HashMap::new();
        let mut job_id_to_task_bound_type: HashMap<String, TaskBoundType> = HashMap::new();

        // Process in batches of BATCH_SIZE
        for batch_start in (0..feature_results.len()).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, feature_results.len());
            let batch = &feature_results[batch_start..batch_end];

            // Collect features for this batch
            let mut batch_job_ids: Vec<String> = Vec::new();
            let mut memory_features_batch: Vec<Vec<f32>> = Vec::new();
            let mut time_features_batch: Vec<Vec<f32>> = Vec::new();
            let mut task_type_features_batch = Vec::new();
            for (job_id, memory_features, time_features, task_type_features) in batch {
                batch_job_ids.push(job_id.clone());
                memory_features_batch.push(memory_features.clone());
                time_features_batch.push(time_features.clone());
                task_type_features_batch.push(task_type_features.clone());
            }

            println!(
                "Processing prediction batch {}/{} ({} jobs)",
                batch_start / batch_size + 1,
                (feature_results.len() + batch_size - 1) / batch_size,
                batch_job_ids.len()
            );

            // Run batch predictions
            let memory_predictions = predict_memory_batch(&memory_features_batch).await;
            let time_predictions = predict_time_batch(&time_features_batch).await;
            let task_type_prediction = predict_task_type_batch(&task_type_features_batch).await;
            // Store results
            for (i, job_id) in batch_job_ids.iter().enumerate() {

                job_id_to_memory_prediction.insert(job_id.clone(), memory_predictions[i]);
                job_id_to_time_prediction.insert(job_id.clone(), time_predictions[i]);
                job_id_to_task_bound_type.insert(job_id.clone(), task_type_prediction[i]);
                
            }
        }

        (
            job_id_to_memory_prediction,
            job_id_to_time_prediction,
            job_id_to_task_bound_type,
        )
    }
}


#[async_trait]
pub trait SchedulerAlgorithm {
    async fn prioritize_tasks(
        &self,
        submitted_jobs: &web::Data<SubmittedJobs>,
    ) -> (Vec<String>, Vec<String>);
}

#[derive(Debug, Clone)]
pub struct Improvement1{}

impl Improvement1{
    pub fn new()->Self{
        Self{}
    }
}

#[async_trait]
impl SchedulerAlgorithm for Improvement1{
    async fn prioritize_tasks(&self, submitted_jobs: &web::Data<SubmittedJobs>)-> (Vec<String>, Vec<String>){
        // Get jobs
        let jobs = submitted_jobs.get_jobs().await;

        if jobs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Collect job IDs first to only load predictions we need
        let job_ids: Vec<String> = jobs.iter().map(|job| job.id.clone()).collect();

        // Load predictions from file - only for jobs we need (no feature extraction or model loading)
        const PREDICTIONS_FILE: &str = "feature_predictions/predictions.json";
        
        // Use scoping to ensure predictions HashMaps are dropped after use
        let (cpu_bound_task_ids, io_bound_task_ids) = {
            let (job_id_to_memory_prediction, job_id_to_time_prediction, job_id_to_task_bound_type) = 
                load_predictions_from_file(PREDICTIONS_FILE, &job_ids)
                    .unwrap_or_else(|e| {
                        eprintln!("[ERROR] Failed to load predictions from {}: {}", PREDICTIONS_FILE, e);
                        eprintln!("[ERROR] Predictions file is required. Exiting.");
                        std::process::exit(1);
                    });
            
            println!("Loaded predictions for {} jobs from {} (no feature extraction or model loading)", job_ids.len(), PREDICTIONS_FILE);

            // Update jobs with memory predictions, time predictions and task bound type (no parallel iterator needed)
            let mut jobs = submitted_jobs.jobs.lock().await;
            for job in jobs.iter_mut() {
                if let Some(prediction) = job_id_to_memory_prediction.get(&job.id) {
                    job.memory_prediction = Some(*prediction);
                }
                if let Some(prediction) = job_id_to_time_prediction.get(&job.id) {
                    job.execution_time_prediction = Some(*prediction);
                }
                if let Some(bound_type) = job_id_to_task_bound_type.get(&job.id) {
                    job.task_bound_type = Some(*bound_type);
                }
            }
            
            // Separate jobs into CPU-bound and I/O-bound task ID vectors (maintaining sort order)
            let mut cpu_bound_task_ids: Vec<String> = Vec::new();
            let mut io_bound_task_ids: Vec<String> = Vec::new();

            for job in jobs.iter() {
                match job.task_bound_type {
                    Some(TaskBoundType::CpuBound) => {
                        cpu_bound_task_ids.push(job.id.clone());
                    }
                    Some(TaskBoundType::IoBound) => {
                        io_bound_task_ids.push(job.id.clone());
                    }
                    _ => {
                        // For Mixed or None, we can decide based on heuristics or add to both
                        // For now, let's add Mixed tasks to CPU-bound as a default
                        cpu_bound_task_ids.push(job.id.clone());
                    }
                }
            }
            
            // Store separated task ID sets in SubmittedJobs
            submitted_jobs
                .set_cpu_bound_task_ids(cpu_bound_task_ids.clone())
                .await;
            submitted_jobs
                .set_io_bound_task_ids(io_bound_task_ids.clone())
                .await;

            println!(
                "[TASK SEPARATION] CPU-bound tasks: {}, I/O-bound tasks: {}",
                cpu_bound_task_ids.len(),
                io_bound_task_ids.len()
            );
            
            // Explicitly drop HashMaps after sorting to free memory
            drop(job_id_to_memory_prediction);
            drop(job_id_to_time_prediction);
            drop(job_id_to_task_bound_type);
            
            (cpu_bound_task_ids, io_bound_task_ids)
        };
        
        // Drop job_ids vector to free memory
        drop(job_ids);
        
        // Return the separated task ID vectors
        (io_bound_task_ids, cpu_bound_task_ids)
    }
}


#[derive(Debug, Clone)]
pub struct Improvement2{}

impl Improvement2{
    pub fn new()->Self{
        Self{}
    }
}

#[async_trait]
impl SchedulerAlgorithm for Improvement2{
    async fn prioritize_tasks(
        &self,
        submitted_jobs: &web::Data<SubmittedJobs>,
    ) -> (Vec<String>, Vec<String>) {
        // Get jobs
        let jobs = submitted_jobs.get_jobs().await;
        let job_ids_before: Vec<_> = jobs
            .iter()
            .map(|job| job.id.clone())
            .collect();

        if jobs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Collect job IDs to only load predictions we need
        let job_ids: Vec<String> = jobs.iter().map(|job| job.id.clone()).collect();

        // Load predictions from file - only for jobs we need (no feature extraction or model loading)
        const PREDICTIONS_FILE: &str = "feature_predictions/predictions.json";
        
        // Use scoping to ensure predictions HashMaps are dropped after use
        let (first_half, second_half) = {
            let (_job_id_to_memory_prediction, job_id_to_time_prediction, job_id_to_task_bound_type) = 
                load_predictions_from_file(PREDICTIONS_FILE, &job_ids)
                    .unwrap_or_else(|e| {
                        eprintln!("[ERROR] Failed to load predictions from {}: {}", PREDICTIONS_FILE, e);
                        eprintln!("[ERROR] Predictions file is required. Exiting.");
                        std::process::exit(1);
                    });
            
            println!("Loaded predictions for {} jobs from {} (no feature extraction or model loading)", job_ids.len(), PREDICTIONS_FILE);

            // Update jobs with time predictions and task bound type (no parallel iterator needed)
            let mut jobs = submitted_jobs.jobs.lock().await;
            for job in jobs.iter_mut() {
                if let Some(prediction) = job_id_to_time_prediction.get(&job.id) {
                    job.execution_time_prediction = Some(*prediction);
                }
                if let Some(bound_type) = job_id_to_task_bound_type.get(&job.id) {
                    job.task_bound_type = Some(*bound_type);
                }
            }

            // Sort jobs by execution time from largest to shortest (descending)
            // This way, when we pop() from the end, we get the job with shortest time
            jobs.sort_by(|a, b| {
                let a_time = a.execution_time_prediction.unwrap_or(0.0);
                let b_time = b.execution_time_prediction.unwrap_or(0.0);

                // Sort by execution time: descending (shortest first, largest last)
                a_time.partial_cmp(&b_time).unwrap_or(std::cmp::Ordering::Equal)
            });

            // Use the jobs we already have locked instead of calling get_jobs() again
            let job_ids_after: Vec<_> = jobs.iter().map(|job| job.id.clone()).collect();

            // Sanity check: how many jobs have changed index after sorting
            let changed_count = count_jobs_with_changed_index(&job_ids_before, &job_ids_after);
            println!(
                "[ORDER CHECK] Jobs with changed index: {} / {}",
                changed_count,
                job_ids_before.len()
            );

            let jobs_len = jobs.len();
            let mut first_half: Vec<String> = Vec::new();
            let mut second_half: Vec<String> = Vec::new();
            
            for (i, job) in jobs.iter().enumerate() {
                if i < jobs_len / 2 {
                    first_half.push(job.id.clone());
                } else {
                    second_half.push(job.id.clone());
                }
            }
            
            // Store separated task ID sets in SubmittedJob
            submitted_jobs
                .set_cpu_bound_task_ids(first_half.clone())
                .await;
            submitted_jobs
                .set_io_bound_task_ids(second_half.clone())
                .await;

            println!(
                "[TASK SEPARATION] CPU-bound tasks: {}, I/O-bound tasks: {}",
                first_half.len(),
                second_half.len()
            );
            
            // Explicitly drop HashMaps after sorting to free memory
            drop(job_id_to_time_prediction);
            drop(job_id_to_task_bound_type);
            
            // job_ids_after is also dropped here
            (first_half, second_half)
        };
        
        // Drop temporary vectors to free memory
        drop(job_ids);
        drop(job_ids_before);

        // Return the separated task ID vectors
        (first_half, second_half)
    }
 }


#[derive(Clone)]
pub struct BaselineStaticSchedulerAlgorithm {}

impl BaselineStaticSchedulerAlgorithm {
    pub fn new() -> Self {
        Self {}
    }

    // Sort jobs by arrival time (oldest first)
    async fn sort_by_arrival_time(&self, submitted_jobs: &web::Data<SubmittedJobs>) {
        let mut jobs = submitted_jobs.jobs.lock().await;
        jobs.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
    }
}

#[async_trait]
impl SchedulerAlgorithm for BaselineStaticSchedulerAlgorithm {
    async fn prioritize_tasks(
        &self,
        submitted_jobs: &web::Data<SubmittedJobs>,
    ) -> (Vec<String>, Vec<String>) {
        self.sort_by_arrival_time(submitted_jobs).await;

        // Separate jobs into CPU-bound and I/O-bound task ID vectors (maintaining sort order)
        let mut first_hald_jobs: Vec<String> = Vec::new();
        let mut second_half_jobs: Vec<String> = Vec::new();
        let jobs = submitted_jobs.get_jobs().await.to_vec();
        let num_jobs = jobs.len();
        for (i, job) in jobs.iter().enumerate() {
            if i < num_jobs / 2 {
                first_hald_jobs.push(job.id.clone());
            } else {
                second_half_jobs.push(job.id.clone());
            }
        }

        // Store separated task ID sets in SubmittedJobs
        submitted_jobs
            .set_cpu_bound_task_ids(first_hald_jobs.clone())
            .await;
        submitted_jobs
            .set_io_bound_task_ids(second_half_jobs.clone())
            .await;

        (first_hald_jobs, second_half_jobs)
    }
}

pub struct MemoryTimeAwareSchedulerAlgorithm {
    utils: SchedulerAlgorithmUtils
}

impl MemoryTimeAwareSchedulerAlgorithm {
    pub fn new() -> Self {
        let utils = SchedulerAlgorithmUtils::new();
        Self {utils}
    }
}

#[async_trait]
impl SchedulerAlgorithm for MemoryTimeAwareSchedulerAlgorithm {
    async fn prioritize_tasks(
        &self,
        submitted_jobs: &web::Data<SubmittedJobs>,
    ) -> (Vec<String>, Vec<String>) {
        // Configuration: batch size for predictions
        const BATCH_SIZE: usize = 20;

        // for each job predict memory and time requirements
        let jobs = submitted_jobs.get_jobs().await;
        let job_ids_before: Vec<_> = submitted_jobs
            .get_jobs()
            .await
            .iter()
            .map(|job| job.id.clone())
            .collect();

        if jobs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Load predictions from file instead of computing them
        const PREDICTIONS_FILE: &str = "feature_predictions/predictions.json";
        
        // Collect job IDs to only load predictions we need
        let job_ids: Vec<String> = jobs.iter().map(|job| job.id.clone()).collect();
        
        // Use scoping to ensure predictions HashMaps are dropped after use
        let (cpu_bound_task_ids, io_bound_task_ids) = {
            let (job_id_to_memory_prediction, job_id_to_time_prediction, job_id_to_task_bound_type) = 
                match load_predictions_from_file(PREDICTIONS_FILE, &job_ids) {
                    Ok(predictions) => {
                        println!("Loaded predictions from {}", PREDICTIONS_FILE);
                        predictions
                    }
                    Err(e) => {
                        eprintln!("Failed to load predictions from file: {}. Falling back to computing predictions.", e);
                        // Fallback to computing predictions if file doesn't exist
                        let total_start = std::time::Instant::now();
                        let feature_results = self.utils.extract_features_parallel(&jobs);
                        let predictions = self.utils.process_predictions_in_batches(&feature_results, BATCH_SIZE).await;
                        let total_duration = total_start.elapsed();
                        println!("Computed predictions in {:.2}ms (fallback mode)", total_duration.as_secs_f64() * 1000.0);
                        predictions
                    }
                };

            println!(
                "job_id_to_memory_prediction: {:?}",
                job_id_to_memory_prediction
            );
            println!("job_id_to_time_prediction: {:?}", job_id_to_time_prediction);

            // Update jobs with memory, time predictions, and task bound type (no parallel iterator needed)
            let mut jobs = submitted_jobs.jobs.lock().await;
            for job in jobs.iter_mut() {
                if let Some(prediction) = job_id_to_memory_prediction.get(&job.id) {
                    job.memory_prediction = Some(*prediction);
                }
                if let Some(prediction) = job_id_to_time_prediction.get(&job.id) {
                    job.execution_time_prediction = Some(*prediction);
                }
                if let Some(bound_type) = job_id_to_task_bound_type.get(&job.id) {
                    job.task_bound_type = Some(*bound_type);
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

                // First sort by execution time: descending (shortesr first, largest last)
                match a_time.partial_cmp(&b_time) {
                    Some(std::cmp::Ordering::Equal) => {
                        // If execution times are equal, sort by memory: ascending (smallest first, largest last)
                        a_mem
                            .partial_cmp(&b_mem)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                    Some(ordering) => ordering,
                    None => std::cmp::Ordering::Equal,
                }
            });

            // Use the jobs we already have locked instead of calling get_jobs() again
            let job_ids_after: Vec<_> = jobs.iter().map(|job| job.id.clone()).collect();

            // Sanity check: how many jobs have changed index after sorting
            let changed_count = count_jobs_with_changed_index(&job_ids_before, &job_ids_after);
            println!(
                "[ORDER CHECK] Jobs with changed index: {} / {}",
                changed_count,
                job_ids_before.len()
            );

            // Separate jobs into CPU-bound and I/O-bound task ID vectors (maintaining sort order)
            let mut cpu_bound_task_ids: Vec<String> = Vec::new();
            let mut io_bound_task_ids: Vec<String> = Vec::new();

            for job in jobs.iter() {
                match job.task_bound_type {
                    Some(TaskBoundType::CpuBound) => {
                        cpu_bound_task_ids.push(job.id.clone());
                    }
                    Some(TaskBoundType::IoBound) => {
                        io_bound_task_ids.push(job.id.clone());
                    }
                    _ => {
                        // For Mixed or None, we can decide based on heuristics or add to both
                        // For now, let's add Mixed tasks to CPU-bound as a default
                        cpu_bound_task_ids.push(job.id.clone());
                    }
                }
            }

            // Store separated task ID sets in SubmittedJobs
            submitted_jobs
                .set_cpu_bound_task_ids(cpu_bound_task_ids.clone())
                .await;
            submitted_jobs
                .set_io_bound_task_ids(io_bound_task_ids.clone())
                .await;

            println!(
                "[TASK SEPARATION] CPU-bound tasks: {}, I/O-bound tasks: {}",
                cpu_bound_task_ids.len(),
                io_bound_task_ids.len()
            );
            
            // Explicitly drop HashMaps after sorting to free memory
            // Keep job_id_to_memory_prediction as it may be needed later
            drop(job_id_to_time_prediction);
            drop(job_id_to_task_bound_type);
            
            // job_ids_after is also dropped here
            (cpu_bound_task_ids, io_bound_task_ids)
        };
        
        // Drop temporary vectors to free memory
        drop(job_ids);
        drop(job_ids_before);

        // Return the separated task ID vectors
        (io_bound_task_ids, cpu_bound_task_ids)
    }
}
