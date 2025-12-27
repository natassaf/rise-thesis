use crate::api::api_objects::{ExecuteTasksRequest, Job, SubmittedJobs, TaskQuery, WasmJobRequest};
use crate::evaluation_metrics::{EvaluationMetrics};
use crate::jobs_order_optimizer::{JobsOrderOptimizer};
use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;
use actix_web::{HttpResponse, Responder, web};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Mutex, Notify};
use serde::{Deserialize, Serialize};

pub async fn handle_kill(
    app_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>>,
) -> impl Responder {
    for h in app_data.lock().await.iter() {
        h.abort();
    }
    HttpResponse::Ok().body(format!("Workers killed"))
}

pub async fn handle_predict_and_sort(
    task: web::Json<ExecuteTasksRequest>,
    app_data: web::Data<Arc<Mutex<JobsOrderOptimizer>>>,
) -> impl Responder {
    let jobs_order_optimizer = app_data.lock().await;
    let scheduling_algorithm = task.into_inner().scheduling_algorithm;
    jobs_order_optimizer.predict_and_sort(scheduling_algorithm).await;
    drop(jobs_order_optimizer);
    HttpResponse::Ok().body(format!("Predictions and sorting completed"))
}

pub async fn handle_execute_tasks(
    evaluation_metrics: web::Data<Arc<EvaluationMetrics>>,
    jobs_logs:web::Data<SubmittedJobs>,
    workers_notification_channel: web::Data<Arc<Notify>>,
    // request_running_flag:web::Data<Mutex<bool>>
) -> impl Responder {
    let task_ids: Vec<String> = jobs_logs.get_jobs().await.iter().map(|j| j.id.clone()).collect();

    // if no tasks were submitted don't notify the worker to start looping
    // let mut running_flag = request_running_flag.lock().await;
    if !task_ids.is_empty() {
        // *running_flag = true;
        evaluation_metrics.initialize(task_ids).await;
        workers_notification_channel.notify_waiters();

    }

    HttpResponse::Ok().body(format!("Executing tasks"))
}

pub async fn handle_get_result(query: web::Query<TaskQuery>) -> impl Responder {
    // Return compressed result
    let compressed_path = format!("results/result_{}.gz", query.id);
    let uncompressed_path = format!("results/result_{}.txt", query.id);

    // Try compressed file first
    match std::fs::read(&compressed_path) {
        Ok(compressed_data) => HttpResponse::Ok()
            .append_header(("Content-Type", "application/gzip"))
            .append_header(("Content-Encoding", "gzip"))
            .append_header((
                "Content-Disposition",
                format!("attachment; filename=\"result_{}.gz\"", query.id),
            ))
            .body(compressed_data),
        Err(_) => {
            // Fallback to uncompressed file
            match std::fs::read(&uncompressed_path) {
                Ok(uncompressed_data) => HttpResponse::Ok()
                    .append_header(("Content-Type", "text/plain"))
                    .append_header((
                        "Content-Disposition",
                        format!("attachment; filename=\"result_{}.txt\"", query.id),
                    ))
                    .body(uncompressed_data),
                Err(_) => HttpResponse::NotFound().body("Result not found"),
            }
        }
    }
}

pub async fn handle_submit_task(
    task: web::Json<WasmJobRequest>,
    submitted_tasks: web::Data<SubmittedJobs>,
) -> impl Responder {
    // Reads the json request and adds the job to the job logger. Returns response immediatelly to client
    let job: Job = task.into_inner().into();
    submitted_tasks.add_task(job).await;
    println!("Submitted tasks count: {}", submitted_tasks.num_jobs.lock().await);
    HttpResponse::Ok().body("Task submitted")
}

#[derive(Serialize, Deserialize)]
struct PredictionData {
    memory_prediction: f64,
    time_prediction: f64,
    task_bound_type: TaskBoundType,
}

/// Generate predictions for all submitted jobs and save to file
pub async fn handle_generate_predictions(
    _app_data: web::Data<Arc<Mutex<JobsOrderOptimizer>>>,
    submitted_jobs: web::Data<SubmittedJobs>,
) -> impl Responder {
    
    const BATCH_SIZE: usize = 20;
    const PREDICTIONS_FILE: &str = "feature_predictions/predictions.json";

    let jobs = submitted_jobs.get_jobs().await;
    
    if jobs.is_empty() {
        return HttpResponse::BadRequest().body("No jobs submitted");
    }

    // Start timing
    let start_time = std::time::Instant::now();

    // Create utils instance to extract features and run predictions
    use crate::optimized_scheduling_preprocessing::scheduler_algorithms::SchedulerAlgorithmUtils;
    let utils = SchedulerAlgorithmUtils::new();

    // Extract features for all jobs in parallel
    let feature_results = utils.extract_features_parallel(&jobs);

    // Process predictions in batches
    let (job_id_to_memory_prediction, job_id_to_time_prediction, job_id_to_task_bound_type) =
        utils.process_predictions_in_batches(&feature_results, BATCH_SIZE).await;

    // Calculate timing metrics
    let total_time = start_time.elapsed();
    let num_tasks = jobs.len();
    let avg_time_per_task = if num_tasks > 0 {
        total_time.as_secs_f64() / num_tasks as f64
    } else {
        0.0
    };

    // Combine predictions into a single HashMap
    let mut predictions: HashMap<String, PredictionData> = HashMap::new();
    
    for job_id in job_id_to_memory_prediction.keys() {
        let memory_pred = job_id_to_memory_prediction.get(job_id).copied().unwrap_or(0.0);
        let time_pred = job_id_to_time_prediction.get(job_id).copied().unwrap_or(0.0);
        let bound_type = job_id_to_task_bound_type.get(job_id).copied().unwrap_or(TaskBoundType::Mixed);
        
        predictions.insert(
            job_id.clone(),
            PredictionData {
                memory_prediction: memory_pred,
                time_prediction: time_pred,
                task_bound_type: bound_type,
            },
        );
    }

    // Save to JSON file
    let json_str = match serde_json::to_string_pretty(&predictions) {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().body(format!("Failed to serialize predictions: {}", e)),
    };
    
    // Ensure results directory exists
    if let Err(e) = std::fs::create_dir_all("results") {
        return HttpResponse::InternalServerError().body(format!("Failed to create results directory: {}", e));
    }
    
    if let Err(e) = std::fs::write(PREDICTIONS_FILE, json_str) {
        return HttpResponse::InternalServerError().body(format!("Failed to write predictions file: {}", e));
    }

    HttpResponse::Ok().body(format!(
        "Predictions generated and saved to {}. Total time: {:.3}s, Average time per task: {:.3}s ({} tasks)",
        PREDICTIONS_FILE, total_time.as_secs_f64(), avg_time_per_task, num_tasks
    ))
}
