use crate::api::api_objects::{ExecuteTasksRequest, Job, SubmittedJobs, TaskQuery, WasmJobRequest};
use crate::evaluation_metrics::{EvaluationMetrics};
use crate::jobs_order_optimizer::{JobsOrderOptimizer};
use actix_web::{HttpResponse, Responder, web};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

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
    println!("Predicting and sorting tasks with {} algorithm", scheduling_algorithm);
    jobs_order_optimizer.predict_and_sort(scheduling_algorithm).await;
    HttpResponse::Ok().body(format!("Predictions and sorting completed"))
}

pub async fn handle_execute_tasks(
    evaluation_metrics: web::Data<Arc<EvaluationMetrics>>,
    jobs_logs:web::Data<SubmittedJobs>,
    workers_notification_channel: web::Data<Arc<Notify>>,
) -> impl Responder {
    let task_ids: Vec<String> = jobs_logs.get_jobs().await.iter().map(|j| j.id.clone()).collect();
 
    // if no tasks were submitted don't notify the worker to start looping
    if !task_ids.is_empty(){
        evaluation_metrics.initialize(task_ids).await;
        workers_notification_channel.notify_waiters();
    }
    HttpResponse::Ok().body(format!("Executing tasks"))
}

pub async fn handle_get_result(query: web::Query<TaskQuery>) -> impl Responder {
    println!("Running get result for task {}", query.id);
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
    println!(
        "Number of tasks waiting: {:?}",
        submitted_tasks.get_num_tasks().await
    );
    HttpResponse::Ok().body("Task submitted")
}
