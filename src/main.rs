mod api;
mod channel_objects;
mod evaluation_metrics;
mod optimized_scheduling_preprocessing;
mod scheduler;
mod utils;
mod wasm_loaders;
mod worker;
pub mod jobs_order_optimizer;
mod memory_monitoring;

use crate::api::api_handlers::{
    handle_execute_tasks, handle_get_result, handle_kill, handle_predict_and_sort,
    handle_submit_task,
};
use crate::api::api_objects::SubmittedJobs;
use crate::evaluation_metrics::EvaluationMetrics;
use crate::jobs_order_optimizer::JobsOrderOptimizer;
use crate::scheduler::SchedulerEngine;
use actix_web::{App, HttpServer, web};
use core_affinity::{CoreId, get_core_ids};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::signal;
use tokio::sync::{Mutex, Notify};
use utils::load_config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Starts the http server with the required shared data. 
    // Web server endpoints share the following app_data: 
        // job_logs (stores the submitted tasks), 
        // evaluation_metrics: Is initialized on execute tasks, calculates and stores total execution time, average response time per task, throughput
        // workers_notification_channel: Channel to notify workers to start / restart 
        // jobs_order_optimizer: Objects used to sort tasks before execution

    println!("Server started");
    let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
    println!("core_ids: {:?}", core_ids);

    // Load config from config.yaml file to get the number of workers/threads to spawn and whether it's cores should be pinned
    let config = load_config();
    let pin_cores = config.pin_cores;
    let num_workers_to_start = config.num_workers;
    let num_concurrent_tasks = config.num_concurrent_tasks.unwrap_or(1);
    println!("pin_cores: {}, num_workers: {}, num_concurrent_tasks: {}", pin_cores, num_workers_to_start, num_concurrent_tasks);

    // Global shutdown flag
    static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false); // used to shutdown the workers and scheduler running on the background

    // Initialize the app data
    let jobs_log: web::Data<SubmittedJobs> = web::Data::new(SubmittedJobs::new());
    let workers_notification_channel = Arc::new(Notify::new());
    let evaluation_metrics = Arc::new(EvaluationMetrics::new());
    let request_running_flag = web::Data::new(Mutex::new(false));
    
    // Create and start scheduler. Scheduler
    let scheduler = {
        let scheduler = Arc::new(Mutex::new(SchedulerEngine::new(
            core_ids,
            jobs_log.clone(),
            num_workers_to_start,
            pin_cores,
            num_concurrent_tasks,
            workers_notification_channel.clone(),
            evaluation_metrics.clone()
        )));
        let scheduler_data: web::Data<Arc<Mutex<SchedulerEngine>>> =
            web::Data::new(scheduler.clone());
        scheduler_data
    };
    
    let scheduler_for_shutdown = scheduler.clone();

    // Store the workers handlers and the scheduler handle so we can abort it on shutdown
    let scheduler_handle = tokio::spawn(async move {
        scheduler.lock().await.start_workers().await.expect("Failed to start workers");
        scheduler.lock().await.start().await;
    });
    

    // Create server with graceful shutdown
    let server = HttpServer::new(move || {
        let jobs_order_optimizer = web::Data::new(Arc::new(Mutex::new(JobsOrderOptimizer::new(jobs_log.clone()))));
        // Configure JSON payload size limit (default is 256KB, increase to 100MB for large image payloads)
        let json_config = web::JsonConfig::default()
            .limit(100 * 1024 * 1024) // 100MB limit for JSON payloads
            .content_type_required(false); // Allow different content types if needed

        let mut app = App::new()
            .app_data(json_config) 
            .app_data(jobs_log.clone())
            .app_data(web::Data::new(workers_notification_channel.clone()))
            .app_data(web::Data::new(evaluation_metrics.clone()))
            .app_data(jobs_order_optimizer.clone());
            // .app_data(request_running_flag.clone());

        app = app.route("/submit_task", web::post().to(handle_submit_task));
        app = app.route("/get_result", web::get().to(handle_get_result));
        app = app.route("/predict_and_sort", web::post().to(handle_predict_and_sort));
        app = app.route("/run_tasks", web::post().to(handle_execute_tasks));
        app = app.route("/kill", web::get().to(handle_kill));
        app
    })
    .bind("[::]:8080")?
    .workers(2)
    .shutdown_timeout(5)
    .run();

    // ******* ENABLES SHUTDOWN *******
    // The main thread blocks here and waits for either the server to complete or a shutdown signal
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down gracefully...");
            // Set global shutdown flag
            SHUTDOWN_FLAG.store(true, Ordering::Relaxed);

            // Signal shutdown to scheduler
            if let Ok(mut sched) = scheduler_for_shutdown.get_ref().try_lock() {
                sched.shutdown().await;
            }

            // Abort the scheduler task
            scheduler_handle.abort();
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // Exit the process: 0 is a success code sent to the OS for clean shutdown
            std::process::exit(0);
        }
    }

    println!("Server shutdown complete");
    Ok(())
}
