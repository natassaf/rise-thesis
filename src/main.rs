mod api;
mod channel_objects;
mod evaluation_metrics;
mod optimized_scheduling_preprocessing;
mod scheduler;
mod utils;
mod wasm_loaders;
mod worker;
use crate::api::api_handlers::{
    handle_execute_tasks, handle_get_result, handle_kill, handle_predict_and_sort,
    handle_submit_task,
};
use crate::api::api_objects::SubmittedJobs;
use crate::channel_objects::Message;
use crate::scheduler::SchedulerEngine;
use actix_web::web::Data;
use actix_web::{App, HttpServer, web};
use core_affinity::{CoreId, get_core_ids};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::signal;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct Config {
    pin_cores: bool,
    num_workers: usize,
    num_concurrent_tasks: Option<usize>, // Optional, defaults to 1 if not specified
}

fn load_config() -> Config {
    // Try to read from config.yaml
    match std::fs::read_to_string("config.yaml") {
        Ok(config_content) => match serde_yaml::from_str::<Config>(&config_content) {
            Ok(config) => config,
            Err(e) => {
                eprintln!(
                    "Warning: Failed to parse config.yaml: {}, using defaults",
                    e
                );
                Config {
                    pin_cores: false,
                    num_workers: 2,
                    num_concurrent_tasks: Some(1),
                }
            }
        },
        Err(_) => {
            eprintln!("Warning: config.yaml not found, using defaults");
            Config {
                pin_cores: false,
                num_workers: 2,
                num_concurrent_tasks: Some(1),
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Server started");
    let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
    println!("core_ids: {:?}", core_ids);

    // Load config from config.yaml file to get the number of workers/threads to spawn and whether it's cores should be pinned
    let config = load_config();
    let pin_cores = config.pin_cores;
    let num_workers_to_start = config.num_workers;
    let num_concurrent_tasks = config.num_concurrent_tasks.unwrap_or(1);
    println!(
        "pin_cores: {}, num_workers: {}, num_concurrent_tasks: {}",
        pin_cores, num_workers_to_start, num_concurrent_tasks
    );

    // Global shutdown flag
    static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

    // Initialize scheduler object and job logger
    // Wrapped in web::Data so that we configure them as shared resources across HTTPServer threads 
    // Jobs will be added here with submit jobs endpoint and the scheduler will be able to see the updated vector, pull the jobs and add then into channels for the workers to pull from and process
    let jobs_log: web::Data<SubmittedJobs> = web::Data::new(SubmittedJobs::new());

    // Wrapped arouns Arc cause we want to share and mutate scheduler across threads
    // Mutex is needed cause some instance fields are mutable across threads
    let scheduler = {
        let scheduler = Arc::new(Mutex::new(SchedulerEngine::new(
            core_ids,
            jobs_log.clone(),
            num_workers_to_start,
            pin_cores,
            num_concurrent_tasks,
        )));
        let scheduler_data: web::Data<Arc<Mutex<SchedulerEngine>>> =
            web::Data::new(scheduler.clone());
        scheduler_data
    };


    let worker_handlers: web::Data<Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>> = {
        let worker_handlers: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(vec![]));
        web::Data::new(worker_handlers.clone())
    };
        

    let scheduler_for_spawn = scheduler.clone();
    let worker_handlers_for_spawn = worker_handlers.clone();
    let scheduler_for_shutdown = scheduler.clone();

    // Store the workers handlers and the scheduler handle so we can abort it on shutdown
    let scheduler_handle = tokio::spawn(async move {
        match scheduler_for_spawn.lock().await.start_scheduler().await {
            Ok(handles) => {
                let mut wh = worker_handlers_for_spawn.lock().await;
                *wh = handles;
            }
            Err(e) => {
                eprintln!("Scheduler error: {:?}", e);
            }
        }
    });
    
    // Create server with graceful shutdown
    let server = HttpServer::new(move || {
        // Configure JSON payload size limit (default is 256KB, increase to 100MB for large image payloads)
        let json_config = web::JsonConfig::default()
            .limit(100 * 1024 * 1024) // 100MB limit for JSON payloads
            .content_type_required(false); // Allow different content types if needed

        let mut app = App::new()
            .app_data(json_config) // Apply JSON config globally
            .app_data(jobs_log.clone())
            .app_data(scheduler.clone())
            .app_data(worker_handlers.clone());
        app = app.route("/submit_task", web::post().to(handle_submit_task));
        app = app.route("/get_result", web::get().to(handle_get_result));
        app = app.route("/predict_and_sort", web::post().to(handle_predict_and_sort));
        app = app.route("/run_tasks", web::post().to(handle_execute_tasks));
        app = app.route("/kill", web::get().to(handle_kill));
        app
    })
    .bind("[::]:8080")?
    .workers(2)
    .shutdown_timeout(5) // 5 seconds timeout for graceful shutdown
    .run();

    // ******* ENABLES SHUTDOWN *******
    // Wait for either the server to complete or a shutdown signal
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
            // Wait a bit for cleanup
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // Exit the process -0 is a success code sent to the OS for clean shutdown
            std::process::exit(0);
        }
    }

    println!("Server shutdown complete");
    Ok(())
}
