mod utils;
mod scheduler;
mod worker;
mod optimized_scheduling_preprocessing;
mod wasm_loaders;
mod api;
use std::{sync::Arc};
use actix_web::web::Data;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer};
use core_affinity::{get_core_ids, CoreId};
use tokio::signal;
use std::sync::atomic::{AtomicBool, Ordering};
use serde::Deserialize;

use crate::scheduler::SchedulerEngine;
use crate::api::api_objects::{SubmittedJobs};
use crate::api::api_handlers::{handle_submit_task, handle_get_result, handle_predict_and_sort, handle_execute_tasks, handle_kill};


#[derive(Debug, Deserialize)]
struct Config {
    pin_cores: bool,
    num_workers: usize
}


fn load_config() -> Config {
    // Try to read from config.yaml
    match std::fs::read_to_string("config.yaml") {
        Ok(config_content) => {
            match serde_yaml::from_str::<Config>(&config_content) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("Warning: Failed to parse config.yaml: {}, using defaults", e);
                    Config {
                        pin_cores: false,
                        num_workers: 2,
                    }
                }
            }
        }
        Err(_) => {
            eprintln!("Warning: config.yaml not found, using defaults");
            Config {
                pin_cores: false,
                num_workers: 2,
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
    println!("pin_cores: {}, num_workers: {}", pin_cores, num_workers_to_start);

    // Global shutdown flag
    static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);
    
    // Initialize scheduler object and job logger 
    // Wrapped in web::Data so that we configure them as shared resources across HTTPServer threads 
    let jobs_log: web::Data<SubmittedJobs> = web::Data::new(SubmittedJobs::new());


    // Wrapped arouns Arc so that we keep one instance in the Heap and when doing .clone we only clone pointers
    // Mutex is needed cause some instance fields are mutable across threads
    let scheduler = { 
        let scheduler = Arc::new( Mutex::new(SchedulerEngine::new(core_ids, jobs_log.clone(), num_workers_to_start, pin_cores)));
        let scheduler_data: web::Data<Arc<Mutex<SchedulerEngine>>> = web::Data::new(scheduler.clone()); 
        scheduler_data
    };
    
    let worker_handlers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(vec![]));
    let handlers_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = web::Data::new(worker_handlers.clone());

    let scheduler_for_spawn = scheduler.clone();
    let worker_handlers_for_spawn = handlers_data.clone();
    let scheduler_for_shutdown = scheduler.clone();

    // Store the scheduler handle so we can abort it on shutdown
    let scheduler_handle = tokio::spawn(async move {
        let scheduler_arc = scheduler_for_spawn.get_ref().clone();
        match scheduler_for_spawn.lock().await.start_scheduler(scheduler_arc).await {
            Ok(handles) => {
                let mut wh = worker_handlers_for_spawn.lock().await;
                *wh = handles;
            }
            Err(e) => {
                eprintln!("Scheduler error: {:?}", e);
            }
        }
    });
     let handlers_data: Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = { 
        let h_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = web::Data::new(worker_handlers).clone(); 
        h_data
    };
    

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
            .app_data(handlers_data.clone());
        app = app.route("/submit_task", web::post().to(handle_submit_task));
        app = app.route("/get_result", web::get().to(handle_get_result));
        app = app.route("/predict_and_sort", web::post().to(handle_predict_and_sort));
        app = app.route("/run_tasks", web::post().to(handle_execute_tasks));
        app = app.route("/kill", web::get().to(handle_kill));
        app
    })
    .bind("[::]:8080")?
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


