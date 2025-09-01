mod various;
mod scheduler;
mod worker;
mod wasm_loaders;
use std::{sync::Arc};
use actix_web::web::Data;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use core_affinity::{get_core_ids, CoreId};
use tokio::signal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::env;

use crate::scheduler::JobsScheduler;
use crate::various::{Job, SubmittedJobs, TaskQuery, WasmJobRequest};

async fn handle_kill(app_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>>)->impl Responder{
    for h in app_data.lock().await.iter(){
        h.abort();
    }
    HttpResponse::Ok().body(format!("Workers killed"))
}

async fn handle_execute_tasks(app_data: web::Data<Arc<Mutex<JobsScheduler>>>)->impl Responder{
    let mut scheduler = app_data.lock().await;
    scheduler.execute_jobs().await;
    HttpResponse::Ok().body(format!("Executing tasks"))
}

async fn handle_get_result(query: web::Query<TaskQuery>) -> impl Responder {
    println!("Running get result for task {}", query.id);
    
    // Return compressed result
    let compressed_path = format!("results/result_{}.gz", query.id);
    let uncompressed_path = format!("results/result_{}.txt", query.id);
    
    // Try compressed file first
    match std::fs::read(&compressed_path) {
        Ok(compressed_data) => {
            HttpResponse::Ok()
                .append_header(("Content-Type", "application/gzip"))
                .append_header(("Content-Encoding", "gzip"))
                .append_header(("Content-Disposition", format!("attachment; filename=\"result_{}.gz\"", query.id)))
                .body(compressed_data)
        },
        Err(_) => {
            // Fallback to uncompressed file
            match std::fs::read(&uncompressed_path) {
                Ok(uncompressed_data) => {
                    HttpResponse::Ok()
                        .header("Content-Type", "text/plain")
                        .header("Content-Disposition", format!("attachment; filename=\"result_{}.txt\"", query.id))
                        .body(uncompressed_data)
                },
                Err(_) => {
                    HttpResponse::NotFound().body("Result not found")
                }
            }
        }
    }
}

async fn handle_submit_task(task: web::Json<WasmJobRequest>, submitted_tasks: web::Data<SubmittedJobs>)->impl Responder {
    // Reads the json request and adds the job to the job logger. Returns response immediatelly to client
    // println!("task: {:?}", task);
    let job: Job = task.into_inner().into();
    submitted_tasks.add_task(job).await;
    // tokio::spawn(async move {
    //     scheduler.calculate_task_priorities().await;
    //     // let handles = scheduler.run_tasks_parallel().await;
    //     // for handle in handles {
    //     //     match handle.await {
    //     //         Ok(result) => {println!("Task completed with result: {}", result);}
    //     //         Err(e) => {eprintln!("Task failed: {}", e);}
    //     //     }
    //     // }    
    // });
    println!("Number of tasks waiting: {:?}", submitted_tasks.get_num_tasks().await);
    HttpResponse::Ok().body("Task submitted")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    unsafe {
        env::set_var("ONNX_DISABLE_SCHEMA_WARNINGS", "1");
    }

    println!("Server started");
    let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
    println!("core_ids: {:?}", core_ids);

    // Global shutdown flag
    static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

    // Initialize scheduler object and job logger 
    // Wrapped in web::Data so that we configure them as shared resources across HTTPServer threads 
    let jobs_log: web::Data<SubmittedJobs> = web::Data::new(SubmittedJobs::new());

    // Wrapped arouns Arc so that we keep one instance in the Heap and when doing .clone we only clone pointers
    // Mutex is needed cause some instance fields are mutable across threads
    let scheduler = { 
        let scheduler = Arc::new( Mutex::new(JobsScheduler::new(core_ids, jobs_log.clone())));
        let scheduler_data: web::Data<Arc<Mutex<JobsScheduler>>> = web::Data::new(scheduler.clone()); 
        scheduler_data
    };
    
    let worker_handlers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(vec![]));
    let handlers_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = web::Data::new(worker_handlers.clone());

    let scheduler_for_spawn = scheduler.clone();
    let worker_handlers_for_spawn = handlers_data.clone();
    let scheduler_for_shutdown = scheduler.clone();

    // Store the scheduler handle so we can abort it on shutdown
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
     let handlers_data: Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = { 
        let h_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = web::Data::new(worker_handlers).clone(); 
        h_data
    };
    

    // Create server with graceful shutdown
    let server = HttpServer::new(move || {
        let mut app = App::new().app_data(jobs_log.clone()).app_data(scheduler.clone()).app_data(handlers_data.clone()) ;
        app = app.route("/submit_task", web::post().to(handle_submit_task));
        app = app.route("/get_result", web::get().to(handle_get_result));
        app = app.route("/run_tasks", web::get().to(handle_execute_tasks));
        app = app.route("/kill", web::get().to(handle_kill));
        app
    })
    .bind("[::]:8080")?
    .shutdown_timeout(5) // 5 seconds timeout for graceful shutdown
    .run();

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
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        }
    }

    println!("Server shutdown complete");
    Ok(())
}


