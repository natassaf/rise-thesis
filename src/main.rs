mod various;
mod scheduler;
mod all_tasks;
mod worker;
mod wasm_loaders;

use std::fmt::Debug;
use std::{sync::Arc};
use tokio::sync::Mutex;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use core_affinity::{get_core_ids, CoreId};

use crate::scheduler::JobsScheduler;
use crate::various::{stored_result_decoder, SubmittedJobs, TaskQuery, WasmJob};

async fn handle_get_result(query: web::Query<TaskQuery>) -> impl Responder {
    println!("Running get result");
    match stored_result_decoder(query.id) {
        Some(result) => HttpResponse::Ok().body(format!("Result: {}", result)),
        None => HttpResponse::NotFound().body("Result not found"),
    }
}

async fn handle_submit_task(task: web::Json<WasmJob>, submitted_tasks: web::Data<SubmittedJobs>)->impl Responder {
    // Reads the json request and adds the job to the job logger. Returns response immediatelly to client
    submitted_tasks.add_task(task.into_inner()).await;
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
    println!("Server started");
    let core_ids: Vec<CoreId> = get_core_ids().expect("Failed to get core IDs");
    println!("core_ids: {:?}", core_ids);

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

    // Spawn scheduler as a background task that runs its loop continuously
    tokio::spawn({
        let s  = scheduler.clone();
        async move {
            if let Err(e) = s.lock().await.start_scheduler().await {
                eprintln!("Scheduler error: {:?}", e);
            }
        }
    });

    // Add jobs logger and scheduler as shared objects
    HttpServer::new(move || {
        let app = App::new().app_data(jobs_log.clone()).app_data(scheduler.clone()).route("/submit_task", web::post().to(handle_submit_task)).route("/get_result", web::get().to(handle_get_result));
        app
    }).bind("[::]:8080")?
    .run().await
}


