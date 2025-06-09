mod various;
mod scheduler;
mod all_tasks;
mod worker;

use std::{sync::Arc};

use tokio::sync::Mutex;
use various::{Job};
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use core_affinity::{get_core_ids, CoreId};
use serde::Deserialize;

use crate::{scheduler::JobsScheduler, various::SubmittedJobs};


use std::fs;
use std::io::{ErrorKind};

pub fn read_result_by_id(id: usize) -> Option<String> {
    let path = format!("results/result_{}.txt", id);
    
    match fs::read_to_string(&path) {
        Ok(content) => {
            // Expect format: "Result: <value>"
            if let Some(line) = content.lines().find(|line| line.starts_with("Result: ")) {
                Some(line.trim_start_matches("Result: ").trim().to_string())
            } else {
                None
            }
        },
        Err(e) => {
            match e.kind() {
                ErrorKind::NotFound => None,
                _ => {
                    eprintln!("Error reading result file {}: {:?}", path, e);
                    None
                }
            }
        }
    }
}


#[derive(Deserialize)]
struct TaskQuery {
    id: usize,
}

async fn handle_get_result(query: web::Query<TaskQuery>) -> impl Responder {
    println!("Running get result");
    match read_result_by_id(query.id) {
        Some(result) => HttpResponse::Ok().body(format!("Result: {}", result)),
        None => HttpResponse::NotFound().body("Result not found"),
    }
}

async fn handle_submit_task(task: web::Json<Job>, submitted_tasks: web::Data<SubmittedJobs>)->impl Responder {
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
