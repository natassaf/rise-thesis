mod various;
mod scheduler;
mod all_tasks;
mod worker;
mod wasm_loaders;
use std::{sync::Arc};
use actix_web::web::Data;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use core_affinity::{get_core_ids, CoreId};

use crate::scheduler::JobsScheduler;
use crate::various::{stored_result_decoder, SubmittedJobs, TaskQuery, WasmJob};

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
    
    let worker_handlers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(vec![]));
    let handlers_data: web::Data<Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>> = web::Data::new(worker_handlers.clone());

    let scheduler_for_spawn = scheduler.clone();
    let worker_handlers_for_spawn = handlers_data.clone();

    tokio::spawn(async move {
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
    

     HttpServer::new(move || {
        let mut app = App::new().app_data(jobs_log.clone()).app_data(scheduler.clone()).app_data(handlers_data.clone()) ;
        app = app.route("/submit_task", web::post().to(handle_submit_task));
        app = app.route("/get_result", web::get().to(handle_get_result));
        app = app.route("/run_tasks", web::get().to(handle_execute_tasks));
        app = app.route("/kill", web::get().to(handle_kill));
        app
    }).bind("[::]:8080")?
    .run().await
}


