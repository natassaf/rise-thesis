mod various;
mod scheduler;
mod all_tasks;

use std::{sync::Arc};

use various::{Job};
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use core_affinity::{get_core_ids, CoreId};


use crate::{scheduler::JobsScheduler, various::SubmittedJobs};


async fn handle_submit_task(task: web::Json<Job>, submitted_tasks: web::Data<SubmittedJobs>,scheduler: web::Data<Arc<JobsScheduler>>)->impl Responder {
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
    let jobs_log: web::Data<SubmittedJobs> = web::Data::new(SubmittedJobs::new());

    let scheduler = { 
        let scheduler = Arc::new(JobsScheduler::new(core_ids, jobs_log.clone()));
        let scheduler_data: web::Data<Arc<JobsScheduler>> = web::Data::new(scheduler.clone()); 
        scheduler_data
    };
    // Spawn scheduler as a background task that runs its loop continuously
    tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        async move {
            if let Err(e) = scheduler.start_scheduler().await {
                eprintln!("Scheduler error: {:?}", e);
            }
        }
    });

    // Add jobs logger and scheduler as shared objects
    HttpServer::new(move || {
        let app = App::new().app_data(jobs_log.clone()).app_data(scheduler.clone()).route("/submit_task", web::post().to(handle_submit_task));
        app
    }).bind("[::]:8080")?
    .run().await
}
