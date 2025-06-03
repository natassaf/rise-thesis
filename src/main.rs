mod various;
mod scheduler;

use std::{sync::Arc, thread, time::Duration};

use various::{Task};
use actix_web::{web, App, HttpResponse, HttpServer, Responder};


use crate::{scheduler::TaskScheduler, various::SubmittedTasks};


async fn handle_submit_task(task: web::Json<Task>, submitted_tasks: web::Data<SubmittedTasks>,scheduler: web::Data<Arc<TaskScheduler>>)->impl Responder {
    // Adds the tasks to the vector with priorities 0  
    println!("task submitted: {:?}", task);
    submitted_tasks.add_task(task.into_inner()).await;
    thread::sleep(Duration::from_secs(2));
    println!("tasks: {:?}", submitted_tasks);
    scheduler.calculate_task_priorities().await;
    let res = scheduler.run_tasks().await;
    println!("tasks after scheduler: {:?}", submitted_tasks);
    HttpResponse::Ok().body("Task submitted")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Server started");
    let submitted_tasks: web::Data<SubmittedTasks> = web::Data::new(SubmittedTasks::new());
    let num_cores = 4;
    let scheduler = Arc::new(TaskScheduler::new(num_cores, submitted_tasks.clone()));
    let scheduler_data = web::Data::new(scheduler.clone()); 
    HttpServer::new(move || {
        let app = App::new().app_data(submitted_tasks.clone()).app_data(scheduler_data.clone()).route("/submit_task", web::post().to(handle_submit_task));
        app
    }).bind("[::]:8080")?
    .run().await
}
