mod various;
use various::{Task};
use rand::Rng;
use std::sync::Arc;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use tokio::sync::Mutex;
use tokio::task;

#[derive(Debug, Clone)]
struct SubmittedTasks {
    tasks: Arc<Mutex<Vec<Task>>> ,
}

impl SubmittedTasks{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        Self{tasks}
    }

    pub async fn add_task(&self, task: Task) {
        let mut guard = self.tasks.lock().await;
        guard.push(task);
    }

    pub async fn calculate_task_priorities(&self) {
        let mut task_list = self.tasks.lock().await;
        let mut rng = rand::rng();

        for task in task_list.iter_mut() {
            task.priority = rng.random_range(1..4); 
        }

        println!("Updated task priorities: {:?}", *task_list);
    }
}

async fn handle_submit_task(task: web::Json<Task>, submitted_tasks: web::Data<SubmittedTasks>,)->impl Responder {
    // Adds the tasks to the vector and calculates priorities  
    println!("task submitted: {:?}", task);
    submitted_tasks.add_task(task.into_inner()).await;
    let submitted_tasks_clone = submitted_tasks.clone();
    task::spawn(async move {
        submitted_tasks_clone.calculate_task_priorities().await;
    });

    println!("tasks: {:?}", submitted_tasks);
    HttpResponse::Ok().body("Task submitted")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Server started");
    let tasks: web::Data<SubmittedTasks> = web::Data::new(SubmittedTasks::new());
    HttpServer::new(move || {
        let app = App::new().app_data(tasks.clone()).route("/submit_task", web::post().to(handle_submit_task));
        app
    }).bind("[::]:8080")?
    .run().await
}
