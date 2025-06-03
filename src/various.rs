use serde::{Deserialize, Deserializer};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize)]
struct TaskInput {
    name: String,
    n: u64,
}


#[derive(Debug, Clone)]
pub struct Task{
    pub name:String,
    n: u64,
    pub priority: u64
}

impl<'de> Deserialize<'de> for Task {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = TaskInput::deserialize(deserializer)?;
        let priority = 0;
        Ok(Task {
            name: input.name,
            n: input.n,
            priority,
        })
    }
}



#[derive(Debug, Clone)]
pub struct SubmittedTasks {
    pub tasks: Arc<Mutex<Vec<Task>>> ,
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