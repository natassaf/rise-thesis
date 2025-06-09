use serde::{Deserialize, Deserializer};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);


#[derive(Deserialize)]
struct JobInput {
    name: String,
    n: usize,
}


#[derive(Debug, Clone)]
pub struct Job{
    pub name:String,
    pub n: usize,
    priority: u64,
    pub id: usize
}
 
 impl Job{
     pub fn set_priority(&mut self, priority: u64) {
        self.priority = priority;
    }

    pub fn get_priority(&mut self) {
        self.priority;
    }
}

impl<'de> Deserialize<'de> for Job {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = JobInput::deserialize(deserializer)?;
        let priority: u64 = 0;
        let id:usize= TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed).try_into().unwrap();
        Ok(Job {
            name: input.name,
            n: input.n,
            id:id,
            priority,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<Job>>> ,
}

impl SubmittedJobs{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        Self{jobs: tasks}
    }

    pub async fn remove_job(&self, job_id: usize) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|job| job.id != job_id);
    }

    pub async fn get_num_tasks(&self)->usize{
        let guard = self.jobs.lock();
        guard.await.len()
    }
    
    pub async fn get_jobs(&self)->Vec<Job>{
        self.jobs.lock().await.to_vec()
    }

    pub async fn add_task(&self, task: Job) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

}

