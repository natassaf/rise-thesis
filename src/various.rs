use serde::{Deserialize, Deserializer};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
pub struct TaskQuery {
    pub id: usize,
}


#[derive(Deserialize, Clone, Debug)]
pub struct JobInput {
    pub task_type: String,
    pub n: usize,
    pub id: usize
}


#[derive(Debug, Clone)]
pub struct WasmJob{
    pub job_input: JobInput,
    pub binary_path:Option<String>,
    pub wat_path:Option<String>,
}

 impl WasmJob{
    pub fn new(binary_path: Option<String>, wat_path: Option<String>, job_input: JobInput)->Self{
        WasmJob{binary_path, wat_path, job_input}
    }
 }


#[derive(Debug, Clone)]
pub struct Job{
    pub name:String,
    pub n: usize,
    priority: u64,
    pub id: usize,
}
 

impl<'de> Deserialize<'de> for WasmJob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = JobInput::deserialize(deserializer)?;
        let priority: u64 = 0;
        let id:usize= TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed).try_into().unwrap();

        Ok(Self {
            job_input: input,
            wat_path:None,
            binary_path:None
        })
    }
}

#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<WasmJob>>> ,
}

impl SubmittedJobs{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        Self{jobs: tasks}
    }

    pub async fn remove_job(&self, job_id: usize) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|job| job.job_input.id != job_id);
    }

    pub async fn get_num_tasks(&self)->usize{
        let guard = self.jobs.lock();
        guard.await.len()
    }
    
    pub async fn get_jobs(&self)->Vec<WasmJob>{
        self.jobs.lock().await.to_vec()
    }

    pub async fn add_task(&self, task: WasmJob) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

}


use std::fs;
use std::io::{ErrorKind};

pub fn stored_result_decoder(id: usize) -> Option<String> {
    /*
        Function that knows the format of the stored results and decodes them into String
    */
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
