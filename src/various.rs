use bincode::config::standard;
use bincode::{encode_to_vec, Encode};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
pub struct TaskQuery {
    pub id: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Encode)]
#[serde(untagged)]
pub enum Input {
    U64(u64),
    F64(f64),
    ListU64(Vec<u64>),
    ListF32(Vec<f32>),
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobInput {
    pub func_name: String,
    pub input: Vec<u8>,
    pub id: usize,
    pub binary_name: String
}

fn encode_input(input: &Input) -> Vec<u8> {
    let config = standard();
    encode_to_vec(input, config).expect("Failed to encode input")
}

// // Custom Deserialize for JobInput to encode `input` dynamically
// impl<'de> Deserialize<'de> for JobInput {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         // First, deserialize to an intermediate struct with Input
//         #[derive(Deserialize)]
//         struct RawJobInput {
//             func_name: String,
//             input: Input,
//             id: usize,
//             binary_name: String,
//         }

//         let raw = RawJobInput::deserialize(deserializer)?;
//         let encoded = encode_input(&raw.input);

//         Ok(JobInput {
//             func_name: raw.func_name,
//             input: encoded,
//             id: raw.id,
//             binary_name: raw.binary_name,
//         })
//     }
// }

#[derive(Debug, Clone)]
pub struct WasmJob{
    pub job_input: JobInput,
    pub binary_path:String,
}

 impl WasmJob{

    pub fn new(binary_path: String, job_input: JobInput) -> Self {
        WasmJob {
            binary_path,
            job_input
        }
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
        let input: JobInput = JobInput::deserialize(deserializer)?;
        let binary_path = "wasm-modules/".to_owned()+ input.binary_name.as_str()+ ".wasm";
        Ok(Self {
            job_input: input.clone(),
            binary_path:binary_path
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
