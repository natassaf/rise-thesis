use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use flate2::read::GzDecoder;
use std::io::Read;
use base64::{Engine as _, engine::general_purpose};


/// Decompress a gzip-compressed base64 payload
fn decompress_gzip_payload(compressed_base64: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Decode base64 to get compressed bytes
    let compressed_bytes = general_purpose::STANDARD.decode(compressed_base64)?;
    
    // Decompress using gzip
    let mut decoder = GzDecoder::new(&compressed_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    
    Ok(decompressed)
}

#[derive(Deserialize)]
pub struct TaskQuery {
    pub id: String,
}

// #[derive(Serialize, Deserialize, Debug, Clone, Encode)]
// #[serde(untagged)]
// pub enum Input {
//     U64(u64),
//     F64(f64),
//     ListU64(Vec<u64>),
//     ListF32(Vec<f32>),
// }

// #[derive(Clone, Debug, Deserialize)]
// pub struct JobInput {
//     pub func_name: String,
//     pub input: Vec<u8>,
//     pub id: usize,
//     pub binary_name: String
// }

// fn encode_input(input: &Input) -> Vec<u8> {
//     let config = standard();
//     encode_to_vec(input, config).expect("Failed to encode input")
// }

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WasmJobRequest{
    binary_name: String,
    func_name: String,
    payload: String,
    payload_compressed: bool,
    task_id: String,
    model_folder_name: String,
}

#[derive(Debug, Clone)]
pub struct Job{
    pub binary_path: String,
    pub func_name: String,
    pub payload: String,
    pub payload_compressed: bool, // Track if payload needs decompression
    pub id: String,
    pub folder_to_mount: String,
    pub status: String,
    pub arrival_time: std::time::SystemTime, // Track when job was submitted for sorting
}

impl From<WasmJobRequest> for Job {
    fn from(request: WasmJobRequest) -> Self {
        // Construct the binary_path from binary_name
        let binary_path = format!("wasm-modules/{}", request.binary_name);
        // DON'T decompress here - do it in the worker to avoid blocking the handler
        // This prevents connection timeouts when handling large compressed payloads
        
        Job {
            binary_path,
            func_name: request.func_name,
            payload: request.payload, // Store compressed payload as-is
            payload_compressed: request.payload_compressed, // Remember if it needs decompression
            id: request.task_id,
            folder_to_mount: "models/".to_string() + &request.model_folder_name,
            status:"waiting".to_string(),
            arrival_time: std::time::SystemTime::now(), // Record arrival time for sorting
        }
    }
}


#[derive(Debug, Clone)]
pub struct SubmittedJobs {
    pub jobs: Arc<Mutex<Vec<Job>>>,
}

impl SubmittedJobs{
    pub fn new()->Self{
        let tasks = Arc::new(Mutex::new(vec![]));
        Self{jobs: tasks}
    }

    pub async fn remove_job(&self, job_id: String) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|job| job.id != job_id);
    }

    pub async fn get_num_tasks(&self)->usize{
        let guard = self.jobs.lock().await;
        guard.len()
    }
    
    pub async fn get_jobs(&self)->Vec<Job>{
        self.jobs.lock().await.to_vec()
    }

    // Get and remove the next job from the queue (for workers to pull jobs)
    // Pops from the front (oldest first after sorting)
    pub async fn pop_next_job(&self) -> Option<Job> {
        let mut jobs = self.jobs.lock().await;
        // Pop from front (oldest first after sorting)
        if jobs.is_empty() {
            None
        } else {
            Some(jobs.remove(0))
        }
    }

    pub async fn add_task(&self, task: Job) {
        let mut guard = self.jobs.lock().await;
        guard.push(task);
    }

}


use std::fs::{self, File};
use std::io::{self, BufWriter, ErrorKind};


/// Decompress gzip result data
fn decompress_gzip_result(compressed_data: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    
    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    
    Ok(decompressed)
}

