use bincode::config::standard;
use bincode::{encode_to_vec, Encode};
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64};
use flate2::read::GzDecoder;
use std::io::Read;
use base64::{Engine as _, engine::general_purpose};

static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    pub id: usize,
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
    task_id: usize,
    model_folder_name: String,
}

#[derive(Debug, Clone)]
pub struct Job{
    pub binary_path: String,
    pub func_name: String,
    pub payload: String,
    pub id: usize,
    pub folder_to_mount: String,
}

impl From<WasmJobRequest> for Job {
    fn from(request: WasmJobRequest) -> Self {
        // Construct the binary_path from binary_name
        let binary_path = format!("wasm-modules/{}", request.binary_name);
        
        // Decompress payload if it's compressed
        let payload = if request.payload_compressed {
            match decompress_gzip_payload(&request.payload) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    eprintln!("Failed to decompress payload: {}", e);
                    request.payload // Fallback to original payload
                }
            }
        } else {
            request.payload
        };
        
        Job {
            binary_path,
            func_name: request.func_name,
            payload,
            id: request.task_id,
            folder_to_mount: "models/".to_string() + &request.model_folder_name,
        }
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


use std::fs::{self, File};
use std::io::{self, BufWriter, ErrorKind};

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

pub fn store_encoded_result<T: Serialize>(
        numbers: &[T],
        file_path: &str,
    ) -> std::io::Result<()> {
        let file = File::create(file_path)?;
        let writer = BufWriter::new(file);

        serde_json::to_writer(writer, numbers) // Serialize directly to the writer
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON serialization failed: {}", e)))?;
        Ok(())
    }