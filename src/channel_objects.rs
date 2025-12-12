use serde::{Deserialize, Serialize};

use crate::api::api_objects::Job;

// #[derive(Debug, Clone, Deserialize, Serialize)]
// struct WorkerJobAskMessage{
//     worker_id: usize,
//     memory_capacity: usize,
//     job_type: JobType,
//     status: String,
// }

// #[derive(Debug, Clone, Deserialize, Serialize)]
// pub struct WorkerJobStatusMessage{
//     worker_id: usize,
//     status: String,
// }

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum JobType {
    IOBound,
    CPUBound,
    Mixed,
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToWorkerMessage {
    pub status: JobAskStatus,
    pub job: Job,
    pub job_type: JobType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToSchedulerMessage {
    pub worker_id: usize,
    pub memory_capacity: usize,
    pub job_type: JobType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Status{
    Success,
    Failed
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStatusMessage{
    pub worker_id: usize,
    pub status: Status,
    pub job_id: String
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum JobAskStatus {
    Found,
    NotFound,
    OutofBoundsJob,
    Terminate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum MessageType {
    ToWorkerMessage,
    ToSchedulerMessage,
    TaskStatusMessage
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub message: String,
    pub message_type: MessageType,
}
