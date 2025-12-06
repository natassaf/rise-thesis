use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

/// Evaluation metrics storage - separate from scheduler
pub struct EvaluationMetrics {
    execution_start_time: Arc<StdMutex<Option<Instant>>>,
    response_time_per_task: Arc<StdMutex<HashMap<String, Duration>>>,
    task_status: Arc<tokio::sync::Mutex<HashMap<String, u8>>>, // task_id -> 0 (not processed) or 1 (processed)
    completed_count: Arc<tokio::sync::Mutex<usize>>,
}

impl EvaluationMetrics {
    pub fn new() -> Self {
        EvaluationMetrics {
            execution_start_time: Arc::new(StdMutex::new(None)),
            response_time_per_task: Arc::new(StdMutex::new(HashMap::new())),
            task_status: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            completed_count: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    pub async fn initialize(&self, task_ids: Vec<String>){
        let num_tasks = task_ids.len();
        let start_time = std::time::Instant::now();
        self.set_execution_start_time(start_time);
        self.initialize_task_status(task_ids).await;
        let total_tasks = self.get_total_tasks().await;
        println!("Evaluation metrics: Initializing task status with {} task IDs", num_tasks);
        println!("=== Execution start time set at {:?} ===", start_time);
        println!("Start executing {} tasks", total_tasks);
    }

    pub async fn are_all_tasks_completed(&self) -> bool {
        let completed_count = self.get_completed_count().await;
        let total_tasks = self.get_total_tasks().await;
        total_tasks > 0 && completed_count > 0 && completed_count == total_tasks
    }
    pub fn set_execution_start_time(&self, start_time: Instant) {
        *self.execution_start_time.lock().unwrap() = Some(start_time);
    }

    pub fn get_execution_start_time(&self) -> Option<Instant> {
        self.execution_start_time.lock().unwrap().clone()
    }

    pub fn set_response_time(&self, task_id: String, response_time: Duration) {
        let mut response_times = self.response_time_per_task.lock().unwrap();
        response_times.insert(task_id, response_time);
    }

    pub fn get_response_time_per_task(&self) -> Arc<StdMutex<HashMap<String, Duration>>> {
        self.response_time_per_task.clone()
    }

    pub async fn initialize_task_status(&self, task_ids: Vec<String>) {
        if task_ids.is_empty() {
            println!(
                "EvaluationMetrics: Warning - initialize_task_status called with empty task_ids list"
            );
            return;
        }

        let mut task_status = self.task_status.lock().await;
        let mut completed_count = self.completed_count.lock().await;
        
        // Always reset for new batch to avoid stale state
        // Clear old tasks and reset completed count
        let old_count = task_status.len();
        task_status.clear();
        *completed_count = 0;
        
        // Initialize with new tasks
        for task_id in task_ids.into_iter() {
            task_status.insert(task_id, 0); // 0 = not processed
        }
        
        println!(
            "EvaluationMetrics: Initialized task status with {} new tasks (cleared {} old tasks)",
            task_status.len(),
            old_count
        );
    }

pub async fn set_task_status(&self, task_id: String, status: u8) {
    let mut task_status = self.task_status.lock().await;
    let was_already_completed = task_status.get(&task_id).copied() == Some(1);
    task_status.insert(task_id, status);
    
    // Only increment if task wasn't already completed
    if !was_already_completed && status == 1 {
        *self.completed_count.lock().await += 1;
    }
}

    pub async fn get_completed_count(&self) -> usize {
        *self.completed_count.lock().await
    }

    pub async fn get_total_tasks(&self) -> usize {
        let task_status = self.task_status.lock().await;
        task_status.len()
    }

    pub async fn calculate_average_response_time(&self) -> f64 {
        let response_times = self.response_time_per_task.lock().unwrap();
        if !response_times.is_empty() {
            response_times.values().sum::<Duration>().as_millis() as f64
                / response_times.len() as f64
        } else {
            0.0
        }
    }

    pub async fn calculate_throughput(&self, completion_time: Instant, total_tasks: usize) -> f64 {
        if let Some(start_time) = self.get_execution_start_time() {
            let total_time_secs = completion_time.duration_since(start_time);
            total_tasks as f64 / total_time_secs.as_secs_f64()
        } else {
            0.0
        }
    }

    pub async fn reset(&self) {
        *self.execution_start_time.lock().unwrap() = None;
        self.task_status.lock().await.clear();
        *self.completed_count.lock().await = 0;
        self.response_time_per_task.lock().unwrap().clear();
    }
}

// Standalone function to store evaluation metrics to file
pub fn store_evaluation_metrics(
    total_tasks: usize,
    total_time_secs: f64,
    total_time_ms: f64,
    avg_time_ms: f64,
    throughput: f64,
) {
    let metrics_content = format!(
        "Total tasks processed: {}\nTotal execution time: {:.2} seconds ({:.2} ms)\nAverage time per task: {:.2} ms\nThroughput: {:.2} tasks/second\n",
        total_tasks, total_time_secs, total_time_ms, avg_time_ms, throughput
    );

    if let Err(e) = std::fs::write("results/evaluation_metrics.txt", metrics_content) {
        eprintln!("Failed to write evaluation metrics to file: {}", e);
    } else {
        println!("Evaluation metrics written to results/evaluation_metrics.txt");
    }
}
