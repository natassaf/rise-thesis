use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::memory_monitoring::{get_peak_memory_kb, get_peak_memory_from_cgroup_kb};

/// Evaluation metrics storage - separate from scheduler
pub struct EvaluationMetrics {
    execution_start_time: Arc<StdMutex<Option<Instant>>>,
    response_time_per_task: Arc<StdMutex<HashMap<String, Duration>>>,
    task_status: Arc<Mutex<HashMap<String, u8>>>, // task_id -> 0 (not processed) or 1 (processed)
    completed_count: Arc<Mutex<usize>>,
    sequential_successful_count: Arc<Mutex<usize>>, // Count of successful runs when sequential_run_flag is true
}

impl EvaluationMetrics {
    pub fn new() -> Self {
        EvaluationMetrics {
            execution_start_time: Arc::new(StdMutex::new(None)),
            response_time_per_task: Arc::new(StdMutex::new(HashMap::new())),
            task_status: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            completed_count: Arc::new(tokio::sync::Mutex::new(0)),
            sequential_successful_count: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    pub async fn all_tasks_completed_callback(&self, jobs_failed: usize, jobs_succeeded: usize ){
        let completed_count = self.get_completed_count().await;
        let total_tasks = self.get_total_tasks().await;
        let completion_time = std::time::Instant::now();
        let peak_memory = self.get_peak_memory().await;
        let throughput = self
            .calculate_throughput(completion_time, total_tasks)
            .await;
        let average_response_time_millis =
            self.calculate_average_response_time().await;
        let sequential_successful = self.get_sequential_successful_count().await;
        let total_time_duration =
            if let Some(start) = self.get_execution_start_time() {
                completion_time.duration_since(start)
            } else {
                std::time::Duration::from_secs(0)
            };

        println!("=== Execution completed ===");
        println!("Total tasks processed: {}", completed_count);
        println!(
            "Total execution time: {:.2} seconds ({:.2} ms)",
            total_time_duration.as_secs(),
            total_time_duration.as_millis()
        );
        println!(
            "Average response time per task: {:.2} ms",
            average_response_time_millis
        );
        println!("Throughput: {:.2} tasks/second", throughput);
        println!("Successful runs in sequential mode: {}", sequential_successful);
        println!("Number of jobs failed: {}", jobs_failed);
        println!("Number of jobs succeeded: {}", jobs_succeeded);

        // Store metrics to file
        crate::evaluation_metrics::store_evaluation_metrics(
            completed_count,
            total_time_duration.as_secs_f64(),
            total_time_duration.as_millis() as f64,
            average_response_time_millis,
            throughput,
            peak_memory,
            sequential_successful,
            jobs_failed,
            jobs_succeeded
        );
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
    let previous_status = task_status.get(&task_id).copied().unwrap_or(0);
    task_status.insert(task_id, status);
    
    // Increment completed count if task moves from unprocessed (0) to any processed status (1=success, 2=failed)
    // This ensures both successful and failed jobs are counted as completed
    if previous_status == 0 && status != 0 {
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

    pub async fn get_peak_memory(&self)->u64{
       // Prefer cgroup peak memory (more accurate when running with cgroup limits like systemd-run)
       // Falls back to process peak RSS (VmHWM) if cgroup is not available
       get_peak_memory_from_cgroup_kb()
           .or_else(|| get_peak_memory_kb())
           .unwrap_or(0)
    }

    pub async fn increment_sequential_successful(&self) {
        let mut count = self.sequential_successful_count.lock().await;
        *count += 1;
    }

    pub async fn get_sequential_successful_count(&self) -> usize {
        *self.sequential_successful_count.lock().await
    }

    pub async fn reset(&self) {
        *self.execution_start_time.lock().unwrap() = None;
        self.task_status.lock().await.clear();
        *self.completed_count.lock().await = 0;
        *self.sequential_successful_count.lock().await = 0;
        self.response_time_per_task.lock().unwrap().clear();
        
    }
}

pub fn store_evaluation_metrics(
    total_tasks: usize,
    total_time_secs: f64,
    total_time_ms: f64,
    avg_time_ms: f64,
    throughput: f64,
    max_memory_kb: u64,
    sequential_successful: usize,
    jobs_failed: usize,
    jobs_succeeded: usize
) {
    let metrics_content = format!(
        "Max memory: {} \nTotal tasks processed: {}\nTotal execution time: {:.2} seconds ({:.2} ms)\nAverage time per task: {:.2} ms\nThroughput: {:.2} tasks/second\nSuccessful runs in sequential mode: {}\nNumber of jobs failed: {}\nNumber of jobs succeeded: {}\n",
        max_memory_kb, total_tasks, total_time_secs, total_time_ms, avg_time_ms, throughput, sequential_successful, jobs_failed, jobs_succeeded
    );

    if let Err(e) = std::fs::write("results/evaluation_metrics.txt", metrics_content) {
        eprintln!("Failed to write evaluation metrics to file: {}", e);
    } else {
        println!("Evaluation metrics written to results/evaluation_metrics.txt");
    }
}
