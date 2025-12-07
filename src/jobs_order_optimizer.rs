use crate::api::api_objects::SubmittedJobs;
use crate::optimized_scheduling_preprocessing::scheduler_algorithms::{
    BaselineStaticSchedulerAlgorithm, Improvement1, MemoryTimeAwareSchedulerAlgorithm, SchedulerAlgorithm
};
use actix_web::web;

/// JobsOrderOptimizer handles task prediction and sorting independently of the scheduler
/// This allows predict_and_sort to run without needing to acquire the scheduler mutex
pub struct JobsOrderOptimizer {
    submitted_jobs: web::Data<SubmittedJobs>,
}

impl JobsOrderOptimizer {
    pub fn new(submitted_jobs: web::Data<SubmittedJobs>) -> Self {
        JobsOrderOptimizer { submitted_jobs }
    }

    pub async fn predict_and_sort(&self, scheduling_algorithm: String) {
        // Create the appropriate scheduler algorithm based on the request
        let scheduler_algo: Box<dyn SchedulerAlgorithm> = match scheduling_algorithm.as_str() {
            "improvement1"=>Box::new(Improvement1::new()),
            // "improvement2"=>Box::new(Improvement2::new()),
            // "improvement3"=>Box::new(Improvement3::new()),
            "memory_time_aware" => Box::new(MemoryTimeAwareSchedulerAlgorithm::new()),
            "baseline" => Box::new(BaselineStaticSchedulerAlgorithm::new()),
            _ => panic!(
                "Invalid scheduling algorithm: {}. Must be 'memory_time_aware' or 'baseline'",
                scheduling_algorithm
            ),
        };

        println!("=== Starting predictions and sorting with {} algorithm ===", scheduling_algorithm);

        // Schedule tasks using the selected algorithm (does predictions and sorting)
        let (io_bound_task_ids, cpu_bound_task_ids) =
            scheduler_algo.prioritize_tasks(&self.submitted_jobs).await;

        // Store the task ID sets in SubmittedJobs
        self.submitted_jobs
            .set_cpu_bound_task_ids(cpu_bound_task_ids)
            .await;
        self.submitted_jobs
            .set_io_bound_task_ids(io_bound_task_ids)
            .await;

        println!("=== Predictions and sorting completed ===");
    }
}

