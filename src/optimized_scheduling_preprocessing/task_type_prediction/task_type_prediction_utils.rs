#[derive(Debug, Clone)]
pub struct TaskTypeFeatures {

    // Size analysis
    pub binary_size_bytes: u64,

    // Function counts
    pub function_count: u32,
    pub high_complexity_functions: u32,

    // Core memory components
    pub linear_memory_bytes: u64,

    // Instance and resource counts
    pub instance_count: u32,
    pub resource_count: u32,

    // Classification and request sizes
    pub model_file_size: u64,
    pub request_payload_size: u64,

    // Function references
    pub total_function_references: u32,
}

impl TaskTypeFeatures {
    pub fn new() -> Self {
        TaskTypeFeatures {
            binary_size_bytes: 0,
            function_count: 0,
            high_complexity_functions: 0,
            linear_memory_bytes: 0,
            instance_count: 0,
            resource_count: 0,
            model_file_size: 0,
            request_payload_size: 0,
            total_function_references: 0,
        }
    }

    /// Create TaskTypeFeatures from MemoryFeatures
    pub fn from_memory_features(memory_features: &crate::optimized_scheduling_preprocessing::memory_prediction::memory_prediction_utils::MemoryFeatures) -> Self {
        TaskTypeFeatures {
            binary_size_bytes: memory_features.binary_size_bytes,
            function_count: memory_features.function_count,
            high_complexity_functions: memory_features.high_complexity_functions,
            linear_memory_bytes: memory_features.linear_memory_bytes,
            instance_count: memory_features.instance_count,
            resource_count: memory_features.resource_count,
            model_file_size: memory_features.model_file_size,
            request_payload_size: memory_features.request_payload_size,
            total_function_references: memory_features.total_function_references,
        }
    }

    pub fn to_vec(&self) -> Vec<f32> {
        vec![
            self.binary_size_bytes as f32,
            self.function_count as f32,
            self.high_complexity_functions as f32,
            self.linear_memory_bytes as f32,
            self.instance_count as f32,
            self.resource_count as f32,
            self.model_file_size as f32,
            self.request_payload_size as f32,
            self.total_function_references as f32,
        ]
    }
}
