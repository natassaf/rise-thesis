#[derive(Debug, Clone)]
pub struct MemoryFeatures {
    // Binary identification
    pub binary_name: String,
    
    // Size analysis
    pub binary_size_bytes: u64,
    pub data_section_size_bytes: u64,

    // Advanced WAT counts
    pub import_count: u32,
    pub export_count: u32,
    pub function_count: u32,
    pub global_variable_count: u32,
    pub type_definition_count: u32,
    pub instance_count: u32,
    pub resource_count: u32,

    // Function locals complexity
    pub total_local_variables: u32,
    pub max_local_variables_per_function: u32,
    pub avg_local_variables_per_function: f32,
    pub high_complexity_functions: u32,

    // Core memory components
    pub linear_memory_bytes: u64,
    pub stack_pointer_offset: u64,
    pub total_function_references: u32,

    // Classification and request sizes
    pub is_ml_workload: bool,
    pub request_payload_size: u64,
    pub model_file_size: u64,
    pub memory_kb: i64,
    pub payload: i64,
    pub task_duration: f64
}
