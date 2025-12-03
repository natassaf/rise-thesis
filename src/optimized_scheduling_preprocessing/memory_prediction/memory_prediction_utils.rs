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
    pub total_function_references: u32,

    // Classification and request sizes
    pub is_ml_workload: bool,
    pub request_payload_size: u64,
    pub model_file_size: u64,
    pub payload: i64,
}

impl MemoryFeatures {
    pub fn new() -> Self {
        MemoryFeatures {
            binary_name: String::new(),
            binary_size_bytes: 0,
            data_section_size_bytes: 0,
            import_count: 0,
            export_count: 0,
            function_count: 0,
            global_variable_count: 0,
            type_definition_count: 0,
            instance_count: 0,
            resource_count: 0,
            total_local_variables: 0,
            max_local_variables_per_function: 0,
            avg_local_variables_per_function: 0.0,
            high_complexity_functions: 0,
            linear_memory_bytes: 0,
            total_function_references: 0,
            is_ml_workload: false,
            request_payload_size: 0,
            model_file_size: 0,
            payload: 0,
        }
    }

    fn hash_binary_name(s: &str) -> f32 {
        let mut hash: i64 = 0;
        for byte in s.bytes() {
            // Use wrapping arithmetic to match Python's behavior with large numbers
            hash = ((hash.wrapping_mul(31)).wrapping_add(byte as i64)) & 0x7FFFFFFFFFFFFFFF;
        }
        hash.abs() as f32
    }

    pub fn to_vec(&self) -> Vec<f32> {
        let binary_name_hash = Self::hash_binary_name(&self.binary_name);
        vec![
            binary_name_hash, // binary_name encoded as hash (feature 0) - matches Python abs(hash(x))
            self.binary_size_bytes as f32,
            self.data_section_size_bytes as f32,
            self.import_count as f32,
            self.export_count as f32,
            self.function_count as f32,
            self.global_variable_count as f32,
            self.type_definition_count as f32,
            self.instance_count as f32,
            self.resource_count as f32,
            self.total_local_variables as f32,
            self.max_local_variables_per_function as f32,
            self.avg_local_variables_per_function,
            self.high_complexity_functions as f32,
            self.linear_memory_bytes as f32,
            self.total_function_references as f32,
            if self.is_ml_workload { 1.0 } else { 0.0 },
            self.request_payload_size as f32,
            self.model_file_size as f32,
            self.payload as f32,
        ]
    }
}
