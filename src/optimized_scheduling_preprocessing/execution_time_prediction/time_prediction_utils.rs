#[derive(Debug, Clone)]
pub struct ExecutionTimeFeatures {
    pub binary_name: String,
    pub binary_size_bytes: u64,
    pub data_section_size_bytes: u64,
    pub function_count: u32,
    pub high_complexity_functions: u32,
    pub linear_memory_bytes: u64,
    pub instance_count: u32,
    pub resource_count: u32,
    pub is_ml_workload: bool,
    pub model_file_size: u64,
    pub request_payload_size: u64,
}

impl ExecutionTimeFeatures {
    pub fn new() -> Self {
        ExecutionTimeFeatures {
            binary_name: String::new(),
            binary_size_bytes: 0,
            data_section_size_bytes: 0,
            function_count: 0,
            high_complexity_functions: 0,
            linear_memory_bytes: 0,
            instance_count: 0,
            resource_count: 0,
            is_ml_workload: false,
            model_file_size: 0,
            request_payload_size: 0,
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
            binary_name_hash as f32,
            self.binary_size_bytes as f32,
            self.data_section_size_bytes as f32,
            self.function_count as f32,
            self.high_complexity_functions as f32,
            self.linear_memory_bytes as f32,
            self.instance_count as f32,
            self.resource_count as f32,
            if self.is_ml_workload { 1.0 } else { 0.0 },
            self.model_file_size as f32,
            self.request_payload_size as f32,
        ]
    }
}