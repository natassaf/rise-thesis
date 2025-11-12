use regex::Regex;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use crate::memory_prediction::memory_prediction_utils::MemoryFeatures;


fn process_line(line: &str, memory_features:&mut MemoryFeatures, memory_regex: &Regex, 
                table_regex: &Regex, data_size_regex: &Regex, 
                local_regex: &Regex, locals_data: &mut Vec<u32>) {
    // Linear memory
    if let Some(c) = memory_regex.captures(&line) {
        if let Ok(pages) = c[1].parse::<u32>() {
            memory_features.linear_memory_bytes += pages as u64 * 64 * 1024;
        }
    }
    
    // Function tables
    for cap in table_regex.captures_iter(&line) {
        if let Ok(size) = cap[1].parse::<u32>() {
            memory_features.total_function_references += size;
        }
    }
    
    // Counts (simple string matching - fast)
    memory_features.import_count += line.matches("(import ").count() as u32;
    memory_features.export_count += line.matches("(export ").count() as u32;
    memory_features.function_count += line.matches("(func ").count() as u32;
    memory_features.global_variable_count += line.matches("(global ").count() as u32;
    memory_features.type_definition_count += line.matches("(type ").count() as u32;
    memory_features.instance_count += line.matches("(instance ").count() as u32;
    memory_features.resource_count += line.matches("(resource ").count() as u32;
    
    // Data section size
    for m in data_size_regex.find_iter(&line) {
        memory_features.data_section_size_bytes += (m.end() - m.start()) as u64;
    }
    
    // Local variables - collect data for aggregate calculation
    for caps in local_regex.captures_iter(&line) {
        let text = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let count = text.split_whitespace().filter(|s| !s.is_empty()).count() as u32;
        memory_features.total_local_variables += count;
        locals_data.push(count);
        if count > 10 {
            memory_features.high_complexity_functions += 1;
        }
    }
    
    // ML workload detection
    let lower = line.to_lowercase();
    if ["wasi:nn", "wasi-nn", "tensor", "inference", "graph-execution-context"]
        .iter()
        .any(|k| lower.contains(k)) {
        memory_features.is_ml_workload = true;
    }

}

fn extract_features_from_wat_batch<'a>(wat_file: &str, memory_features:&'a mut MemoryFeatures) -> &'a mut MemoryFeatures {
    // Compile regexes ONCE (outside the loop) - this is the key optimization!
    let memory_regex = Regex::new(r"\(memory\s+\(;\d+;\)\s+(\d+)\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let table_regex = Regex::new(r"\(table\s+\d+\s+(\d+)\s+funcref\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let data_size_regex = Regex::new(r"\(data[\s\S]*?\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let local_regex = Regex::new(r"\(local\s+([^)]*)\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    
    // Open file
    let file = File::open(wat_file).unwrap();
    let reader = BufReader::new(file);
    
    // Track locals for aggregate calculations
    let mut locals_data: Vec<u32> = Vec::new();
    
    // Process each line one by one (reads line-by-line, not all at once)
    for line_result in reader.lines() {
        let line = line_result.unwrap_or_else(|_| String::new());
        process_line(
            &line,
            memory_features,
            &memory_regex,
            &table_regex,
            &data_size_regex,
            &local_regex,
            &mut locals_data,
        );
    }
    
    // Calculate aggregates once at the end
    if !locals_data.is_empty() {
        memory_features.max_local_variables_per_function = *locals_data.iter().max().unwrap_or(&0);
        memory_features.avg_local_variables_per_function = 
            memory_features.total_local_variables as f32 / locals_data.len() as f32;
    }
    
    memory_features
}


pub async fn build_memory_features(
    wasm_file: &str,
    wat_file: &str,
    payload: &str,
    model_folder_name: &str,
) -> MemoryFeatures {
    let mut memory_features = MemoryFeatures::new();

    // Binary size
    memory_features.binary_size_bytes = fs::metadata(wasm_file).map(|m| m.len()).unwrap_or(0);

    // Read WAT
    extract_features_from_wat_batch(&wat_file, &mut memory_features);

    // Request payload and model size
    memory_features.request_payload_size = payload.len() as u64;
    memory_features.model_file_size = compute_model_folder_size(model_folder_name);

    // Parse payload: try to extract n from JSON, fallback to length
    memory_features.payload= if let Ok(parsed_data) = serde_json::from_str::<serde_json::Value>(payload) {
        println!("Parsed data: {:?}", parsed_data);
        if let Some(n_value) = parsed_data.get("n") {
            if let Some(n) = n_value.as_f64() {
                println!("Using n value from JSON: {}", n);
                n as i64
            } else {
                println!("n field exists but not a number, using payload length: {}", payload.len());
                payload.len() as i64
            }
        } else {
            println!("No n field in JSON, using payload length: {}", payload.len());
            payload.len() as i64
        }
    } else {
        println!("Failed to parse JSON, using payload length: {}", payload.len());
        payload.len() as i64
    };

    // Extract binary name from the file path
    let binary_name = wasm_file.split("/").last().unwrap();
    println!("binary name new{:?}", binary_name);
    memory_features.binary_name = binary_name.to_string();
    

    memory_features
}


fn compute_model_folder_size(model_folder_name: &str) -> u64 {
    if model_folder_name.is_empty() { return 0; }
    let base = if cfg!(target_os = "linux") {
        "/home/pi/memory-estimator/models/"
    } else {
        "/Users/athanasiapharmake/workspace/wasm-memory-calculation/memory-estimator/models/"
    };
    let folder = format!("{}{}/", base, model_folder_name);
    if let Ok(entries) = fs::read_dir(&folder) {
        let mut total = 0u64;
        for e in entries.flatten() {
            if let Ok(meta) = e.metadata() { if meta.is_file() { total += meta.len(); } }
        }
        total
    } else { 0 }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn create_default_memory_features() -> MemoryFeatures {
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

    fn get_test_case_1() -> (String, String) {
        (
            "wasm-modules/fibonacci_optimized.wat".to_string(),
            "wasm-modules/fibonacci_optimized.wasm".to_string()
        )
    }

    fn get_test_case_2() -> (String, String) {
        (
            "wasm-modules/matrix_multiplication_component.wat".to_string(),
            "wasm-modules/matrix_multiplication_component.wasm".to_string()
        )
    }

    fn get_test_case_3() -> (String, String) {
        (
            "wasm-modules/matrix_transpose.wat".to_string(),
            "wasm-modules/matrix_transpose.wasm".to_string()        
        )
    }
    #[test]
    fn test_extract_features_from_wat_1() {
        use std::path::Path;
        
        // Use the existing .wat file from wasm-modules/
        // let wat_file = "wasm-modules/image_classification_resnet_onnx_batch.wat";
        // let wasm_file = "wasm-modules/image_classification_resnet_onnx_batch.wasm";
        let (wat_file, _) = get_test_case_1();

        
        // Check if the file exists
        assert!(
            Path::new(&wat_file).exists(),
            "Test file {} does not exist",
            wat_file
        );
        
        // Test extract_features_from_wat (line-by-line processing)
        let mut features_new = create_default_memory_features();
        extract_features_from_wat_batch(&wat_file, &mut features_new);
        
        println!("features_new.linear_memory_bytes: {:?}", features_new.linear_memory_bytes);
        assert_eq!(
            features_new.linear_memory_bytes, 
            1114112,
            "linear_memory_bytes should match"
        );
        
        println!("features_new.total_function_references: {:?}", features_new.total_function_references);
        assert_eq!(
            features_new.total_function_references,
            0,
            "total_function_references should match"
        );
        
        println!("features_new.import_count: {:?}", features_new.import_count);

        assert_eq!(
            features_new.import_count,
            58,
            "import_count should match"
        );
        
        println!("features_new.export_count: {:?}", features_new.export_count);

        assert_eq!(
            features_new.export_count,
            110,
            "export_count should match"
        );
        
        println!("features_new.function_count: {:?}", features_new.function_count);
        assert_eq!(
            features_new.function_count,
            606,
            "function_count should match"
        );
        
        println!("features_new.global_variable_count: {:?}", features_new.global_variable_count);
        assert_eq!(
            features_new.global_variable_count,
            4,
            "global_variable_count should match"
        );
        
        println!("features_new.type_definition_count: {:?}", features_new.type_definition_count);
        assert_eq!(
            features_new.type_definition_count,
            714,
            "type_definition_count should match"
        );
        
        println!("features_new.instance_count: {:?}", features_new.instance_count);
        assert_eq!(
            features_new.instance_count,
            25,
            "instance_count should match"
        );
        
        println!("features_new.resource_count: {:?}", features_new.resource_count);
        assert_eq!(
            features_new.resource_count,
            0,
            "resource_count should match"
        );
        
        println!("features_new.data_section_size_bytes: {:?}", features_new.data_section_size_bytes);
        assert_eq!(
            features_new.data_section_size_bytes,
            38,
            "data_section_size_bytes should match"
        );
        
        println!("features_new.total_local_variables: {:?}", features_new.total_local_variables);
        assert_eq!(
            features_new.total_local_variables,
            1165,
            "total_local_variables should match"
        );
        
        println!("features_new.max_local_variables_per_function: {:?}", features_new.max_local_variables_per_function);
        assert_eq!(
            features_new.max_local_variables_per_function,
            33,
            "max_local_variables_per_function should match"
        );

        println!("features_new.high_complexity_functions: {:?}", features_new.high_complexity_functions);
        assert_eq!(
            features_new.high_complexity_functions,
            22,
            "high_complexity_functions should match"
        );
        
        // For avg_local_variables_per_function, allow small floating point differences
        println!("features_new.avg_local_variables_per_function: {:?}", features_new.avg_local_variables_per_function);
 

        assert_eq!(
            features_new.is_ml_workload,
            false,
            "is_ml_workload should match"
        );
    }

    #[tokio::test]
    async fn test_build_memory_features_old_vs_new_test_case_1() {
        use std::path::Path;
        
        let (wat_file, wasm_file) = get_test_case_2();
        let payload = r#"{"n": 10}"#;
        let model_folder_name = "";

        // Check if files exist
        assert!(
            Path::new(&wat_file).exists(),
            "Test file {} does not exist",
            wat_file
        );
        assert!(
            Path::new(&wasm_file).exists(),
            "Test file {} does not exist",
            wasm_file
        );
        
        println!("\n=== Testing {} ===", wat_file);
        
        // Build features using new method
        let features_new = build_memory_features(
            &wasm_file,
            &wat_file,
            payload,
            model_folder_name
        ).await;
        
        println!("binary name new{:?}", features_new.binary_name);
        
        // Hardcoded old values (from build_memory_features_old output)
        let features_old = MemoryFeatures {
            binary_name: "matrix_multiplication_component.wasm".to_string(),
            binary_size_bytes: 195353,
            linear_memory_bytes: 1114112,
            total_function_references: 0,
            import_count: 53,
            export_count: 102,
            function_count: 585,
            global_variable_count: 4,
            type_definition_count: 694,
            instance_count: 23,
            resource_count: 0,
            data_section_size_bytes: 38,
            total_local_variables: 1171,
            max_local_variables_per_function: 33,
            avg_local_variables_per_function: 4.418868,
            high_complexity_functions: 21,
            is_ml_workload: false,
            request_payload_size: 9,
            model_file_size: 0,
            payload: 10,
        };
        
        // Compare all fields
        println!("Comparing binary_name...");
        assert_eq!(
            features_new.binary_name,
            features_old.binary_name,
            "binary_name should match"
        );
        
        println!("Comparing binary_size_bytes...");
        assert_eq!(
            features_new.binary_size_bytes,
            features_old.binary_size_bytes,
            "binary_size_bytes should match"
        );
        
        println!("Comparing linear_memory_bytes...");
        assert_eq!(
            features_new.linear_memory_bytes,
            features_old.linear_memory_bytes,
            "linear_memory_bytes should match"
        );
        
        println!("Comparing total_function_references...");
        assert_eq!(
            features_new.total_function_references,
            features_old.total_function_references,
            "total_function_references should match"
        );
        
        println!("Comparing import_count...");
        assert_eq!(
            features_new.import_count,
            features_old.import_count,
            "import_count should match"
        );
        
        println!("Comparing export_count...");
        assert_eq!(
            features_new.export_count,
            features_old.export_count,
            "export_count should match"
        );
        
        println!("Comparing function_count...");
        assert_eq!(
            features_new.function_count,
            features_old.function_count,
            "function_count should match"
        );
        
        println!("Comparing global_variable_count...");
        assert_eq!(
            features_new.global_variable_count,
            features_old.global_variable_count,
            "global_variable_count should match"
        );
        
        println!("Comparing type_definition_count...");
        assert_eq!(
            features_new.type_definition_count,
            features_old.type_definition_count,
            "type_definition_count should match"
        );
        
        println!("Comparing instance_count...");
        assert_eq!(
            features_new.instance_count,
            features_old.instance_count,
            "instance_count should match"
        );
        
        println!("Comparing resource_count...");
        assert_eq!(
            features_new.resource_count,
            features_old.resource_count,
            "resource_count should match"
        );
        
        println!("Comparing data_section_size_bytes...");
        assert_eq!(
            features_new.data_section_size_bytes,
            features_old.data_section_size_bytes,
            "data_section_size_bytes should match"
        );
        
        println!("Comparing total_local_variables...");
        assert_eq!(
            features_new.total_local_variables,
            features_old.total_local_variables,
            "total_local_variables should match"
        );
        
        println!("Comparing max_local_variables_per_function...");
        assert_eq!(
            features_new.max_local_variables_per_function,
            features_old.max_local_variables_per_function,
            "max_local_variables_per_function should match"
        );
        
        println!("Comparing high_complexity_functions...");
        assert_eq!(
            features_new.high_complexity_functions,
            features_old.high_complexity_functions,
            "high_complexity_functions should match"
        );
        
        println!("Comparing avg_local_variables_per_function...");
        let avg_diff = (features_new.avg_local_variables_per_function - features_old.avg_local_variables_per_function).abs();
        assert!(
            avg_diff < 0.001,
            "avg_local_variables_per_function should match (diff: {})",
            avg_diff
        );
        
        println!("Comparing is_ml_workload...");
        assert_eq!(
            features_new.is_ml_workload,
            features_old.is_ml_workload,
            "is_ml_workload should match"
        );
        
        println!("Comparing request_payload_size...");
        assert_eq!(
            features_new.request_payload_size,
            features_old.request_payload_size,
            "request_payload_size should match"
        );
        
        println!("Comparing model_file_size...");
        assert_eq!(
            features_new.model_file_size,
            features_old.model_file_size,
            "model_file_size should match"
        );
        
        println!("Comparing payload...");
        assert_eq!(
            features_new.payload,
            features_old.payload,
            "payload should match"
        );
        
        println!("✓ All fields match for test_case_1!");
    }
}