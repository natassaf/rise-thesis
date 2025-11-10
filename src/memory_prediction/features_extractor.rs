use regex::Regex;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use crate::memory_prediction::memory_prediction_utils::MemoryFeatures;


fn process_line(line: &str, memory_features:&mut MemoryFeatures, memory_regex: &Regex, 
                stack_regex: &Regex, i32_const_regex: &Regex, stack_pointer_found: &mut bool, 
                prev_line_had_stack_pointer:&mut bool, table_regex: &Regex, data_size_regex: &Regex, 
                local_regex: &Regex, stack_pointer_get_regex: &Regex, locals_data: &mut Vec<u32>) {
    // Linear memory
    if let Some(c) = memory_regex.captures(&line) {
        if let Ok(pages) = c[1].parse::<u32>() {
            memory_features.linear_memory_bytes += pages as u64 * 64 * 1024;
        }
    }
    
    // Stack pointer - handle both single-line and cross-line matches
    // Only match the FIRST occurrence to match old version behavior
    // The old regex matches "stack_pointer\\s+i32.const" which in practice matches
    // "global.get $__stack_pointer" on one line followed by "i32.const X" on the next line
    if !*stack_pointer_found {
        // First check if current line matches the pattern (single line)
        if let Some(c) = stack_regex.captures(&line) {
            if let Ok(v) = c[1].parse::<u64>() {
                memory_features.stack_pointer_offset = v;
                *stack_pointer_found = true; // Correctly dereference the boolean
            }
        }
        // Check if previous line had "global.get $__stack_pointer" and current line has i32.const (cross-line match)
        // This matches the actual pattern: "global.get $__stack_pointer" followed by "i32.const 6160"
        else if *prev_line_had_stack_pointer {
            if let Some(c) = i32_const_regex.captures(&line) {
                if let Ok(v) = c[1].parse::<u64>() {
                    memory_features.stack_pointer_offset = v;
                    *stack_pointer_found = true; // Stop looking after first match
                }
            }
        }
    }
    
    // Check if current line has "global.get $__stack_pointer" for next iteration (only if we haven't found it yet)
    // This is more specific than just "stack_pointer" to avoid false matches
    if !*stack_pointer_found {
        *prev_line_had_stack_pointer = stack_pointer_get_regex.is_match(&line);
    }
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
    // Stack pointer regex: matches stack_pointer followed by whitespace and i32.const
    // This can match across lines when reading full file, so we need to handle line-by-line differently
    let stack_regex = Regex::new(r"stack_pointer\s+i32\.const\s+(\d+)").unwrap_or_else(|_| Regex::new("$").unwrap());
    // Check for stack_pointer in context of "global.get" (the actual usage, not the definition)
    // The old version matches: "global.get $__stack_pointer" followed by "i32.const 6160" on next line
    let stack_pointer_get_regex = Regex::new(r"global\.get\s+\$__stack_pointer").unwrap_or_else(|_| Regex::new("$").unwrap());
    let i32_const_regex = Regex::new(r"i32\.const\s+(\d+)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let table_regex = Regex::new(r"\(table\s+\d+\s+(\d+)\s+funcref\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let data_size_regex = Regex::new(r"\(data[\s\S]*?\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    let local_regex = Regex::new(r"\(local\s+([^)]*)\)").unwrap_or_else(|_| Regex::new("$").unwrap());
    
    // Open file
    let file = File::open(wat_file).unwrap();
    let reader = BufReader::new(file);
    
    // Track locals for aggregate calculations
    let mut locals_data: Vec<u32> = Vec::new();
    
    // Track if previous line had stack_pointer (for cross-line matching)
    // Only match the FIRST occurrence to match old version behavior
    let mut prev_line_had_stack_pointer = false;
    let mut stack_pointer_found = false; // Track if we've already found the stack pointer offset
    
    // Process each line one by one (reads line-by-line, not all at once)
    for line_result in reader.lines() {
        let line = line_result.unwrap_or_else(|_| String::new());
        process_line(
            &line,
            memory_features,
            &memory_regex,
            &stack_regex,
            &i32_const_regex,
            &mut stack_pointer_found,
            &mut prev_line_had_stack_pointer,
            &table_regex,
            &data_size_regex,
            &local_regex,
            &stack_pointer_get_regex,
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
            stack_pointer_offset: 0,
            total_function_references: 0,
            is_ml_workload: false,
            request_payload_size: 0,
            model_file_size: 0,
            memory_kb: -1,
            payload: 0,
            task_duration: 0.0,
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
        
        println!("features_new.stack_pointer_offset: {:?}", features_new.stack_pointer_offset);

        assert_eq!(
            features_new.stack_pointer_offset,
            128,
            "stack_pointer_offset should match"
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
     }