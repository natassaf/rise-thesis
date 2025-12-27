use crate::optimized_scheduling_preprocessing::memory_prediction::standard_scaler::StandardScaler;
use crate::optimized_scheduling_preprocessing::features_extractor::TaskBoundType;
use anyhow::Result;
use ndarray::{Array, ArrayD};
use ort::{inputs, session::Session, value::Value};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;

// Lazy static to ensure the ONNX model is loaded only once
// Using Arc<tokio::sync::Mutex<>> for async-safe access
static MODEL_SESSION: OnceLock<Arc<Mutex<Session>>> = OnceLock::new();

/// Initialize and load the ONNX model
fn initialize_model() -> Result<()> {
    let model_path =
        "src/optimized_scheduling_preprocessing/task_type_prediction/task_type_model.onnx";
    
    // Check if file exists first
    if !std::path::Path::new(model_path).exists() {
        return Err(anyhow::anyhow!("Model file not found: {}", model_path));
    }
    
    let session = Session::builder()
        .map_err(|e| anyhow::anyhow!("Failed to create session builder: {:?}", e))?
        .commit_from_file(model_path)
        .map_err(|e| anyhow::anyhow!("Failed to load model from {}: {:?}", model_path, e))?;

    MODEL_SESSION
        .set(Arc::new(Mutex::new(session)))
        .map_err(|_| anyhow::anyhow!("Failed to initialize model session (already initialized)"))?;

    Ok(())
}

/// Predict task bound type from task type features (single prediction)
/// Returns TaskBoundType::CpuBound (class 0) or TaskBoundType::IoBound (class 1)
pub async fn predict_task_type(task_type_features: &Vec<f32>) -> TaskBoundType {
    let features_scaler_path: &str =
        "src/optimized_scheduling_preprocessing/task_type_prediction/scaler_x.json";

    let features_scaler = StandardScaler::new(features_scaler_path);

    // Normalize features
    let normalized_features = features_scaler.transform(task_type_features);

    // Initialize model if not already loaded
    if MODEL_SESSION.get().is_none() {
        // Try to initialize - if it fails because another thread already initialized it, that's fine
        if let Err(e) = initialize_model() {
            // If initialization failed but model is now available, that's okay (race condition)
            if MODEL_SESSION.get().is_none() {
                eprintln!("[ERROR] Failed to initialize ONNX model: {:?}", e);
                return TaskBoundType::Mixed;
            }
            // Otherwise, another thread initialized it, so we're good
        }
    }

    // Convert normalized features to ArrayD with shape (1, num_features)
    let num_features = normalized_features.len();
    let input_array = match Array::from_shape_vec((1, num_features), normalized_features) {
        Ok(arr) => arr,
        Err(e) => {
            eprintln!("[ERROR] Failed to create input array: {:?}", e);
            return TaskBoundType::Mixed;
        }
    };

    // Convert to ArrayD for ONNX (dynamic dimensions)
    let input_array_dyn: ArrayD<f32> = input_array.into_dyn();

    // Create a Value from the array
    let input_value = match Value::from_array(input_array_dyn) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("[ERROR] Failed to create input value: {:?}", e);
            return TaskBoundType::Mixed;
        }
    };

    // Run inference
    let (output_shape, output_data) = {
        let mut session_guard = match MODEL_SESSION.get() {
            Some(s) => s.lock().await,
            None => {
                eprintln!("[ERROR] Model session not initialized");
                return TaskBoundType::Mixed;
            }
        };

        let session = &mut *session_guard;

        // Get input and output names as owned strings to avoid borrow conflicts
        let input_name = session.inputs[0].name.clone();
        let output_name = session.outputs[0].name.clone();

        // Run inference
        let outputs = match session.run(inputs![input_name.as_str() => input_value]) {
            Ok(outputs) => outputs,
            Err(e) => {
                eprintln!("[ERROR] Failed to run inference: {:?}", e);
                return TaskBoundType::Mixed;
            }
        };

        // Extract the output - try i64 first (class predictions), then f32 (probabilities)
        match outputs[output_name.as_str()].try_extract_tensor::<i64>() {
            Ok((_shape, data)) => {
                // Direct class predictions (i64)
                if data.is_empty() {
                    eprintln!("[ERROR] Empty output data");
                    return TaskBoundType::Mixed;
                }
                let predicted_class = data[0] as i32;
                return match predicted_class {
                    0 => TaskBoundType::CpuBound,
                    1 => TaskBoundType::IoBound,
                    _ => {
                        eprintln!("[ERROR] Unexpected class prediction: {}", predicted_class);
                        TaskBoundType::Mixed
                    }
                };
            }
            Err(_) => {
                // Try f32 (probabilities)
                match outputs[output_name.as_str()].try_extract_tensor::<f32>() {
                    Ok((shape, data)) => (shape.to_vec(), data.to_vec()),
                    Err(e) => {
                        eprintln!("[ERROR] Failed to extract output tensor: {:?}", e);
                        return TaskBoundType::Mixed;
                    }
                }
            }
        }
    };

    // If we get here, we have probabilities (f32)
    // Logistic regression outputs probabilities: [prob_class_0, prob_class_1]
    // Class 0 = 'cpu' -> TaskBoundType::CpuBound
    // Class 1 = 'io' -> TaskBoundType::IoBound
    // Take argmax to get predicted class
    let predicted_class = if output_shape.len() == 2 {
        // Output shape is (1, 2) - probabilities for both classes
        if output_data.len() >= 2 {
            if output_data[0] >= output_data[1] {
                0 // Class 0 (cpu)
            } else {
                1 // Class 1 (io)
            }
        } else {
            eprintln!("[ERROR] Unexpected output data length: {}", output_data.len());
            return TaskBoundType::Mixed;
        }
    } else if output_shape.len() == 1 && output_shape[0] == 2 {
        // Output shape is (2,) - probabilities for both classes
        if output_data.len() >= 2 {
            if output_data[0] >= output_data[1] {
                0 // Class 0 (cpu)
            } else {
                1 // Class 1 (io)
            }
        } else {
            eprintln!("[ERROR] Unexpected output data length: {}", output_data.len());
            return TaskBoundType::Mixed;
        }
    } else if output_shape.len() == 1 && output_shape[0] == 1 {
        // Output shape is (1,) - single class prediction (shouldn't happen but handle it)
        if !output_data.is_empty() {
            // If it's a single value, assume it's a probability threshold
            if output_data[0] >= 0.5 {
                1 // Class 1 (io)
            } else {
                0 // Class 0 (cpu)
            }
        } else {
            eprintln!("[ERROR] Empty output data");
            return TaskBoundType::Mixed;
        }
    } else {
        eprintln!(
            "[ERROR] Unexpected output shape: {:?}, data len: {}",
            output_shape,
            output_data.len()
        );
        return TaskBoundType::Mixed;
    };

    // Map predicted class to TaskBoundType
    match predicted_class {
        0 => TaskBoundType::CpuBound,
        1 => TaskBoundType::IoBound,
        _ => TaskBoundType::Mixed,
    }
}

/// Predict task bound type from task type features (batch prediction)
/// Returns Vec<TaskBoundType> where each element corresponds to the input feature vector
pub async fn predict_task_type_batch(task_type_features_batch: &[Vec<f32>]) -> Vec<TaskBoundType> {
    if task_type_features_batch.is_empty() {
        return Vec::new();
    }

    let features_scaler_path: &str =
        "src/optimized_scheduling_preprocessing/task_type_prediction/scaler_x.json";

    let features_scaler = StandardScaler::new(features_scaler_path);

    // Normalize all features in the batch
    let mut normalized_batch: Vec<Vec<f32>> = Vec::new();
    for features in task_type_features_batch {
        let normalized = features_scaler.transform(features);
        normalized_batch.push(normalized);
    }

    // Initialize model if not already loaded
    if MODEL_SESSION.get().is_none() {
        // Try to initialize - if it fails because another thread already initialized it, that's fine
        if let Err(e) = initialize_model() {
            // If initialization failed but model is now available, that's okay (race condition)
            if MODEL_SESSION.get().is_none() {
                eprintln!("[ERROR] Failed to initialize ONNX model: {:?}", e);
                return vec![TaskBoundType::Mixed; task_type_features_batch.len()];
            }
            // Otherwise, another thread initialized it, so we're good
        }
    }

    let batch_size = task_type_features_batch.len();
    let num_features = task_type_features_batch[0].len();

    // Create input array with shape (batch_size, num_features)
    let mut all_features: Vec<f32> = Vec::with_capacity(batch_size * num_features);
    for normalized_features in &normalized_batch {
        all_features.extend_from_slice(normalized_features);
    }

    let input_array = match Array::from_shape_vec((batch_size, num_features), all_features) {
        Ok(arr) => arr,
        Err(e) => {
            eprintln!("[ERROR] Failed to create input array: {:?}", e);
            return vec![TaskBoundType::Mixed; batch_size];
        }
    };

    let input_array_dyn: ArrayD<f32> = input_array.into_dyn();
    let input_value = match Value::from_array(input_array_dyn) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("[ERROR] Failed to create input value: {:?}", e);
            return vec![TaskBoundType::Mixed; batch_size];
        }
    };

    // Run batch inference
    let (output_shape, output_data) = {
        let mut session_guard = match MODEL_SESSION.get() {
            Some(s) => s.lock().await,
            None => {
                eprintln!("[ERROR] Model session not initialized");
                return vec![TaskBoundType::Mixed; batch_size];
            }
        };

        let session = &mut *session_guard;
        let input_name = session.inputs[0].name.clone();
        let output_name = session.outputs[0].name.clone();

        let outputs = match session.run(inputs![input_name.as_str() => input_value]) {
            Ok(outputs) => outputs,
            Err(e) => {
                eprintln!("[ERROR] Failed to run inference: {:?}", e);
                return vec![TaskBoundType::Mixed; batch_size];
            }
        };

        // Try i64 first (class predictions), then f32 (probabilities)
        match outputs[output_name.as_str()].try_extract_tensor::<i64>() {
            Ok((shape, data)) => {
                // Direct class predictions (i64)
                let mut predictions = Vec::new();
                // Handle different output shapes: (batch_size,) or (batch_size, 1)
                let num_predictions = if shape.len() == 2 && shape[1] == 1 {
                    // Shape is (batch_size, 1) - take first element of each row
                    shape[0] as usize
                } else if shape.len() == 1 {
                    // Shape is (batch_size,)
                    shape[0] as usize
                } else {
                    // Unexpected shape, use data length
                    data.len()
                };
                
                for i in 0..num_predictions.min(data.len()) {
                    let class = data[i];
                    predictions.push(match class {
                        0 => TaskBoundType::CpuBound,
                        1 => TaskBoundType::IoBound,
                        _ => {
                            eprintln!("[ERROR] Unexpected class prediction: {}", class);
                            TaskBoundType::Mixed
                        }
                    });
                }
                // Ensure we have the right number of predictions
                while predictions.len() < batch_size {
                    predictions.push(TaskBoundType::Mixed);
                }
                // Truncate if we have too many
                predictions.truncate(batch_size);
                return predictions;
            }
            Err(_) => {
                // Try f32 (probabilities)
                match outputs[output_name.as_str()].try_extract_tensor::<f32>() {
                    Ok((shape, data)) => (shape.to_vec(), data.to_vec()),
                    Err(e) => {
                        eprintln!("[ERROR] Failed to extract output tensor: {:?}", e);
                        return vec![TaskBoundType::Mixed; batch_size];
                    }
                }
            }
        }
    };

    // If we get here, we have probabilities (f32)
    // Extract predictions from batch output
    // Logistic regression outputs probabilities: shape (batch_size, 2) or (batch_size * 2,)
    let mut predictions = Vec::new();
    
    if output_shape.len() == 2 {
        // Output shape is (batch_size, num_classes) where num_classes = 2
        let num_classes = output_shape[1] as usize;
        let actual_batch_size = output_shape[0] as usize;
        
        if num_classes != 2 {
            eprintln!(
                "[ERROR] Expected 2 classes, got {} classes",
                num_classes
            );
            return vec![TaskBoundType::Mixed; batch_size];
        }

        for i in 0..actual_batch_size {
            let idx = i * num_classes;
            if idx + 1 < output_data.len() {
                let prob_class_0 = output_data[idx];
                let prob_class_1 = output_data[idx + 1];
                // Take argmax
                let predicted_class = if prob_class_0 >= prob_class_1 { 0 } else { 1 };
                predictions.push(match predicted_class {
                    0 => TaskBoundType::CpuBound,
                    1 => TaskBoundType::IoBound,
                    _ => TaskBoundType::Mixed,
                });
            } else {
                eprintln!(
                    "[ERROR] Index {} out of bounds for output_data.len()={}",
                    idx,
                    output_data.len()
                );
                predictions.push(TaskBoundType::Mixed);
            }
        }
    } else if output_shape.len() == 1 {
        // Could be (batch_size,) for single predictions or (batch_size * 2,) for flattened probabilities
        if output_data.len() == batch_size {
            // Single predictions per sample
            for &prob in &output_data {
                let predicted_class = if prob >= 0.5 { 1 } else { 0 };
                predictions.push(match predicted_class {
                    0 => TaskBoundType::CpuBound,
                    1 => TaskBoundType::IoBound,
                    _ => TaskBoundType::Mixed,
                });
            }
        } else {
            // Flattened probabilities (batch_size * 2,)
            let expected_len = batch_size * 2;
            if output_data.len() >= expected_len {
                for i in 0..batch_size {
                    let idx = i * 2;
                    let prob_class_0 = output_data[idx];
                    let prob_class_1 = output_data[idx + 1];
                    // Take argmax
                    let predicted_class = if prob_class_0 >= prob_class_1 { 0 } else { 1 };
                    predictions.push(match predicted_class {
                        0 => TaskBoundType::CpuBound,
                        1 => TaskBoundType::IoBound,
                        _ => TaskBoundType::Mixed,
                    });
                }
            } else {
                eprintln!(
                    "[ERROR] Output data length {} is less than expected {}",
                    output_data.len(),
                    expected_len
                );
                return vec![TaskBoundType::Mixed; batch_size];
            }
        }
    } else {
        eprintln!(
            "[ERROR] Unexpected output shape: {:?} for batch size {}",
            output_shape, batch_size
        );
        return vec![TaskBoundType::Mixed; batch_size];
    }

    predictions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_predict_task_type() {
        // Example test with sample features
        // Features: [binary_name_hash, binary_size_bytes, function_count, high_complexity_functions,
        //           linear_memory_bytes, instance_count, resource_count, is_ml_workload,
        //           model_file_size, request_payload_size, total_function_references]
        let task_type_features: Vec<f32> = vec![
            1234567890.0, // binary_name_hash
            195353.0,     // binary_size_bytes
            585.0,        // function_count
            21.0,         // high_complexity_functions
            1114112.0,    // linear_memory_bytes
            23.0,         // instance_count
            0.0,          // resource_count
            0.0,          // is_ml_workload (false)
            0.0,          // model_file_size
            9.0,          // request_payload_size
            0.0,          // total_function_references
        ];
        
        let prediction = predict_task_type(&task_type_features).await;
        // Should predict either CpuBound or IoBound, not Mixed
        assert!(
            matches!(prediction, TaskBoundType::CpuBound | TaskBoundType::IoBound),
            "Prediction should be CpuBound or IoBound, got {:?}",
            prediction
        );
    }

    #[tokio::test]
    async fn test_predict_task_type_batch() {
        let task_type_features_batch: Vec<Vec<f32>> = vec![
            vec![
                1234567890.0, 195353.0, 585.0, 21.0, 1114112.0, 23.0, 0.0, 0.0, 0.0, 9.0, 0.0,
            ],
            vec![
                9876543210.0, 500000.0, 100.0, 5.0, 2000000.0, 10.0, 0.0, 1.0, 1000000.0, 100.0, 0.0,
            ],
        ];
        
        let predictions = predict_task_type_batch(&task_type_features_batch).await;
        assert_eq!(predictions.len(), 2);
        for prediction in &predictions {
            assert!(
                matches!(prediction, &TaskBoundType::CpuBound | &TaskBoundType::IoBound),
                "Prediction should be CpuBound or IoBound, got {:?}",
                prediction
            );
        }
    }
}

