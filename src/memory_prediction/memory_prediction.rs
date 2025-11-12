use crate::memory_prediction::standard_scaler::StandardScaler;
use ort::{session::Session, inputs, value::Value};
use ndarray::{Array, ArrayD};
use std::sync::OnceLock;
use anyhow::Result;
use tokio::sync::Mutex;
use std::sync::Arc;

// Lazy static to ensure the ONNX model is loaded only once
// Using Arc<tokio::sync::Mutex<>> for async-safe access
static MODEL_SESSION: OnceLock<Arc<Mutex<Session>>> = OnceLock::new();

/// Initialize and load the ONNX model
fn initialize_model() -> Result<()> {
    let model_path = "src/memory_prediction/memory_model/memory_model.onnx";
    let session = Session::builder()?
        .commit_from_file(model_path)?;
    
    MODEL_SESSION.set(Arc::new(Mutex::new(session)))
        .map_err(|_| anyhow::anyhow!("Failed to initialize model session"))?;
    
    Ok(())
}

pub async fn predict_memory(memory_features: &Vec<f32>) -> f64 {
    // println!("[DEBUG] predict_memory called with {} features", memory_features.len());
    
    let memory_features_scaler_path: &str = "src/memory_prediction/memory_model/scaler_x.json";
    let memory_target_scaler_path: &str = "src/memory_prediction/memory_model/scaler_y.json";

    // println!("[DEBUG] Loading scalers...");
    let memory_features_scaler = StandardScaler::new(memory_features_scaler_path);
    let memory_target_scaler = StandardScaler::new(memory_target_scaler_path);

    // println!("[DEBUG] Transforming features...");
    let memory_features_normalized = memory_features_scaler.transform(memory_features);
    // println!("[DEBUG] Normalized features: {:?}", memory_features_normalized);
    
    // Initialize model if not already loaded
    if MODEL_SESSION.get().is_none() {
        println!("[DEBUG] Initializing model...");
        if let Err(e) = initialize_model() {
            eprintln!("[ERROR] Failed to initialize ONNX model: {:?}", e);
            return 0.0;
        }
        // println!("[DEBUG] Model initialized successfully");
    } else {
        println!("[DEBUG] Model already initialized");
    }
    
    // Convert normalized features to ArrayD with shape (1, num_features) for batch inference
    // The features are already normalized, so we just need to reshape them
    let num_features = memory_features_normalized.len();
    // println!("[DEBUG] Creating input array with shape (1, {})", num_features);
    let input_array = match Array::from_shape_vec((1, num_features), memory_features_normalized) {
        Ok(arr) => arr,
        Err(e) => {
            eprintln!("[ERROR] Failed to create input array: {:?}", e);
            return 0.0;
        }
    };
    
    // Convert to ArrayD for ONNX (dynamic dimensions)
    let input_array_dyn: ArrayD<f32> = input_array.into_dyn();
    // println!("[DEBUG] Input array shape: {:?}", input_array_dyn.shape());
    
    // Create a Value from the array
    println!("[DEBUG] Creating ONNX Value from array...");
    let input_value = match Value::from_array(input_array_dyn) {
        Ok(value) => {
            // println!("[DEBUG] Successfully created input value");
            value
        },
        Err(e) => {
            eprintln!("[ERROR] Failed to create input value: {:?}", e);
            return 0.0;
        }
    };
    
    // Run inference using inputs! macro (similar to Python's sess.run(None, {input_name: X_test_normalized_float32}))
    // Do everything in a single lock scope to avoid multiple locks and potential deadlocks
    // println!("[DEBUG] Acquiring session lock...");
    let (output_shape, output_data) = {
        let mut session_guard = match MODEL_SESSION.get() {
            Some(s) => {
                // println!("[DEBUG] Locking session...");
                s.lock().await
            },
            None => {
                eprintln!("[ERROR] Model session not initialized");
                return 0.0;
            }
        };
        
        let session = &mut *session_guard;
        // println!("[DEBUG] Session locked successfully");
        
        // Get input and output names as owned strings to avoid borrow conflicts
        let input_name = session.inputs[0].name.clone();
        let output_name = session.outputs[0].name.clone();
        // println!("[DEBUG] Input name: {}, Output name: {}", input_name, output_name);
        
        // Run inference
        println!("[DEBUG] Running inference...");
        let outputs = match session.run(inputs![input_name.as_str() => input_value]) {
            Ok(outputs) => {
                // println!("[DEBUG] Inference successful");
                outputs
            },
            Err(e) => {
                eprintln!("[ERROR] Failed to run inference: {:?}", e);
                return 0.0;
            }
        };
        
        // Extract the output (y_predicted_normalized) - clone the data to own it
        // The output is typically a 2D array with shape (batch_size, 1)
        // println!("[DEBUG] Extracting output tensor...");
        match outputs[output_name.as_str()].try_extract_tensor::<f32>() {
            Ok((shape, data)) => {
                // println!("[DEBUG] Output shape: {:?}, data len: {}", shape, data.len());
                (shape.to_vec(), data.to_vec())
            },
            Err(e) => {
                eprintln!("[ERROR] Failed to extract output tensor: {:?}", e);
                return 0.0;
            }
        }
    };
    

    // Extract the prediction value (normalized)
    // If output is (1, 1), get [0, 0]; if it's (1,), get [0]
    // println!("[DEBUG] Extracting prediction from output...");
    let prediction_normalized = if output_shape.len() == 2 && output_shape[0] > 0 && output_shape[1] > 0 {
        // println!("[DEBUG] Output is 2D: shape {:?}, taking [0,0]", output_shape);
        output_data[0]
    } else if output_shape.len() == 1 && output_shape[0] > 0 {
        // println!("[DEBUG] Output is 1D: shape {:?}, taking [0]", output_shape);
        output_data[0]
    } else if !output_data.is_empty() {
        // println!("[DEBUG] Output data is not empty, taking first element");
        output_data[0]
    } else {
        eprintln!("[ERROR] Unexpected output shape: {:?}, data len: {}", output_shape, output_data.len());
        return 0.0;
    };
    
    // println!("[DEBUG] Normalized prediction: {}", prediction_normalized);
    
    // Denormalize the prediction using scaler_y.inverse_transform()
    // The scaler expects a slice, so we pass the normalized prediction as a single-element array
    // println!("[DEBUG] Denormalizing prediction...");
    let prediction_normalized_vec = vec![prediction_normalized];
    let prediction_denormalized = memory_target_scaler.inverse_transform(&prediction_normalized_vec);
    
    // println!("[DEBUG] Denormalized prediction: {}", prediction_denormalized[0]);
    
    // Return the denormalized prediction
    prediction_denormalized[0] as f64
}


#[cfg(test)]
mod tests{
    use super::*;

    #[tokio::test]
    async fn test_predict_memory() {
        let memory_features:Vec<f32> = vec![5437617119388410010.0, 689248.0, 38.0, 58.0, 110.0, 606.0, 4.0, 714.0, 25.0, 0.0, 1165.0,
        33.0, 4.4129, 22.0, 1114112.0, 0.0, 0.0, 9.0, 0.0, 26.0, 25376.0];
        let prediction = predict_memory(&memory_features).await;
        let expected_prediction = 25242.838;
        assert!((prediction - expected_prediction).abs() < 0.01, 
            "Prediction {} does not match expected {} (diff: {})", 
            prediction, expected_prediction, (prediction - expected_prediction).abs());
    }
}