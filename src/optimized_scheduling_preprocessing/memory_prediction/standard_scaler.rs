use std::fs;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct StandardScaler {
    mean: Vec<f32>,
    scale: Vec<f32>,
    var: Vec<f32>,
}

impl StandardScaler {
    pub fn new(scaler_file_path: &str) -> Self {
        let json_str = fs::read_to_string(scaler_file_path).unwrap();
        let scaler: StandardScaler = serde_json::from_str(&json_str).unwrap();
        scaler
    }

    /// Equivalent to sklearn's StandardScaler.transform(X)
    pub fn transform(&self, x: &[f32]) -> Vec<f32> {
        x.iter()
            .zip(&self.mean)
            .zip(&self.scale)
            .map(|((&xi, &m), &s)| (xi - m) / s)
            .collect()
    }

    /// Equivalent to sklearn's StandardScaler.inverse_transform(X_scaled)
    pub fn inverse_transform(&self, x_scaled: &[f32]) -> Vec<f32> {
        x_scaled
            .iter()
            .zip(&self.mean)
            .zip(&self.scale)
            .map(|((&xi_scaled, &m), &s)| xi_scaled * s + m)
            .collect()
    }

    /// Batch inverse transform for single-output scalers (when mean and scale have only 1 element)
    /// This applies the same mean/scale to all values in the batch
    pub fn inverse_transform_batch(&self, x_scaled: &[f32]) -> Vec<f32> {
        if self.mean.is_empty() || self.scale.is_empty() {
            return Vec::new();
        }
        let mean = self.mean[0];
        let scale = self.scale[0];
        x_scaled
            .iter()
            .map(|&xi_scaled| xi_scaled * scale + mean)
            .collect()
    }
}


#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn test_scaler_transform_and_inverse_transform() -> Result<(), Box<dyn std::error::Error>> {
        // Load the scaler parameters from JSON
        let json_str = fs::read_to_string("src/optimized_scheduling_preprocessing/memory_prediction/memory_model/scaler_x.json")?;
        let scaler: StandardScaler = serde_json::from_str(&json_str)?;
    
        println!("Loaded Scaler: {:?}", scaler);
    
        // Example data (one feature)
        let x = vec![5437617119388410010.0, 689248.0, 38.0];
        let x_scaled = scaler.transform(&x);
        assert!(x_scaled[0]-0.77589974<0.00001);
        assert!(x_scaled[1]-0.01514298<0.00001);
        assert!(x_scaled[2]-0.58957656<0.00001);
        let x_reconstructed = scaler.inverse_transform(&x_scaled);
        assert!(x_reconstructed[0]-5437617119388410010.0<0.00001);
        assert!(x_reconstructed[1]-689248.0<0.00001);
        assert!(x_reconstructed[2]-38.0<0.00001);
    
        Ok(())
    }
}
