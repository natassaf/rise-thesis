
use base64::{Engine as _, engine::general_purpose};
use flate2::read::GzDecoder;

use std::io::Read;
/// Decompress a gzip-compressed base64 payload
fn decompress_gzip_payload(compressed_base64: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Decode base64 to get compressed bytes
    let compressed_bytes = general_purpose::STANDARD.decode(compressed_base64)?;
    
    // Decompress using gzip
    let mut decoder = GzDecoder::new(&compressed_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    
    Ok(decompressed)
}