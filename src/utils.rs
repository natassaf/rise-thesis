// #[derive(Serialize, Deserialize, Debug, Clone, Encode)]
// #[serde(untagged)]
// pub enum Input {
//     U64(u64),
//     F64(f64),
//     ListU64(Vec<u64>),
//     ListF32(Vec<f32>),
// }

// #[derive(Clone, Debug, Deserialize)]
// pub struct JobInput {
//     pub func_name: String,
//     pub input: Vec<u8>,
//     pub id: usize,
//     pub binary_name: String
// }

// fn encode_input(input: &Input) -> Vec<u8> {
//     let config = standard();
//     encode_to_vec(input, config).expect("Failed to encode input")
// }

// // Custom Deserialize for JobInput to encode `input` dynamically
// impl<'de> Deserialize<'de> for JobInput {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         // First, deserialize to an intermediate struct with Input
//         #[derive(Deserialize)]
//         struct RawJobInput {
//             func_name: String,
//             input: Input,
//             id: usize,
//             binary_name: String,
//         }

//         let raw = RawJobInput::deserialize(deserializer)?;
//         let encoded = encode_input(&raw.input);

//         Ok(JobInput {
//             func_name: raw.func_name,
//             input: encoded,
//             id: raw.id,
//             binary_name: raw.binary_name,
//         })
//     }
// }

use std::fs::{self, File};
use std::io::{self, BufWriter, ErrorKind};

/// Decompress gzip result data
fn decompress_gzip_result(compressed_data: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;

    Ok(decompressed)
}
