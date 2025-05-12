
use serde::Deserialize;
use spin_sdk::http::Request;


#[derive(Deserialize)]
pub struct AddToQueueRequest {
    pub n: u64,
}

impl AddToQueueRequest{
    pub fn new(req:Request)->Self{
        serde_json::from_slice(req.body()).expect("Invalid JSON")
    } 
}