
use spin_sdk::http::Request;

use crate::encoders_decoders_objects::AddToQueueRequest;


pub fn decode_request(req:Request)->AddToQueueRequest {
    AddToQueueRequest::new(req)
}
