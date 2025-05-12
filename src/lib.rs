mod encoders_decoders;
mod requests_queue;
mod encoders_decoders_objects;

use std::collections::HashMap;

use spin_sdk::http::{IntoResponse, Request, Response};
use spin_sdk::http_component;


/// A simple Spin HTTP component.
#[http_component]
fn handle_thesis_web_server_spin(req: Request) -> anyhow::Result<impl IntoResponse> {
    let request = encoders_decoders::decode_request(req);
    let result: HashMap<String, String> = requests_queue::process_request(&request);
    let response_str = serde_json::to_string(&result).unwrap();
    Ok(Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(response_str)
        .build())
}
