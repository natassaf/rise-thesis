use std::{collections::HashMap, time::Instant};

use crate::encoders_decoders_objects::AddToQueueRequest;
use std::collections::VecDeque;
use once_cell::sync::Lazy;
use std::sync::Mutex;


pub static REQUESTS_QUEUE: Lazy< Mutex<VecDeque<MyTask>>> = Lazy::new(|| {
    println!("Initializing config...");
    Mutex::new(VecDeque::new()) // Do your setup here
});


#[derive(Debug)]
pub enum Tasks{
    Fibonacci
}

#[derive(Debug)]
pub struct MyTask{
    n:u64,
    task_type: Tasks
}

impl MyTask{
    pub fn new_fib_task(req: &AddToQueueRequest)->Self{
        Self{
            n:req.n,
            task_type:Tasks::Fibonacci
        }
    }
}

pub fn add_to_queue(req: &AddToQueueRequest){
    let redis_address = "redis://127.0.0.1:6379";
    let task = MyTask::new_fib_task(req);
    let connection = spin_sdk::redis::Connection::open(&redis_address).unwrap();
    let data = connection.get("greeting").unwrap();
    println!("data: {:?}", String::from_utf8_lossy(&data.unwrap()));
}

pub fn process_request(req: &AddToQueueRequest)->HashMap<String, String>{
    let start = Instant::now();
    let duration = start.elapsed();
    add_to_queue(&req);
    let mut result = HashMap::new();
    result.insert("Duration".to_string(), duration.as_nanos().to_string());
    result
}