    use std::{collections::{VecDeque}, sync::Arc};

    use core_affinity::CoreId;
    use tokio::{sync::Mutex};

    use tokio::{task};
    use crate::{all_tasks::fibonacci, various::{Job}};
    
    // Worker is mapped to a core id and runs the tasks located in each queue
    pub struct Worker{
        worker_id:usize,
        core_id:CoreId,
        thread_queue: Arc<Mutex<VecDeque<Job>>>
    }

    impl Worker{
        pub fn new(worker_id:usize, core_id:CoreId)->Self{
            let thread_queue = Arc::new(Mutex::new(VecDeque::new()));
            // let core = core_ids[i % num_cores];
            Worker{worker_id, core_id, thread_queue}
        }

        pub async fn add_to_queue(&self, jobs:Vec<Job>){
            let mut queue = self.thread_queue.lock().await;
            for j in jobs.iter(){
                queue.push_back(j.clone());
            }
        }

        pub fn store_result(task_id:usize, result:usize){
            // Store result in a file named after the task
            let file_name = format!("results/result_{}.txt", task_id);
            std::fs::write(&file_name, format!("Result: {}", result)).expect("Failed to write result to file");
        }

        pub async fn start(&self){
            println!("Worker: {:?} started on core id : {:?}", self.worker_id, self.core_id);
            loop{
                tokio::time::sleep(std::time::Duration::from_secs(5)).await; // for testing

                // Retrieve the next task from the queue runs it buy awaiting and returns result
                let mut queue = self.thread_queue.lock().await; // lock the mutex
                println!("Worker: {:?} queue: {:?} ", self.worker_id, queue);
                let my_task = queue.pop_front();

                match my_task{
                    None=> (),
                    Some(my_task_1)=>{
                        let task_id = my_task_1.id.clone(); // for testing
                        let task_name = my_task_1.name.clone(); // for testing
                        let task_n: usize = my_task_1.n.clone();
                        let core_id = self.core_id.clone(); // clone cause we need to pass by value a copy on each thread and it is bound to self
                        
                        // Spawn a blocking task to map the worker to the core 
                        let handle = task::spawn_blocking(move || {     
                            core_affinity::set_for_current(core_id);
                            println!("Running task {} on core {:?}", task_name, core_id);
                            let result = fibonacci(task_n);
                            Self::store_result(task_id, result);
                            println!("Finished task {}", task_name);
                            result
                        });
                        let _result = match handle.await {
                            Ok(result) => Some(result),
                            Err(e) => None,
                        };
                        ()
                        }
                    }
                }
            }
    }
