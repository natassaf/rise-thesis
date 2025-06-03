use serde::{Deserialize, Deserializer};


#[derive(Deserialize)]
struct TaskInput {
    name: String,
    n: u64,
}


#[derive(Debug)]
pub struct Task{
    name:String,
    n: u64,
    pub priority: u64
}

impl<'de> Deserialize<'de> for Task {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = TaskInput::deserialize(deserializer)?;
        let priority = 0;
        Ok(Task {
            name: input.name,
            n: input.n,
            priority,
        })
    }
}