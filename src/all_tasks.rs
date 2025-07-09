use serde_json::{json, Value};



pub fn fibonacci(n: usize) -> Value{
    if n == 0 {
        return json!(0);
    }
    let mut a = 0;
    let mut b = 1;
    for _ in 2..n {
        let temp = a + b;
        a = b;
        b = temp;
    }
    b.try_into().unwrap()
}