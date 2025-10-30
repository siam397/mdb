use serde_json::{json, Value};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

// Common test data generation
pub struct TestData {
    pub key: String,
    pub value: Value,
}

impl TestData {
    pub fn generate_single() -> Self {
        Self {
            key: generate_random_key(),
            value: generate_sample_json(),
        }
    }

    pub fn generate_batch(size: usize) -> Vec<Self> {
        (0..size)
            .map(|_| Self::generate_single())
            .collect()
    }
}

fn generate_random_key() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

fn generate_sample_json() -> Value {
    let types = ["user", "order", "product", "review"];
    let statuses = ["active", "pending", "completed", "cancelled"];
    
    json!({
        "id": thread_rng().gen::<u32>(),
        "type": types[thread_rng().gen_range(0..types.len())],
        "status": statuses[thread_rng().gen_range(0..statuses.len())],
        "timestamp": chrono::Utc::now().timestamp(),
        "metadata": {
            "version": format!("{}.{}.{}", 
                thread_rng().gen_range(0..5),
                thread_rng().gen_range(0..10),
                thread_rng().gen_range(0..100)
            ),
            "priority": thread_rng().gen_range(1..6),
            "tags": (0..thread_rng().gen_range(1..4))
                .map(|_| generate_random_key())
                .collect::<Vec<String>>()
        }
    })
}