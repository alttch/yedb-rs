//use yedb::*;
use serde_json::Value;

fn main() {
    let mut db = yedb::YedbClient::new("tcp://127.0.0.1:8870");
    let key_name = "test/key1".to_owned();
    db.key_set(&key_name, Value::from(123u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
}
