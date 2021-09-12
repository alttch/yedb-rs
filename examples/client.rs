use yedb::YedbClient;
use serde_json::Value;

fn main() {
    let mut db = YedbClient::new("tcp://127.0.0.1:8870");
    let key_name = "test/key1";
    db.key_set(&key_name, Value::from(123_u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
}
