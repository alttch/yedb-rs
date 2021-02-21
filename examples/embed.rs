//use yedb::{Database, ErrorKind};
use serde_json::Value;

fn main() {
    let mut db = yedb::Database::new();
    db.set_db_path(&"/tmp/db1".to_owned()).unwrap();
    db.open().unwrap();
    let key_name = "test/key1".to_owned();
    db.key_set(&key_name, Value::from(123u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
    db.close().unwrap();
}
