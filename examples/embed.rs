use serde_json::Value;
use yedb::Database;

fn main() {
    let mut db = Database::new();
    db.set_db_path(&"/tmp/db1").unwrap();
    db.open().unwrap();
    let key_name = "test/key1";
    db.key_set(&key_name, Value::from(123_u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
    db.close().unwrap();
}
