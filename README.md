# yedb - rugged embedded and client/server key-value database (Rust implementation)

## Why YEDB?

- Is it fast?
- Rust version is pretty fast, except writes are still slow if auto-flush is
  enabled.

- Is it smart?
- No

- So what is YEDB for?
- YEDB is ultra-reliable, thread-safe and very easy to use.

- I don't like Rust
- There are other [implementations](https://www.yedb.org)

YEDB is absolutely reliable rugged key-value database, which can survive in any
power loss, unless the OS file system die. Keys data is saved in the very
reliable way and immediately flushed to disk (this can be disabled to speed up
the engine but is not recommended - why then YEDB is used for).

## Rust version features

- Rust version is built on top of [Serde](https://serde.rs) framework.

- All key values are *serde_json::Value* objects.

- Storage serialization formats supported: JSON (default), YAML, MessagePack
  and CBOR.

- As binary type is not supported by *serde_json::Value* at this moment, Rust
  version can not handle binary key values.

- Contains: embedded library, async server and command-line client (TCP/Unix
  socket only).

- The command-line client is very basic. If you need more features, use [yedb
  Python CLI](https://github.com/alttch/yedb-py).

## Embedded example

```rust
use yedb::Database;
use serde_json::Value;

fn main() {
    let mut db = yedb::Database::new();
    db.set_db_path(&"/tmp/db1").unwrap();
    db.open().unwrap();
    let key_name = "test/key1";
    db.key_set(&key_name, Value::from(123u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
    db.close().unwrap();
}
```

## TCP client example

```rust
use yedb::YedbClient;
use serde_json::Value;

fn main() {
    let mut db = yedb::YedbClient::new("tcp://127.0.0.1:8870");
    let key_name = "test/key1";
    db.key_set(&key_name, Value::from(123u8)).unwrap();
    println!("{:?}", db.key_get(&key_name));
    db.key_delete(&key_name).unwrap();
}
```

## Cargo crate

[crates.io/crates/yedb](https://crates.io/crates/yedb)

## Client/server binaries

Available at [releases page](https://github.com/alttch/yedb-rs/releases).

## Specification

[www.yedb.org](https://www.yedb.org/)

