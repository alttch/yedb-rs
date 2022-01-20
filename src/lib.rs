//! # yedb - rugged crash-free embedded and client/server key-value database (Rust implementation)
//! 
//! ## Cargo crate
//! 
//! [crates.io/crates/yedb](https://crates.io/crates/yedb)
//! 
//! Features:
//! 
//! - **client-sync** synchronous client
//! - **client-async** asynchronous client
//! - **cli** yedb-cli
//! - **server** yedb-server
//! 
//! ## Why YEDB?
//! 
//! - Is it fast?
//! - Rust version is pretty fast, except writes are still slow if auto-flush is
//!   enabled.
//! 
//! - Is it smart?
//! - No
//! 
//! - So what is YEDB for?
//! - YEDB is ultra-reliable, thread-safe and very easy to use.
//! 
//! - I don't like Rust
//! - There are other [implementations](https://www.yedb.org).
//! 
//! [![Power loss data survive
//! demo](https://img.youtube.com/vi/i3hSWjrNqLo/0.jpg)](https://www.youtube.com/watch?v=i3hSWjrNqLo)
//! 
//! <https://www.youtube.com/watch?v=i3hSWjrNqLo>
//! 
//! 
//! YEDB is absolutely reliable rugged key-value database, which can survive in any
//! power loss, unless the OS file system die. Keys data is saved in the very
//! reliable way and immediately flushed to disk (this can be disabled to speed up
//! the engine but is not recommended - why then YEDB is used for).
//! 
//! ## Rust version features
//! 
//! - Rust version is built on top of [Serde](https://serde.rs) framework.
//! 
//! - All key values are *serde_json::Value* objects.
//! 
//! - Storage serialization formats supported: JSON (default), YAML, MessagePack
//!   and CBOR.
//! 
//! - As byte type is not supported by *serde_json::Value* at this moment, Rust
//!   version can not handle byte key values.
//! 
//! - Contains: embedded library, async server and command-line client (TCP/Unix
//!   socket only).
//! 
//! - The command-line client is very basic. If you need more features, use [yedb
//!   Python CLI](https://github.com/alttch/yedb-py).
//! 
//! ## Additional features over YEDB specs
//! 
//! - "delete" command does not delete keys, they are moved to .trash folder
//!   instead
//! 
//! - .trash folder is being cleaned when "purge" method is invoked.
//! 
//! - "auto\_bak" property tells server to automatically create backup key versions
//!   when key data is modified
//! 
//! - "bak" keys are hidden
//! 
//! ## Client/server
//! 
//! Binaries available at the [releases
//! page](https://github.com/alttch/yedb-rs/releases).
//! 
//! Run server:
//! 
//! ```shell
//! ./yedb-server /tmp/db1
//! ```
//! 
//! Use client:
//! 
//! ```shell
//! # get server info
//! ./yedb-cli info
//! # set key value
//! ./yedb-cli set x 5 -p number
//! # list all keys
//! ./yedb-cli ls /
//! # edit key with $EDITOR
//! ./yedb-cli edit x
//! # get key as JSON
//! ./yedb-cli get x
//! # get help for all commands
//! ./yedb-cli -h
//! ```
//! 
//! ## Code examples
//! 
//! The database/client objects can be safely shared between threads using any kind
//! of Lock/Mutex preferred.
//! 
//! ### Embedded example
//! 
//! ```rust
//! use yedb::Database;
//! use serde_json::Value;
//! 
//! let mut db = Database::new();
//! db.set_db_path("/tmp/db1").unwrap();
//! db.open().unwrap();
//! let key_name = "test/key1";
//! db.key_set(&key_name, Value::from(123_u8)).unwrap();
//! println!("{:?}", db.key_get(&key_name));
//! db.key_delete(&key_name).unwrap();
//! db.close().unwrap();
//! ```
//! 
//! ### TCP/Unix socket client example
//! 
//! ```rust
//! use yedb::YedbClient;
//! use serde_json::Value;
//! 
//! let mut client = YedbClient::new("tcp://127.0.0.1:8870");
//! let key_name = "test/key1";
//! client.key_set(&key_name, Value::from(123_u8)).unwrap();
//! println!("{:?}", client.key_get(&key_name));
//! client.key_delete(&key_name).unwrap();
//! ```
//! 
//! ### Async TCP/Unix socket client example
//! 
//! ```rust
//! use serde_json::Value;
//! use yedb::YedbClientAsync;
//! 
//! async fn test() {
//!     let mut client = YedbClientAsync::new("tcp://127.0.0.1:8870");
//!     let key_name = "test/key1";
//!     client.key_set(&key_name, Value::from(123_u8)).await.unwrap();
//!     println!("{:?}", client.key_get(&key_name).await);
//!     client.key_delete(&key_name).await.unwrap();
//! }
//! ```
//! 
//! ### Async TCP/Unix socket client pool example
//! 
//! ```rust
//! use serde_json::Value;
//! use std::sync::Arc;
//! use std::time::Duration;
//! use yedb::YedbClientPoolAsync;
//! 
//! async fn test() {
//!     let pool = Arc::new(
//!         YedbClientPoolAsync::create()
//!             .size(10)
//!             .path("tcp://127.0.0.1:8870")
//!             .retries(3)
//!             .timeout(Duration::from_secs(2))
//!             .build()
//!     );
//!     let mut futs = Vec::new();
//!     for i in 0..10 {
//!         let task_pool = pool.clone();
//!         let fut = tokio::spawn(async move {
//!             let mut client = task_pool.get().await;
//!             let key = format!("test/key{}", i);
//!             client.key_set(&key, Value::from(i)).await.unwrap();
//!             println!("{}", client.key_get(&key).await.unwrap());
//!             client.key_delete(&key).await.unwrap();
//!         });
//!         futs.push(fut);
//!     }
//!     for fut in futs {
//!         fut.await.unwrap();
//!     }
//! }
//! ```
//! 
//! ## Specification
//! 
//! [www.yedb.org](https://www.yedb.org/)
//! 
//! ## Some benchmark data
//! 
//! * CPU: Intel Core i7-8550U (4 cores)
//! * Drive: Samsung MZVLB512HAJQ-000L7 (NVMe)
//! 
//! - auto\_flush: false
//! - connection: Unix socket
//! - server workers: 2
//! - client threads: 4
//! 
//! ```shell
//! set/number: 8164 ops/sec
//! set/string: 7313 ops/sec
//! set/array: 7152 ops/sec
//! set/object: 5272 ops/sec
//! 
//! get/number: 49709 ops/sec
//! get/string: 33338 ops/sec
//! get/array: 31426 ops/sec
//! get/object: 11654 ops/sec
//! 
//! get(cached)/number: 122697 ops/sec
//! get(cached)/string: 61206 ops/sec
//! get(cached)/array: 59309 ops/sec
//! get(cached)/object: 34583 ops/sec
//! 
//! increment: 7079 ops/sec
//! ```
use fs2::FileExt;
use glob::glob;
use jsonschema::{Draft, JSONSchema};
use lru::LruCache;
use regex::Regex;
use serde::{de::Error as deError, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::process;
use std::time::{Duration, Instant, SystemTime, SystemTimeError};

use log::{debug, error, warn};

#[macro_use]
extern crate lazy_static;

pub const SERVER_ID: &str = "yedb-altt-rs";
pub const VERSION: &str = "0.2.1";
pub const ENGINE_VERSION: u8 = 1;

pub const DEFAULT_CACHE_SIZE: usize = 1000;

const SLEEP_STEP: Duration = Duration::from_millis(50);

lazy_static! {
    static ref RE_BAK: Regex = Regex::new(r"\.bak(\d)*$").unwrap();
}

trait ExplainValue {
    fn get_len(&self) -> Option<u64>;
    fn get_type(&self) -> String;
}

impl ExplainValue for Value {
    fn get_len(&self) -> Option<u64> {
        match self {
            Value::Null | Value::Bool(_) | Value::Number(_) => None,
            Value::String(v) => Some(v.len() as u64),
            Value::Array(v) => Some(v.len() as u64),
            Value::Object(v) => Some(v.len() as u64),
        }
    }
    fn get_type(&self) -> String {
        match self {
            Value::Null => "null".to_owned(),
            Value::Bool(_) => "boolean".to_owned(),
            Value::Number(_) => "number".to_owned(),
            Value::String(_) => "string".to_owned(),
            Value::Array(_) => "array".to_owned(),
            Value::Object(_) => "object".to_owned(),
        }
    }
}

#[path = "common.rs"]
pub mod common;
pub use common::{DBInfo, Error, ErrorKind, KeyExplained};

#[cfg(feature = "client-sync")]
#[path = "client.rs"]
pub mod client;
#[cfg(feature = "client-sync")]
pub use client::YedbClient;

#[cfg(feature = "client-async")]
#[path = "client_async.rs"]
pub mod client_async;
#[cfg(feature = "client-async")]
pub use client_async::YedbClientAsync;
#[cfg(feature = "client-async")]
pub use client_async::YedbClientPoolAsync;

#[cfg(feature = "client-elbus-async")]
#[path = "client_elbus_async.rs"]
pub mod client_elbus_async;
#[cfg(feature = "client-elbus-async")]
pub use client_elbus_async::YedbClientElbusAsync;

#[cfg(any(feature = "server", feature = "server-embed"))]
#[path = "server.rs"]
pub mod server;

#[derive(Debug)]
enum DataKey<'a> {
    Name(&'a str),
    File(&'a str),
}

impl<'a> DataKey<'a> {
    #[allow(dead_code)]
    fn is_file(&self) -> bool {
        match self {
            DataKey::Name(_) => false,
            DataKey::File(_) => true,
        }
    }
    fn is_name(&self) -> bool {
        match self {
            DataKey::Name(_) => true,
            DataKey::File(_) => false,
        }
    }
    fn get(&self) -> &str {
        match self {
            DataKey::Name(v) | DataKey::File(v) => v,
        }
    }
}

macro_rules! get_engine {
    ($e:expr) => {
        if let Some(engine) = $e.engine {
            engine
        } else {
            return Err(Error::new(
                ErrorKind::NotOpened,
                "The database is not opened",
            ));
        }
    };
}

macro_rules! impl_err_io {
    ($e: path) => {
        impl From<$e> for Error {
            fn from(e: $e) -> Self {
                Error::new(ErrorKind::IOError, e)
            }
        }
    };
}

macro_rules! impl_err_data {
    ($e: path) => {
        impl From<$e> for Error {
            fn from(e: $e) -> Self {
                Error::new(ErrorKind::DataError, e)
            }
        }
    };
}

impl_err_io!(glob::PatternError);
impl_err_io!(fs_extra::error::Error);
impl_err_io!(io::Error);

impl_err_data!(rmp_serde::decode::Error);
impl_err_data!(rmp_serde::encode::Error);
impl_err_data!(serde_cbor::Error);
impl_err_data!(serde_json::Error);
impl_err_data!(serde_yaml::Error);
impl_err_data!(hex::FromHexError);

impl From<jsonschema::CompilationError> for Error {
    fn from(e: jsonschema::CompilationError) -> Self {
        Error::new(ErrorKind::SchemaValidationError, e)
    }
}

impl From<SystemTimeError> for Error {
    fn from(e: SystemTimeError) -> Self {
        Error::new(ErrorKind::Other, e)
    }
}

macro_rules! timestamp_ns {
    () => {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos() as u64
    };
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SerializationEngine {
    Json,
    Msgpack,
    Cbor,
    Yaml,
}

impl Serialize for SerializationEngine {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl fmt::Display for SerializationEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SerializationEngine::Json => "json",
                SerializationEngine::Msgpack => "msgpack",
                SerializationEngine::Cbor => "cbor",
                SerializationEngine::Yaml => "yaml",
            }
        )
    }
}

impl SerializationEngine {
    pub fn as_u8(&self) -> u8 {
        match self {
            SerializationEngine::Json => 1,
            SerializationEngine::Msgpack => 2,
            SerializationEngine::Cbor => 3,
            SerializationEngine::Yaml => 4,
        }
    }

    /// # Panics
    ///
    /// Will panic if the format is unimplemented
    pub fn from_u8(fmt: u8) -> Self {
        match fmt {
            1 => SerializationEngine::Json,
            2 => SerializationEngine::Msgpack,
            3 => SerializationEngine::Cbor,
            4 => SerializationEngine::Yaml,
            _ => unimplemented!(),
        }
    }

    /// # Errors
    ///
    /// Will return errors if the format is unsupported
    pub fn from_string(fmt: &str) -> Result<Self, Error> {
        match fmt {
            "json" => Ok(SerializationEngine::Json),
            "msgpack" => Ok(SerializationEngine::Msgpack),
            "cbor" => Ok(SerializationEngine::Cbor),
            "yaml" => Ok(SerializationEngine::Yaml),
            _ => Err(Error::new(ErrorKind::UnsupportedFormat, fmt)),
        }
    }

    pub fn suffix(&self, checksums: bool) -> String {
        let mut sfx = match self {
            SerializationEngine::Json => ".json".to_owned(),
            SerializationEngine::Msgpack => ".mp".to_owned(),
            SerializationEngine::Cbor => ".cb".to_owned(),
            SerializationEngine::Yaml => ".yml".to_owned(),
        };
        if checksums {
            sfx += "c";
        }
        sfx
    }

    pub fn is_binary(&self) -> bool {
        match self {
            SerializationEngine::Json | SerializationEngine::Yaml => false,
            SerializationEngine::Cbor | SerializationEngine::Msgpack => true,
        }
    }

    /// # Errors
    ///
    /// Will return errors on deserialization errors
    pub fn deserialize(&self, buf: &[u8]) -> Result<Value, Error> {
        Ok(match self {
            SerializationEngine::Msgpack => rmp_serde::from_read_ref(buf)?,
            SerializationEngine::Cbor => serde_cbor::from_slice(buf)?,
            SerializationEngine::Json => serde_json::from_slice(buf)?,
            SerializationEngine::Yaml => serde_yaml::from_slice(buf)?,
        })
    }

    /// # Errors
    ///
    /// Will return errors on serialization errors
    pub fn serialize(&self, value: &Value) -> Result<Vec<u8>, Error> {
        Ok(match self {
            SerializationEngine::Msgpack => rmp_serde::to_vec_named(value)?,
            SerializationEngine::Cbor => serde_cbor::to_vec(value)?,
            SerializationEngine::Json => {
                let mut v = serde_json::to_vec(value)?;
                if v.is_empty() || v[v.len() - 1] != 0x0A_u8 {
                    v.push(0x0A_u8);
                }
                v
            }
            SerializationEngine::Yaml => {
                let mut v = serde_yaml::to_vec(value)?;
                if v.is_empty() || v[v.len() - 1] != 0x0A_u8 {
                    v.push(0x0A_u8);
                }
                v
            }
        })
    }
}

fn de_fmt<'de, D>(deserializer: D) -> Result<Option<SerializationEngine>, D::Error>
where
    D: Deserializer<'de>,
{
    match &String::deserialize(deserializer) {
        Ok(v) => match SerializationEngine::from_string(v) {
            Ok(v) => Ok(Some(v)),
            Err(_) => Ok(None),
        },
        Err(e) => Err(D::Error::custom(e)),
    }
}

#[derive(Deserialize, Serialize, Debug, Copy, Clone, PartialEq)]
struct Engine {
    #[serde(
        deserialize_with = "de_fmt",
        rename(serialize = "fmt", deserialize = "fmt")
    )]
    se: Option<SerializationEngine>,
    created: u64,
    version: u8,
    checksums: bool,
}

impl Engine {
    fn get_suffix(&self) -> String {
        self.se.unwrap().suffix(self.checksums)
    }
    fn is_serialization_binary(&self) -> bool {
        self.se.unwrap().is_binary()
    }
}

#[derive(Serialize, Debug)]
pub struct ServerInfo {
    pub name: String,
    pub version: u8,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerInfo {
    pub fn new() -> Self {
        Self {
            name: "yedb".to_owned(),
            version: ENGINE_VERSION,
        }
    }
}

fn sync_dir(dir: &str) -> Result<(), Error> {
    match fs::File::open(dir) {
        Ok(dh) => {
            debug!("Syncing dir {}", dir);
            dh.sync_all()?;
        }
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            debug!("Dir {} not found, skipping sync", dir);
        }
        Err(e) => return Err(Error::new(ErrorKind::IOError, e)),
    };
    Ok(())
}

fn lock_ex(fh: &fs::File, timeout: Duration) -> Result<bool, Error> {
    let start = Instant::now();
    let mut locked_instantly = true;
    debug!("Locking the database");
    loop {
        match fh.try_lock_exclusive() {
            Ok(()) => return Ok(locked_instantly),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                locked_instantly = false;
                std::thread::sleep(SLEEP_STEP);
                if Instant::now() - start > timeout {
                    return Err(Error::new(ErrorKind::TimeoutError, "lock timeout"));
                }
                continue;
            }
            Err(e) => return Err(Error::new(ErrorKind::IOError, e)),
        }
    }
}

fn fmt_key(key: &str) -> String {
    let mut x = 0;
    for c in key.chars() {
        if c == '/' {
            x += 1;
        } else {
            break;
        }
    }
    let name = match x {
        0 => key.to_owned(),
        _ => key[x..key.len()].to_owned(),
    };
    name.replace("../", "")
}

fn create_dirs(basepath: &str, dirname: &str) -> Result<Vec<String>, Error> {
    // own function to return vec of created dirs
    let parts = dirname.split('/');
    let mut created: Vec<String> = Vec::new();
    let mut cdir = basepath.to_string();
    for p in parts {
        cdir += "/";
        cdir += p;
        match fs::create_dir(&cdir) {
            Ok(_) => created.push(cdir.clone()),
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::IOError,
                    format!("Unable to create directory {}: {}", cdir, e),
                ))
            }
        };
    }
    Ok(created)
}

struct KeyInfo {
    key_file: String,
    checksum: Option<[u8; 32]>,
    metadata: fs::Metadata,
    stime: Option<u64>,
}

#[allow(clippy::struct_excessive_bools)]
pub struct Database {
    path: String,
    key_path: String,
    lock_path: String,
    pub auto_repair: bool,
    pub auto_flush: bool,
    pub write_modified_only: bool,
    pub timeout: Duration,
    pub lock_ex: bool,
    // auto-backup keys
    pub auto_bak: u64,
    // do not backup the following keys (and their subkeys)
    pub skip_bak: Vec<String>,
    pub strict_schema: bool,
    default_fmt: SerializationEngine,
    default_checksums: bool,
    meta_path: String,
    trash_path: String,
    engine: Option<Engine>,
    cache: Box<LruCache<String, Value>>,
    repair_recommended: bool,
    lock_fh: Option<fs::File>,
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.engine.is_some() {
            let _r = self.close();
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        Database {
            path: String::new(),
            key_path: String::new(),
            default_fmt: SerializationEngine::Json,
            default_checksums: true,
            auto_repair: true,
            auto_flush: true,
            auto_bak: 0,
            skip_bak: Vec::new(),
            strict_schema: false,
            lock_ex: true,
            write_modified_only: true,
            timeout: Duration::from_secs(5),
            lock_path: String::new(),
            meta_path: String::new(),
            trash_path: String::new(),
            cache: Box::new(LruCache::new(DEFAULT_CACHE_SIZE)),
            engine: None,
            repair_recommended: false,
            lock_fh: None,
        }
    }

    pub fn is_open(&self) -> bool {
        self.engine.is_some()
    }

    fn need_skip_bak(&self, key: &str) -> bool {
        for k in &self.skip_bak {
            let l = k.len();
            if k == key || (key.starts_with(k) && key.get(l..=l).map_or(false, |s| s == "/")) {
                return true;
            }
        }
        false
    }

    fn need_backup(&self, key: &str) -> bool {
        self.auto_bak > 0
            && !key.starts_with(".schema/")
            && key != ".schema"
            && !self.need_skip_bak(key)
    }

    /// # Errors
    ///
    /// Will return Err if the database is already open or the format is unknown
    pub fn set_default_fmt(&mut self, fmt: &str, checksums: bool) -> Result<(), Error> {
        debug!(
            "Setting the default format to {} with checksums={}",
            fmt, checksums
        );
        if self.engine.is_some() {
            Err(Error::new(
                ErrorKind::Busy,
                "the database is already opened",
            ))
        } else {
            self.default_fmt = match SerializationEngine::from_string(fmt) {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            self.default_checksums = checksums;
            Ok(())
        }
    }

    /// # Errors
    ///
    /// Will return Err if the database is already open
    pub fn set_db_path(&mut self, path: &str) -> Result<(), Error> {
        debug!("Setting the DB path to {}", path);
        if self.engine.is_some() {
            Err(Error::new(ErrorKind::Busy, "the database is opened"))
        } else {
            self.path = path.to_owned();
            self.key_path = self.path.clone() + "/keys";
            debug!("Key path set to {}", self.key_path);
            self.trash_path = self.key_path.clone() + "/.trash";
            self.lock_path = path.to_string() + "/db.lock";
            debug!("Lock file set to {}", self.lock_path);
            self.meta_path = path.to_string() + "/.yedb";
            debug!("Lock meta file set to {}", self.meta_path);
            Ok(())
        }
    }

    pub fn set_cache_size(&mut self, size: usize) {
        debug!("Setting the cache size to {} keys", size);
        self.cache.resize(size);
    }

    /// # Errors
    ///
    /// Will return Err if the database is already open
    pub fn set_lock_path(&mut self, path: &str) -> Result<(), Error> {
        debug!("Setting lock path to {}", path);
        if self.engine.is_some() {
            Err(Error::new(ErrorKind::Busy, "the database is opened"))
        } else {
            self.lock_path = path.to_owned();
            Ok(())
        }
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors or if the engine is not initialized
    pub fn open(&mut self) -> Result<ServerInfo, Error> {
        debug!("Opening the database, path: {}", self.path);
        if self.path.is_empty() {
            return Err(Error::new(ErrorKind::NotInitialized, "db path not set"));
        }
        if self.engine.is_some() {
            return Err(Error::new(
                ErrorKind::Busy,
                "the database is already opened",
            ));
        }
        self.repair_recommended = false;
        self.cache.clear();
        let db_engine: Option<Engine>;
        match fs::read_to_string(self.meta_path.clone()) {
            Ok(buf) => {
                let engine: Engine = serde_json::from_str(&buf)?;
                if engine.se.is_none() {
                    return Err(Error::new(
                        ErrorKind::UnsupportedFormat,
                        "unsupoorted database format",
                    ));
                } else if engine.version > ENGINE_VERSION {
                    return Err(Error::new(ErrorKind::UnsupportedVersion, engine.version));
                }
                db_engine = Some(engine);
                debug!("Database opened successfully");
            }
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                debug!("No database found, creating new");
                if Path::new(&self.path).exists() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "the directory already exists but no meta file found",
                    ));
                }
                let engine = Engine {
                    se: Some(self.default_fmt),
                    created: timestamp_ns!(),
                    version: ENGINE_VERSION,
                    checksums: self.default_checksums,
                };
                fs::create_dir_all(&self.path)?;
                let mut fh = fs::File::create(&self.meta_path)?;
                fh.write_all(serde_json::to_string(&engine)?.as_bytes())?;
                if self.auto_flush {
                    fh.flush()?;
                    fh.sync_all()?;
                }
                db_engine = Some(engine);
                debug!("Database created successfully");
            }
            Err(e) => {
                return Err(Error::new(ErrorKind::IOError, e));
            }
        }
        if self.lock_ex {
            let lock_path = Path::new(&self.lock_path);
            if lock_path.exists() {
                let lock_fh = fs::File::open(&self.lock_path)?;
                match lock_ex(&lock_fh, self.timeout) {
                    Ok(v) => self.repair_recommended = v,
                    Err(e) => return Err(e),
                };
            }
            let mut lock_fh = fs::File::create(&self.lock_path)?;
            match lock_ex(&lock_fh, self.timeout) {
                Ok(_) => {}
                Err(e) => return Err(e),
            };
            lock_fh.write_all(process::id().to_string().as_bytes())?;
            if self.auto_flush {
                lock_fh.flush()?;
                lock_fh.sync_all()?;
            }
            self.lock_fh = Some(lock_fh);
        }
        self.engine = db_engine;
        if self.repair_recommended {
            warn!("warning: database repair is recommended");
            if self.auto_repair {
                warn!("warning: starting auto-repair");
                match self.repair() {
                    Ok(_) => warn!("auto-repair completed"),
                    Err(e) => error!("auto-repair failed {}", e),
                };
            }
        } else {
            debug!("The database is clean, no repairing recommended");
        }
        let _r = fs::create_dir_all(&self.trash_path);
        let _r = fs::create_dir_all(&self.key_path);
        if self.auto_flush {
            sync_dir(&self.path)?;
        }
        Ok(ServerInfo::new())
    }

    /// # Errors
    ///
    /// Will return errors if the database is not opened or on I/O errors
    pub fn close(&mut self) -> Result<(), Error> {
        debug!("Closing the database {}", self.path);
        if self.engine.is_none() {
            return Err(Error::new(
                ErrorKind::NotOpened,
                "the database is not opened",
            ));
        }
        self.engine = None;
        if self.lock_fh.is_some() {
            self.lock_fh = None;
            fs::remove_file(&self.lock_path)?;
        }
        Ok(())
    }

    fn find_schema_key(&mut self, key: &str) -> Result<Option<String>, Error> {
        if key.starts_with(".schema/") || key == ".schema" {
            debug!("Schema key for {} is virtual", key);
            return Ok(Some("!JSON Schema Draft-7".to_owned()));
        }
        let mut schema_key = ".schema/".to_owned() + key;
        loop {
            if self.key_exists(&schema_key)? {
                debug!("Found Schema schema_key for {} at {}", key, schema_key);
                return Ok(Some(schema_key));
            }
            match schema_key.rfind('/') {
                Some(pos) => {
                    schema_key = schema_key[..pos].to_string();
                }
                None => {
                    break;
                }
            };
        }
        Ok(None)
    }

    fn validate_schema(&mut self, key: &str, value: &Value) -> Result<(), Error> {
        debug!("Validating schema for {}", key);
        if key.starts_with(".schema/") || key == ".schema" {
            JSONSchema::options()
                .with_draft(Draft::Draft7)
                .compile(value)?;
            Ok(())
        } else {
            // TODO cache compiled schemas
            match self.find_schema_key(key)? {
                Some(schema_key) => {
                    let schema = self.get_key_data(DataKey::Name(&schema_key), false)?.0;
                    let compiled = JSONSchema::options()
                        .with_draft(Draft::Draft7)
                        .compile(&schema)?;
                    compiled.validate(value).map_err(|e| {
                        let mut err: String = String::new();
                        for error in e {
                            if !err.is_empty() {
                                err += "\n";
                            }
                            err += error.to_string().as_str();
                        }
                        Error::new(ErrorKind::SchemaValidationError, err)
                    })
                }
                None => {
                    if self.strict_schema {
                        Err(Error::new(
                            ErrorKind::SchemaValidationError,
                            "schema not defined",
                        ))
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }

    fn set_key_data(
        &mut self,
        key: &str,
        value: Value,
        stime: Option<u64>,
        ignore_schema: bool,
    ) -> Result<(), Error> {
        debug!("Setting value for key {}", key);
        let key = fmt_key(key);
        if key.is_empty() {
            return Err(Error::new(ErrorKind::KeyNotFound, key));
        }
        let engine = get_engine!(self);
        let mut dts: Vec<String> = Vec::new();
        if !ignore_schema {
            self.validate_schema(&key, &value)?;
        }
        let key_file = self.key_path.clone() + "/" + key.as_str() + engine.get_suffix().as_str();
        if self.write_modified_only {
            match self.key_get(&key) {
                Ok(v) => {
                    if v == value {
                        debug!("Key {} not modified, skipping set", key);
                        return Ok(());
                    }
                }
                Err(_) => {
                    debug!("Key {} not cached", key);
                }
            }
        }
        let key_dir = match key.rfind('/') {
            Some(pos) => {
                let key_dir = self.key_path.clone() + "/" + &key[0..pos];
                let dirs = create_dirs(&self.key_path, &key[0..pos])?;
                if self.auto_flush {
                    for dir in dirs {
                        let d = dir[..dir.rfind('/').unwrap()].to_string();
                        if !dts.contains(&d) {
                            dts.push(d);
                        }
                    }
                }
                key_dir
            }
            None => self.key_path.clone(),
        };
        let temp_file = self.key_path.clone() + "/" + key.as_str() + ".tmp";
        let content = engine.se.unwrap().serialize(&value)?;
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let digest = hasher.finalize();
        let mut file = fs::File::create(&temp_file)?;
        let is_binary = engine.is_serialization_binary();
        if engine.checksums {
            if is_binary {
                file.write_all(&digest)?;
            } else {
                file.write_all(hex::encode(&digest).as_bytes())?;
                file.write_all(&[0x0A_u8])?;
            }
            let stime = match stime {
                Some(v) => v,
                None => timestamp_ns!(),
            };
            if is_binary {
                file.write_all(&stime.to_le_bytes())?;
            } else {
                file.write_all(hex::encode(&stime.to_le_bytes()).as_bytes())?;
                file.write_all(&[0x0A_u8])?;
            }
        }
        file.write_all(&content)?;
        if self.auto_flush {
            file.flush()?;
            file.sync_all()?;
        }
        drop(file);
        fs::rename(&temp_file, key_file)?;
        if self.auto_flush && !dts.contains(&key_dir) {
            dts.push(key_dir);
        }
        self.cache.pop(&key);
        self.cache.put(key, value);
        if self.auto_flush {
            for dir in dts {
                let _r = sync_dir(&dir);
            }
        }
        Ok(())
    }

    fn purge_cache_by_path(&mut self, key: &str) {
        let key = if key.ends_with('/') {
            key.to_owned()
        } else {
            key.to_owned() + "/"
        };
        debug!("Purging cache for {}*", key);
        let to_remove: Vec<_> = self
            .cache
            .iter()
            .filter(|&(k, _)| k.starts_with(&key))
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove {
            self.cache.pop(&k);
        }
    }

    fn _delete_key(
        &mut self,
        key: &str,
        recursive: bool,
        no_flush: bool,
        dir_only: bool,
    ) -> Result<(), Error> {
        debug!(
            "Deleting key: {}, recursive: {}, no_flush: {}, dir_only: {}",
            key, recursive, no_flush, dir_only
        );
        let engine = get_engine!(self);
        let key = fmt_key(key);
        if key.starts_with(".trash/") || key == ".trash" {
            return Err(Error::new(
                ErrorKind::KeyNotFound,
                "Use purge to remove trashed items",
            ));
        }
        if key.is_empty() && !recursive {
            return Ok(());
        }
        self.cache.pop(&key);
        let mut dts: Vec<String> = Vec::new();
        let dn = self.key_path.clone() + "/" + key.as_str();
        if Path::new(&dn).is_dir() && recursive {
            let trashed = format!(
                "{}/{}.{}",
                self.trash_path.clone(),
                key.replace("/", "_"),
                timestamp_ns!()
            );
            debug!("renaming dir {} to {}", &dn, &trashed);
            fs::create_dir_all(&self.trash_path)?;
            let mut options = fs_extra::dir::CopyOptions::new();
            options.copy_inside = true;
            options.overwrite = true;
            fs_extra::dir::move_dir(&dn, &trashed, &options)?;
            dts.push(dn);
            self.purge_cache_by_path(&key);
        }
        let mut key_dir = match key.rfind('/') {
            Some(n) => {
                let key_path = &key[..n];
                self.key_path.clone() + "/" + key_path
            }
            None => self.key_path.clone(),
        };
        if !dir_only && !key.is_empty() {
            let key_file =
                self.key_path.clone() + "/" + key.as_str() + engine.get_suffix().as_str();
            //if self.auto_flush && !no_flush {
            //match fs::File::create(&key_file) {
            //Ok(mut fh) => {
            //fh.flush()?;
            //fh.sync_all()?;
            //}
            //Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            //Err(e) => return Err(Error::new(ErrorKind::IOError, e)),
            //}
            //}
            let trashed = format!(
                "{}/{}.{}{}",
                self.trash_path.clone(),
                key.replace("/", "_"),
                timestamp_ns!(),
                engine.get_suffix()
            );
            debug!("renaming file {} to {}", &key_file, &trashed);
            fs::create_dir_all(&self.trash_path)?;
            let mut options = fs_extra::file::CopyOptions::new();
            options.overwrite = true;
            let _r = fs_extra::file::move_file(&key_file, &trashed, &options);
            if !dts.contains(&key_dir) {
                dts.push(key_dir.clone());
            }
        }
        loop {
            if key_dir == self.key_path {
                if self.auto_flush && !dts.contains(&key_dir) {
                    dts.push(key_dir);
                }
                break;
            }
            if fs::remove_dir(&key_dir).is_err() {
                if self.auto_flush && !dts.contains(&key_dir) {
                    dts.push(key_dir);
                }
                break;
            }
            key_dir = key_dir[0..key_dir.rfind('/').unwrap()].to_owned();
        }
        if self.auto_flush && !no_flush {
            for dir in dts {
                let _r = sync_dir(&dir);
            }
            let _r = sync_dir(&self.trash_path);
        }
        Ok(())
    }

    fn list_subkeys(&self, key: &str, hidden: bool) -> Result<Vec<String>, Error> {
        debug!("Listing subkeys of {}, hidden: {}", key, hidden);
        let engine = get_engine!(self);
        let key = fmt_key(key);
        let mut result: Vec<String> = Vec::new();
        let mut pattern = self.key_path.clone();
        let suffix = engine.get_suffix();
        let suffix_len = suffix.len();
        let path_len = self.key_path.len();
        if !key.is_empty() {
            pattern += "/";
            pattern += key.as_str();
            pattern += "/";
        }
        pattern += "/**/*";
        pattern += suffix.as_str();
        for entry in (glob(pattern.as_str())?).flatten() {
            let k = entry.to_str().unwrap();
            let key_name = &k[path_len + 1..k.len() - suffix_len];
            if hidden
                || (!key_name.starts_with('.')
                    && !key_name.contains("/.")
                    && !RE_BAK.is_match(key_name))
            {
                result.push(key_name.to_owned());
            }
        }
        Ok(result)
    }

    fn get_key_data(
        &mut self,
        key: DataKey,
        extended_info: bool,
    ) -> Result<(Value, Option<KeyInfo>), Error> {
        debug!("Getting key {:?}, extended_info: {}", key, extended_info);
        let engine = get_engine!(self);
        if key.is_name() {
            let key = fmt_key(key.get());
            if key.is_empty() {
                return Err(Error::new(ErrorKind::KeyNotFound, key));
            } else if !extended_info {
                if let Some(v) = self.cache.get(&key) {
                    debug!("Using cached value for {}", key);
                    return Ok((v.clone(), None));
                }
            }
        }
        let is_binary = engine.is_serialization_binary();
        let key_file = match key {
            DataKey::File(v) => v.to_owned(),
            DataKey::Name(v) => self.key_path.clone() + "/" + v + engine.get_suffix().as_str(),
        };
        let buf = match fs::read(&key_file) {
            Ok(v) => v,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(Error::new(ErrorKind::KeyNotFound, key.get()))
            }
            Err(e) => return Err(Error::new(ErrorKind::IOError, e)),
        };
        let checksum: Option<[u8; 32]>;
        let stime: Option<u64>;
        let value: Value = engine.se.unwrap().deserialize(if engine.checksums {
            if (is_binary && buf.len() < 41) || (!is_binary && buf.len() < 83) {
                return Err(Error::new(
                    ErrorKind::DataError,
                    format!("the key file is corrupted: {}", key_file),
                ));
            }
            let mut hasher = Sha256::new();
            if is_binary {
                hasher.update(&buf[40..buf.len()]);
            } else {
                hasher.update(&buf[82..buf.len()]);
            }
            let digest = hasher.finalize();
            if (is_binary && *digest != buf[0..32])
                || (!is_binary && *digest != *hex::decode(&buf[0..64])?.as_slice())
            {
                return Err(Error::new(
                    ErrorKind::DataError,
                    format!("checksum does not match: {}", key_file),
                ));
            }
            checksum = Some(digest.into());
            if is_binary {
                stime = Some(u64::from_le_bytes([
                    buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
                ]));
                &buf[40..buf.len()]
            } else {
                let s = hex::decode(&buf[65..81])?;
                stime = Some(u64::from_le_bytes([
                    s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
                ]));
                &buf[82..buf.len()]
            }
        } else {
            checksum = None;
            stime = None;
            &buf
        })?;
        self.cache.put(key.get().to_owned(), value.clone());
        Ok((
            value,
            if extended_info {
                let metadata = fs::metadata(&key_file)?;
                Some(KeyInfo {
                    key_file,
                    checksum,
                    metadata,
                    stime,
                })
            } else {
                None
            },
        ))
    }

    fn list_key_and_subkeys(&mut self, key: &str, hidden: bool) -> Result<Vec<String>, Error> {
        let mut result = match self.list_subkeys(key, hidden) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        match self.key_exists(key) {
            Ok(v) => {
                if v {
                    result.push(key.to_owned());
                }
            }
            Err(e) => return Err(e),
        };
        Ok(result)
    }

    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    fn _purge(&mut self, keep_broken: bool) -> Result<Vec<String>, Error> {
        debug!("Purge requested, keep_broken: {}", keep_broken);
        let mut result: Vec<String> = Vec::new();
        let engine = get_engine!(self);
        let mut dts: Vec<String> = Vec::new();
        let path_len = self.key_path.len();
        let suffix = engine.get_suffix();
        let suffix_len = suffix.len();
        debug!("Cleaning up trash");
        fs::remove_dir_all(&self.trash_path)?;
        debug!("Cleaning up files and broken keys");
        // clean up files and broken keys
        for entry in glob(&(self.key_path.clone() + "/**/*"))? {
            match entry {
                Ok(p) => {
                    let k = p.to_str().unwrap();
                    let mut need_remove = false;
                    if !p.is_dir() && k != self.trash_path {
                        if k.ends_with(&suffix) {
                            if !keep_broken {
                                let key_name = k[path_len + 1..k.len() - suffix_len].to_owned();
                                if self.get_key_data(DataKey::Name(&key_name), false).is_err() {
                                    result.push(key_name);
                                    need_remove = true;
                                }
                            }
                        } else if !keep_broken || !k.ends_with(".tmp") {
                            need_remove = true;
                        }
                    }
                    if need_remove {
                        fs::remove_file(k)?;
                        if self.auto_flush {
                            let parent = p.parent().unwrap().to_str().unwrap().to_owned();
                            if !dts.contains(&parent) {
                                dts.push(parent);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error while browsing db directory: {}", e);
                }
            }
        }
        debug!("Cleaning up directories");
        // clean up directories
        let mut dirs: Vec<String> = Vec::new();
        for entry in glob(&(self.key_path.clone() + "/**"))? {
            match entry {
                Ok(p) => {
                    if p.is_dir() {
                        dirs.push(p.to_str().unwrap().to_string());
                    }
                }
                Err(e) => {
                    return Err(Error::new(ErrorKind::IOError, e));
                }
            }
        }
        dirs.sort();
        dirs.reverse();
        for d in dirs {
            if fs::remove_dir(&d).is_ok() {
                let parent = d[..d.rfind('/').unwrap()].to_string();
                if self.auto_flush && !dts.contains(&parent) {
                    dts.push(parent);
                }
            }
        }
        if self.auto_flush {
            for dir in dts {
                let _r = sync_dir(&dir);
            }
        }
        fs::create_dir_all(&self.trash_path)?;
        self.cache.clear();
        sync_dir(&self.key_path)?;
        debug!("Purge completed");
        Ok(result)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_load_from_serialized(&mut self, data: &[Value]) -> Result<(), Error> {
        for d in data {
            match d {
                Value::Array(v) => {
                    let key: String = match v[0].as_str() {
                        Some(v) => v.to_string(),
                        None => {
                            return Err(Error::new(
                                ErrorKind::DataError,
                                format!("Invalid key name '{:?}'", v),
                            ))
                        }
                    };
                    self.set_key_data(&key, v[1].clone(), None, true)?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::DataError,
                        format!("Invalid record '{}'", d),
                    ))
                }
            }
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Will return Err if the database is not open
    ///
    /// # Panics
    ///
    /// Will panic on internal errors
    pub fn info(&self) -> Result<DBInfo, Error> {
        let engine = get_engine!(self);
        Ok(DBInfo {
            repair_recommended: self.repair_recommended,
            auto_flush: self.auto_flush,
            cached_keys: self.cache.len(),
            cache_size: self.cache.cap(),
            auto_bak: self.auto_bak,
            strict_schema: self.strict_schema,
            path: self.path.clone(),
            lock_path: self.lock_path.clone(),
            server: (SERVER_ID.to_owned(), VERSION.to_owned()),
            fmt: engine.se.unwrap().to_string(),
            checksums: engine.checksums,
            created: engine.created,
            version: engine.version,
        })
    }

    /// # Errors
    ///
    /// Will return Err on invalid property or value
    #[allow(clippy::cast_possible_truncation)]
    pub fn server_set(&mut self, name: &str, value: Value) -> Result<(), Error> {
        debug!("Setting server option {}={}", name, value);
        macro_rules! invalid_server_option_value {
            ($n:expr, $value: expr) => {
                return Err(Error::new(
                    ErrorKind::DataError,
                    format!("Invalid server option value {}={}", $n, $value),
                ))
            };
        }
        match name {
            "auto_flush" => match value {
                Value::Bool(v) => self.auto_flush = v,
                _ => invalid_server_option_value!(name, &value),
            },
            "auto_bak" => match value.as_u64() {
                Some(v) => self.auto_bak = v,
                _ => invalid_server_option_value!(name, &value),
            },
            "strict_schema" => match value {
                Value::Bool(v) => self.strict_schema = v,
                _ => invalid_server_option_value!(name, &value),
            },
            "repair_recommended" => match value {
                Value::Bool(v) => self.repair_recommended = v,
                _ => invalid_server_option_value!(name, &value),
            },
            "cache_size" => match &value {
                Value::Number(v) => {
                    let size: u64 = match v.as_u64() {
                        Some(v) => v,
                        None => invalid_server_option_value!(name, &value),
                    };
                    self.cache.resize(size as usize);
                }
                _ => invalid_server_option_value!(name, &value),
            },
            _ => {
                return Err(Error::new(
                    ErrorKind::DataError,
                    format!("Invalid server option {}", name),
                ))
            }
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn purge(&mut self) -> Result<Vec<String>, Error> {
        self._purge(false)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn safe_purge(&mut self) -> Result<(), Error> {
        match self._purge(true) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn purge_cache(&mut self) -> Result<(), Error> {
        self.cache.clear();
        Ok(())
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_delete(&mut self, key: &str) -> Result<(), Error> {
        if self.need_backup(key) {
            for n in 1..=self.auto_bak {
                let key_name = format!("{}.bak{}", key, n);
                if let Err(e) = self._delete_key(&key_name, false, false, false) {
                    if e.kind() != ErrorKind::KeyNotFound {
                        return Err(e);
                    }
                }
            }
        }
        self._delete_key(key, false, false, false)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_delete_recursive(&mut self, key: &str) -> Result<(), Error> {
        self._delete_key(key, true, false, false)
    }

    // TODO context
    // TODO convert_fmt()

    fn key_exists(&mut self, key: &str) -> Result<bool, Error> {
        let key = fmt_key(key);
        if key.is_empty() {
            return Ok(false);
        }
        let engine = get_engine!(self);
        if self.cache.contains(&key) {
            Ok(true)
        } else {
            let key_file =
                self.key_path.clone() + "/" + key.as_str() + engine.get_suffix().as_str();
            Ok(Path::new(&key_file).exists())
        }
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found and on I/O errors
    ///
    /// # Panics
    ///
    /// Will panic on internal errors
    #[allow(clippy::cast_possible_truncation)]
    pub fn key_explain(&mut self, key: &str) -> Result<KeyExplained, Error> {
        let v = match self.get_key_data(DataKey::Name(key), true) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let value = v.0;
        let info = v.1.unwrap();
        let value_len = value.get_len();
        let value_type = value.get_type();
        Ok(KeyExplained {
            value,
            schema: self.find_schema_key(key)?,
            len: value_len,
            tp: value_type,
            mtime: info
                .metadata
                .modified()?
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_nanos() as u64,
            sha256: info.checksum,
            stime: info.stime,
            size: info.metadata.len(),
            file: info.key_file,
        })
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found and on I/O errors
    pub fn key_get(&mut self, key: &str) -> Result<Value, Error> {
        Ok(self.get_key_data(DataKey::Name(key), false)?.0)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_get_recursive(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        let mut result = Vec::new();
        for key in self.list_key_and_subkeys(key, false)? {
            let value = self.get_key_data(DataKey::Name(&key), false)?.0;
            result.push((key, value));
        }
        Ok(result)
    }

    /// Get key field
    ///
    /// field may contain a simple path (e.g. field/subfield/subsubfield)
    ///
    /// # Errors
    ///
    /// Will return Err if the key is not found, on I/O or serialization errors
    pub fn key_get_field(&mut self, key: &str, field: &str) -> Result<Value, Error> {
        let mut data: &Value = &self.key_get(key)?;
        for f in field.split('/') {
            match data {
                Value::Object(v) => match v.get(f) {
                    Some(value) => data = value,
                    None => {
                        return Err(Error::new(
                            ErrorKind::FieldNotFound,
                            format!("no such field '{}'", field),
                        ));
                    }
                },
                _ => return Err(Error::new(ErrorKind::DataError, "field is not an object")),
            }
        }
        Ok(data.clone())
    }

    /// Set key field
    ///
    /// field may contain a simple path (e.g. field/subfield/subsubfield)
    ///
    /// # Errors
    ///
    /// Will return Err on I/O or serialization errors
    ///
    /// # Panics
    ///
    /// Will panic on internal serde errors
    pub fn key_set_field(&mut self, key: &str, field: &str, value: Value) -> Result<(), Error> {
        let mut key_data: Value = match self.key_get(key) {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::KeyNotFound => Value::from(serde_json::Map::new()),
            Err(e) => return Err(e),
        };
        let mut data_ptr = &mut key_data;
        let fname = match field.rfind('/') {
            Some(pos) => {
                for f in field[..pos].split('/') {
                    match data_ptr.as_object_mut() {
                        Some(v) => {
                            if !v.contains_key(f) {
                                let m = serde_json::Map::new();
                                let d = Value::from(m);
                                v.insert(f.to_owned(), d);
                            }
                            data_ptr = if let Some(v) = v.get_mut(f) {
                                v
                            } else {
                                return Err(Error::new(
                                    ErrorKind::DataError,
                                    "unable to get field",
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::new(ErrorKind::DataError, "field is not an object"))
                        }
                    }
                }
                &field[pos + 1..]
            }
            None => field,
        };
        match data_ptr.as_object_mut() {
            Some(o) => {
                let oldval = o.get(fname);
                if oldval.is_none() || oldval.unwrap() != &value || !self.write_modified_only {
                    o.insert(fname.to_owned(), value);
                    self.cache.pop(&fmt_key(key));
                    self.key_set(key, key_data)?;
                }
                Ok(())
            }
            None => Err(Error::new(ErrorKind::DataError, "field is not object")),
        }
    }

    /// Delete key field
    ///
    /// field may contain a simple path (e.g. field/subfield/subsubfield)
    ///
    /// # Errors
    ///
    /// Will return Err on I/O or serialization errors
    pub fn key_delete_field(&mut self, key: &str, field: &str) -> Result<(), Error> {
        let mut key_data: Value = match self.key_get(key) {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::KeyNotFound => Value::from(serde_json::Map::new()),
            Err(e) => return Err(e),
        };
        let mut data_ptr = &mut key_data;
        let fname = match field.rfind('/') {
            Some(pos) => {
                for f in field[..pos].split('/') {
                    match data_ptr.as_object_mut() {
                        Some(v) => {
                            if !v.contains_key(f) {
                                // parent field not found - abort
                                return Ok(());
                            }
                            data_ptr = if let Some(v) = v.get_mut(f) {
                                v
                            } else {
                                return Err(Error::new(
                                    ErrorKind::DataError,
                                    "unable to get field",
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::new(ErrorKind::DataError, "field is not an object"))
                        }
                    }
                }
                &field[pos + 1..]
            }
            None => field,
        };
        match data_ptr.as_object_mut() {
            Some(o) => {
                if o.contains_key(fname) || !self.write_modified_only {
                    o.remove(fname);
                    self.cache.pop(&fmt_key(key));
                    self.key_set(key, key_data)?;
                }
                Ok(())
            }
            None => Err(Error::new(ErrorKind::DataError, "field is not object")),
        }
    }

    /// # Errors
    ///
    /// Will return Err on I/O or serialization errors
    pub fn key_set(&mut self, key: &str, value: Value) -> Result<(), Error> {
        self.validate_schema(key, &value)?;
        if self.need_backup(key) {
            for n in (1..=self.auto_bak).rev() {
                let key_from = if n == 1 {
                    key.to_owned()
                } else {
                    format!("{}.bak{}", key, n - 1)
                };
                let key_to = format!("{}.bak{}", key, n);
                match self._rename(&key_from, &key_to, false, true) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::KeyNotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
        self.set_key_data(key, value, None, true)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_list(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.list_key_and_subkeys(key, false)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    pub fn key_list_all(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.list_key_and_subkeys(key, true)
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found or is unable to be copied
    pub fn key_copy(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        debug!("Copying key {} to {}", key, dst_key);
        let value = self.get_key_data(DataKey::Name(key), false)?.0;
        self.set_key_data(dst_key, value, None, false)
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found, is not numeric or I/O error occured
    pub fn key_increment(&mut self, key: &str) -> Result<i64, Error> {
        debug!("Incrementing key {}", key);
        let key = fmt_key(key);
        let mut value = match self.get_key_data(DataKey::Name(&key), false) {
            Ok(v) => match v.0.as_i64() {
                Some(n) => n,
                None => return Err(Error::new(ErrorKind::DataError, "Unable to increment key")),
            },
            Err(ref e) if e.kind() == ErrorKind::KeyNotFound => 0_i64,
            Err(e) => return Err(e),
        };
        if value == std::i64::MAX {
            return Err(Error::new(ErrorKind::DataError, "counter overflow"));
        }
        value += 1;
        match self.set_key_data(&key, Value::from(value), None, false) {
            Ok(_) => Ok(value),
            Err(e) => Err(e),
        }
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found, is not numeric or I/O error occured
    pub fn key_decrement(&mut self, key: &str) -> Result<i64, Error> {
        debug!("Decrementing key {}", key);
        let key = fmt_key(key);
        let mut value = match self.get_key_data(DataKey::Name(&key), false) {
            Ok(v) => match v.0.as_i64() {
                Some(n) => n,
                None => return Err(Error::new(ErrorKind::DataError, "Unable to decrement key")),
            },
            Err(ref e) if e.kind() == ErrorKind::KeyNotFound => 0_i64,
            Err(e) => return Err(e),
        };
        if value == std::i64::MIN {
            return Err(Error::new(ErrorKind::DataError, "counter overflow"));
        }
        value -= 1;
        match self.set_key_data(&key, Value::from(value), None, false) {
            Ok(_) => Ok(value),
            Err(e) => Err(e),
        }
    }

    /// # Errors
    ///
    /// Will return Err if the key is not found or is unable to be renamed
    pub fn key_rename(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        self._rename(key, dst_key, true, false)
    }

    fn _rename(
        &mut self,
        key: &str,
        dst_key: &str,
        flush: bool,
        key_only: bool,
    ) -> Result<(), Error> {
        debug!("Renaming key {} to {}", key, dst_key);
        let engine = get_engine!(self);
        let mut dts: Vec<String> = Vec::new();
        let key = fmt_key(key);
        let dst_key = fmt_key(dst_key);
        if key.is_empty() || dst_key.is_empty() {
            return Err(Error::new(ErrorKind::KeyNotFound, key));
        }

        let pos = dst_key.rfind('/');
        let dst_key_path = match pos {
            Some(p) => &dst_key[..p],
            None => "",
        };
        let dst_key_dir = self.key_path.clone() + "/" + dst_key_path;

        let dirs = create_dirs(&self.key_path, dst_key_path)?;
        if self.auto_flush && flush {
            for dir in dirs {
                let d = dir[..dir.rfind('/').unwrap()].to_string();
                if !dts.contains(&d) {
                    dts.push(d);
                }
            }
        }

        let mut renamed = false;

        let key_file = self.key_path.clone() + "/" + key.as_str() + engine.get_suffix().as_str();
        let dst_key_file =
            self.key_path.clone() + "/" + dst_key.as_str() + engine.get_suffix().as_str();

        // rename file
        let mut options = fs_extra::file::CopyOptions::new();
        options.overwrite = true;
        match fs_extra::file::move_file(&key_file, &dst_key_file, &options) {
            Ok(_) => {
                renamed = true;
                if let Some(v) = self.cache.pop(&key) {
                    self.cache.put(dst_key.clone(), v);
                }
                if self.auto_flush && flush {
                    let d1 = key_file[..key_file.rfind('/').unwrap()].to_string();
                    if !dts.contains(&d1) {
                        dts.push(d1);
                    }
                    if !dts.contains(&dst_key_dir) {
                        dts.push(dst_key_dir);
                    }
                }
            }
            Err(e) => match e.kind {
                fs_extra::error::ErrorKind::NotFound => {}
                _ => return Err(Error::new(ErrorKind::IOError, e)),
            },
        };

        // rename dir
        if !key_only {
            let dir_name = self.key_path.clone() + "/" + key.as_str();
            let dst_dir_name = self.key_path.clone() + "/" + dst_key.as_str();

            let mut options = fs_extra::dir::CopyOptions::new();
            options.copy_inside = true;
            options.overwrite = true;
            match fs_extra::dir::move_dir(&dir_name, &dst_dir_name, &options) {
                Ok(_) => {
                    renamed = true;
                    self.purge_cache_by_path(&dir_name);
                    if self.auto_flush && flush {
                        let d1 = dir_name[..dir_name.rfind('/').unwrap()].to_string();
                        let d2 = dst_dir_name[..dst_dir_name.rfind('/').unwrap()].to_string();
                        if !dts.contains(&d1) {
                            dts.push(d1);
                        }
                        if !dts.contains(&d2) {
                            dts.push(d2);
                        }
                    }
                }
                Err(e) => match e.kind {
                    fs_extra::error::ErrorKind::NotFound => {}
                    fs_extra::error::ErrorKind::Io(e)
                        if e.kind() == io::ErrorKind::InvalidInput => {}
                    _ => return Err(Error::new(ErrorKind::IOError, e)),
                },
            };
        }

        if self.auto_flush && flush {
            for dir in dts {
                let _r = sync_dir(&dir);
            }
        }

        if renamed {
            self._delete_key(&key, false, false, true)?;
            Ok(())
        } else {
            Err(Error::new(ErrorKind::KeyNotFound, key))
        }
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    ///
    /// # Panics
    ///
    /// Will panic on internal path errors
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    pub fn check(&mut self) -> Result<Vec<String>, Error> {
        debug!("Checking the database");
        let engine = get_engine!(self);
        let mut broken: Vec<String> = Vec::new();
        let suffix = engine.get_suffix();
        let path_len = self.key_path.len();
        for entry in glob(&(self.key_path.clone() + "/**/*"))? {
            match entry {
                Ok(p) => {
                    let key_file = p.to_str().unwrap().to_string();
                    if key_file.ends_with(&suffix) {
                        if self.get_key_data(DataKey::File(&key_file), false).is_err() {
                            let key =
                                key_file[path_len + 1..key_file.rfind('.').unwrap()].to_string();
                            debug!("Broken key found: {}", key);
                            broken.push(key);
                        }
                    } else if key_file.ends_with(".tmp") {
                        let key = key_file[path_len + 1..key_file.rfind('.').unwrap()].to_string();
                        debug!("Lost key found: {}", key);
                        broken.push(key);
                    }
                }
                Err(e) => {
                    error!("Error while browsing db directory: {}", e);
                }
            }
        }
        Ok(broken)
    }

    /// # Errors
    ///
    /// Will return Err on I/O errors
    ///
    /// # Panics
    ///
    /// Will panic on internal path errors
    pub fn repair(&mut self) -> Result<Vec<(String, bool)>, Error> {
        debug!("Repairing the database");
        let engine = get_engine!(self);
        let mut result: Vec<(String, bool)> = Vec::new();
        let mut dts: Vec<String> = Vec::new();
        let suffix = engine.get_suffix();
        let path_len = self.key_path.len();
        self.cache.clear();
        for entry in glob(&(self.key_path.clone() + "/**/*.tmp"))? {
            match entry {
                Ok(p) => {
                    let key_file = p.to_str().unwrap().to_string();
                    if self.get_key_data(DataKey::File(&key_file), false).is_ok() {
                        fs::rename(
                            &key_file,
                            &(key_file[..key_file.rfind('.').unwrap()].to_string() + &suffix),
                        )?;
                        let key = key_file[path_len + 1..key_file.rfind('.').unwrap()].to_string();
                        debug!("Recovered lost key {}", key);
                        result.push((key, true));
                    } else {
                        fs::remove_file(&key_file)?;
                        let key = key_file[path_len + 1..key_file.rfind('.').unwrap()].to_string();
                        debug!("Deleted broken key {}", key);
                        result.push((key, false));
                    }
                    if self.auto_flush {
                        let parent = key_file[..key_file.rfind('/').unwrap()].to_owned();
                        if !dts.contains(&parent) {
                            dts.push(parent);
                        }
                    }
                }
                Err(e) => {
                    error!("Error while browsing db directory: {}", e);
                }
            }
        }
        if self.auto_flush {
            for dir in dts {
                let _r = sync_dir(&dir);
            }
        }
        for key in self._purge(false)? {
            result.push((key, false));
        }
        debug!("Repair completed");
        self.repair_recommended = false;
        Ok(result)
    }

    /// # Errors
    ///
    /// Will return Err if keys are unable to be read
    pub fn key_dump(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        debug!("Dump requested for {}", key);
        let mut result = Vec::new();
        for key in self.list_key_and_subkeys(key, true)? {
            if let Ok(v) = self.get_key_data(DataKey::Name(&key), false) {
                debug!("Dumped key {}", key);
                result.push((key, v.0));
            }
        }
        Ok(result)
    }

    /// # Errors
    ///
    /// Will return Err if key is not found or unable to be read
    pub fn key_load(&mut self, data: Vec<(String, Value)>) -> Result<(), Error> {
        debug!("Key load requested");
        for d in data {
            debug!("Loading key {}", d.0);
            self.set_key_data(&d.0, d.1, None, true)?;
        }
        Ok(())
    }
}

//const CONTEXT_ENGINE_VERSION: u8 = 1;

//use byteorder::{LittleEndian, ReadBytesExt};

//pub trait ContextConversible {
//fn get_len() -> usize;
//fn to_bytes(&self, max_len: u32) -> Vec<u8>;
//fn from_bytes(buf: &Vec<u8>) -> Self;
//fn increment(&self) -> Self;
//fn decrement(&self) -> Self;
//fn copy(&self) -> Self;
//}

//impl ContextConversible for i64 {
//fn get_len() -> usize {
//8
//}
//fn to_bytes(&self, _len: u32) -> Vec<u8> {
//let mut result = Vec::new();
//result.extend(&self.to_le_bytes());
//result
//}
//fn from_bytes(buf: &Vec<u8>) -> Self {
//buf.as_slice().read_i64::<LittleEndian>().unwrap()
//}
//fn increment(&self) -> Self {
//let value = *self;
//if value == std::i64::MAX {
//std::i64::MIN
//} else {
//value + 1
//}
//}
//fn decrement(&self) -> Self {
//let value = *self;
//if value == std::i64::MIN {
//std::i64::MIN
//} else {
//value - 1
//}
//}
//fn copy(&self) -> Self {
//*self
//}
//}

//impl ContextConversible for f64 {
//fn get_len() -> usize {
//8
//}
//fn to_bytes(&self, _len: u32) -> Vec<u8> {
//let mut result = Vec::new();
//result.extend(&self.to_le_bytes());
//result
//}
//fn from_bytes(buf: &Vec<u8>) -> Self {
//buf.as_slice().read_f64::<LittleEndian>().unwrap()
//}
//fn increment(&self) -> Self {
//*self
//}
//fn decrement(&self) -> Self {
//*self
//}
//fn copy(&self) -> Self {
//*self
//}
//}

//impl ContextConversible for String {
//fn get_len() -> usize {
//0
//}
//fn to_bytes(&self, len: u32) -> Vec<u8> {
//let mut result = Vec::new();
//result.extend(self.as_bytes());
//if result.len() > len as usize {
//result.truncate(len as usize);
//} else {
//result.resize(len as usize, 0u8);
//}
//result
//}
//fn from_bytes(buf: &Vec<u8>) -> Self {
//let mut b = buf.clone();
//match b.iter().position(|x| *x == 0u8) {
//Some(pos) => b.truncate(pos),
//None => {}
//}
//Self::from_utf8(buf.clone()).unwrap()
//}
//fn increment(&self) -> Self {
//self.clone()
//}
//fn decrement(&self) -> Self {
//self.clone()
//}

//fn copy(&self) -> Self {
//self.to_owned()
//}
//}

//pub struct Context<T: ContextConversible> {
//path: String,
//version: u8,
//element_type: u8,
//element_len: u32,
//size: u64,
//cache: LruCache<u64, T>,
//fh: std::fs::File,
//}

//impl<T: ContextConversible> Context<T> {
//pub fn create(path: &str, size: u64, len: Option<u32>) -> Self {
//let mut fh = fs::File::create(&path).unwrap();
//let mut buf: Vec<u8> = Vec::new();
//let element_len = match len {
//Some(v) => v,
//None => T::get_len() as u32,
//};
//buf.push(CONTEXT_ENGINE_VERSION); // 0
//buf.extend(&[0u8]); // 1
//buf.extend(&element_len.to_le_bytes()); // 2-5
//buf.extend(&size.to_le_bytes()); // 6-13
//fh.write(&buf).unwrap();
//for _ in 0..size {
//fh.write(&vec![0u8; element_len as usize]).unwrap();
//}
//let fh = fs::OpenOptions::new()
//.read(true)
//.write(true)
//.open(path)
//.unwrap();
//Self {
//path: path.to_owned(),
//version: CONTEXT_ENGINE_VERSION,
//element_type: 0,
//element_len,
//size,
//fh,
//cache: LruCache::new(DEFAULT_CACHE_SIZE),
//}
//}

//pub fn set_cache_size(&mut self, size: usize) {
//self.cache.resize(size);
//}

//pub fn clear_cache(&mut self) {
//self.cache.clear();
//}

//pub fn increment(&mut self, reg: u64) {
//let value: T = self.get(reg);
//self.set(reg, value.increment());
//}

//pub fn decrement(&mut self, reg: u64) {
//let value = self.get(reg);
//self.set(reg, value.decrement());
//}

//pub fn get(&mut self, reg: u64) -> T {
//let value = self.cache.get(&reg);
//match value {
//Some(v) => v.copy(),
//None => {
//self.fh
//.seek(io::SeekFrom::Start(14 + reg * self.element_len as u64))
//.unwrap();
//let mut buf: Vec<u8> = vec![0u8; self.element_len as usize];
//self.fh.read_exact(buf.as_mut_slice()).unwrap();
//T::from_bytes(&buf)
//}
//}
//}

//pub fn set(&mut self, reg: u64, value: T) {
//if reg > self.size {
//panic!("overflow");
//}
//self.fh
//.seek(io::SeekFrom::Start(14 + reg * self.element_len as u64))
//.unwrap();
//self.fh.write(&value.to_bytes(self.element_len)).unwrap();
//self.cache.put(reg, value);
//}
//}

#[cfg(test)]
mod tests {
    #[test]
    fn test_db() {
        use super::*;
        use serde_json::map::Map;
        use serde_json::Value;
        use std::fs;
        let db_path = "/tmp/yedb-test-db";

        for checksums in vec![false, true] {
            for db_format in vec!["json", "yaml", "msgpack", "cbor"] {
                let _ = fs::remove_dir_all(&db_path);
                let mut db = Database::new();
                db.set_db_path(&db_path).unwrap();
                db.set_default_fmt(&db_format, checksums).unwrap();
                db.set_cache_size(100);
                db.open().unwrap();
                let i = db.info().unwrap();
                assert_eq!(i.repair_recommended, false);
                assert_eq!(i.auto_flush, true);
                assert_eq!(i.cached_keys, 0);
                assert_eq!(i.cache_size, 100);
                assert_eq!(i.path, db_path);
                assert_eq!(i.fmt, db_format);
                assert_eq!(i.checksums, checksums);

                db.server_set("auto_flush", Value::from(false)).unwrap();
                db.server_set("repair_recommended", Value::from(true))
                    .unwrap();
                db.server_set("cache_size", Value::from(1000)).unwrap();

                let i = db.info().unwrap();
                assert_eq!(i.repair_recommended, true);
                assert_eq!(i.auto_flush, false);
                assert_eq!(i.cache_size, 1000);

                db.purge().unwrap();
                db.safe_purge().unwrap();

                db.key_set("test", Value::from(123)).unwrap();
                db.key_set("x/y/z", Value::from("test")).unwrap();
                db.key_set(".a", Value::from("z")).unwrap();

                assert_eq!(db.key_list("/").unwrap(), vec!["test", "x/y/z"]);
                assert_eq!(db.key_list_all("/").unwrap(), vec![".a", "test", "x/y/z"]);

                assert_eq!(db.key_get("test").unwrap().as_u64().unwrap(), 123);

                let i = db.info().unwrap();
                assert_eq!(i.cached_keys, 3);

                db.purge_cache().unwrap();
                let i = db.info().unwrap();
                assert_eq!(i.cached_keys, 0);

                db.key_delete("test").unwrap();

                assert_eq!(db.key_get("test").is_err(), true);

                db.key_rename("x/y/z", "x/y/a").unwrap();

                assert_eq!(db.key_get("x/y/z").is_err(), true);

                assert_eq!(db.key_get("x/y/a").unwrap().as_str().unwrap(), "test");

                db.key_delete_recursive("x/y").unwrap();

                assert_eq!(db.key_get("x/y/a").is_err(), true);

                db.key_copy(".a", "a/b/c").unwrap();

                assert_eq!(db.key_get("a/b/c").unwrap().as_str().unwrap(), "z");

                let mut schema = Map::new();

                schema.insert("type".to_owned(), Value::from("number"));

                db.key_set(".schema/n", Value::from(schema)).unwrap();

                assert_eq!(db.key_set("n/x", Value::from("test")).is_err(), true);

                db.key_set("n/x", Value::from(123)).unwrap();

                let ki = db.key_explain("n/x").unwrap();

                assert_eq!(ki.value, Value::from(123));
                assert_eq!(ki.schema, Some(".schema/n".to_owned()));
                assert_eq!(ki.len, None);
                assert_eq!(ki.tp, "number");

                assert_eq!(ki.sha256.is_some(), checksums);

                let se = SerializationEngine::from_string(db_format).unwrap();

                let key_path = db_path.to_owned() + "/keys/n/x" + se.suffix(checksums).as_str();

                assert_eq!(ki.file, key_path);

                db.close().unwrap();
            }
        }

        let _ = fs::remove_dir_all(&db_path);
    }

    //fn test_ctx() {
    //let mut ctx = super::Context::<String>::create("/tmp/ctx1", 10, Some(100));
    //ctx.set_cache_size(0);
    //ctx.set(9, "this is a test".to_owned());
    //ctx.set(10, "123".to_owned());
    //ctx.set(9, -777.0);
    //ctx.set(10, std::f64::MAX);
    //println!("{}", ctx.get(9));

    //let dbpath: String = "/tmp/db1".to_owned();
    //let mut db = super::Database::new();
    //db.set_db_path(&dbpath).unwrap();
    //db.set_default_fmt(&"msgpack".to_owned(), true).unwrap();
    //db.open().unwrap();
    //let key: String = "tests/rs/x".to_owned();
    //db.key_set(&key, super::Value::from(2993)).unwrap();
    //println!("{}", db.key_get(&key).unwrap());
    //println!("purge: {:?}", db.safe_purge().unwrap());
    //println!("{:?}", db.key_explain(&key).unwrap());
    //println!("{}", db.key_exists(&key).unwrap());
    //println!("{}", db.key_exists(&"tsts/rs/aaa".to_owned()).unwrap());
    //println!("{:?}", db.info().unwrap());
    //println!("{:?}", db.key_list(&"/".to_owned()).unwrap());
    //println!("{}", db.key_get(&key).unwrap());
    //println!("{:?}", db.key_get(&"abc".to_owned()));
    //println!("{:?}", db.check());
    //
    //db.key_delete_recursive(&"/".to_owned()).unwrap();
    //db.key_delete_recursive(&"/tests/rs".to_owned()).unwrap();
    //std::thread::sleep(std::time::Duration::from_secs(3));
    //println!("{}", db.get(&key).unwrap());
    //db.close().unwrap();
    //}
}
