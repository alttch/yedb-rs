use chrono::{DateTime, Local};
use colored::Colorize;
use openssl::sha::Sha256;
use serde_json::Value;
use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::process;
use std::sync::atomic;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yedb::{
    Error, ErrorKind, SerializationEngine, YedbClientAsync, YedbClientAsyncExt,
    YedbClientBusRtAsync, YedbClientLocalAsync, ENGINE_VERSION, VERSION,
};

use clap::Clap;

#[macro_use]
extern crate prettytable;

#[macro_use]
extern crate bma_benchmark;

const DUMP_BUF_SIZE: usize = 32768;

#[derive(PartialEq, Eq, Copy, Clone)]
enum BenchmarkOp {
    Set,
    Get,
    GetCached,
}

impl fmt::Display for BenchmarkOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BenchmarkOp::Set => "key_set",
                BenchmarkOp::Get => "key_get",
                BenchmarkOp::GetCached => "key_get(cached)",
            }
        )
    }
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
async fn benchmark(
    db: &mut Box<dyn YedbClientAsyncExt>,
    path: &str,
    nt: u32,
    iterations: u32,
) -> Result<(), Error> {
    let i = db.info().await.unwrap();
    let old_cache_size = i.cache_size;
    db.key_delete_recursive(".benchmark").await.unwrap();
    db.server_set("cache_size", Value::from(iterations * 4))
        .await
        .unwrap();
    let test_number = Value::from(777.777);
    let mut test_string = String::new();
    for _ in 0..1000 {
        test_string += "x";
    }
    let test_string = Value::from(test_string);
    let test_array: Vec<f64> = vec![777.777; 100];
    let test_array = Value::from(test_array);
    let mut test_dict = serde_json::Map::new();
    for i in 0..100 {
        test_dict.insert(format!("v{}", i), Value::from(f64::from(i) * 777.777));
    }
    let test_dict = Value::from(test_dict);
    for bm_op in [BenchmarkOp::Set, BenchmarkOp::Get, BenchmarkOp::GetCached] {
        for op in vec![
            ("number", test_number.clone()),
            ("string", test_string.clone()),
            ("array", test_array.clone()),
            ("object", test_dict.clone()),
        ] {
            let mut handlers = Vec::new();
            let errors = Arc::new(atomic::AtomicU32::new(0));
            staged_benchmark_start!(&format!("{} {}", bm_op, op.0));
            for thread_no in 0..nt {
                let op = op.clone();
                let op_name = op.0;
                let op_value = op.1;
                let db_path = path.to_owned();
                let errs = errors.clone();
                let mut session = create_client(&db_path, thread_no + 1).await?;
                let t = tokio::spawn(async move {
                    for x in 0..(iterations / nt) {
                        let key_name = format!(".benchmark/t{}/{}_{}", thread_no, &op_name, x);
                        if match bm_op {
                            BenchmarkOp::Set => {
                                session.key_set(&key_name, op_value.clone()).await.is_err()
                            }
                            BenchmarkOp::Get | BenchmarkOp::GetCached => {
                                session.key_get(&key_name).await.is_err()
                            }
                        } {
                            errs.fetch_add(1, atomic::Ordering::SeqCst);
                        }
                    }
                });
                handlers.push(t);
            }
            for h in handlers {
                h.await.unwrap();
            }
            staged_benchmark_finish_current!(iterations, errors.load(atomic::Ordering::SeqCst));
        }
        if bm_op == BenchmarkOp::Set {
            db.purge_cache().await.unwrap();
        }
    }
    let mut handlers = Vec::new();
    let errors = Arc::new(atomic::AtomicU32::new(0));
    staged_benchmark_start!("key_increment");
    for thread_no in 0..nt {
        let db_path = path.to_owned();
        let errs = errors.clone();
        let mut session = create_client(&db_path, thread_no + 1).await?;
        let t = tokio::spawn(async move {
            let key_name = format!(".benchmark/incr/increment_{}", thread_no);
            for _ in 0..(iterations / nt) {
                if session.key_increment(&key_name).await.is_err() {
                    errs.fetch_add(1, atomic::Ordering::SeqCst);
                }
            }
        });
        handlers.push(t);
    }
    for h in handlers {
        h.await.unwrap();
    }
    staged_benchmark_finish_current!(iterations, errors.load(atomic::Ordering::SeqCst));
    println!("Cleaning up...");
    db.key_delete_recursive(".benchmark").await.unwrap();
    db.purge().await.unwrap();
    db.server_set("cache_size", Value::from(old_cache_size))
        .await
        .unwrap();
    staged_benchmark_print!();
    Ok(())
}

#[derive(Clap)]
struct Opts {
    #[clap(
        short = 'C',
        long = "connect",
        about = "path to database dir or socket (must end with .sock, .socket or .ipc) or tcp://host:port or rt://<BUS_PATH>:<BUS_TARGET> for BUS/RT",
        default_value = "tcp://127.0.0.1:8870"
    )]
    path: String,
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Clap)]
enum Cmd {
    Get(GetCommand),
    GetField(GetFieldCommand),
    #[clap(about = "Gets key value as a batch source")]
    Source(SourceCommand),
    Incr(KeyCommand),
    Decr(KeyCommand),
    Explain(KeyCommand),
    Edit(KeyEditCommand),
    Set(SetCommand),
    SetField(SetFieldCommand),
    DeleteField(DeleteFieldCommand),
    r#Copy(KeyDstCommand),
    Rename(KeyDstCommand),
    Delete(KeyRCommand),
    Ls(KeyLsCommand),
    Test,
    Info,
    Server(ServerPropCommand),
    Benchmark(BenchmarkCommand),
    Dump(DumpCommands),
    Check,
    Repair,
    Purge,
    Version,
}

#[derive(Clap)]
struct KeyCommand {
    key: String,
}

#[derive(Clap)]
struct KeyEditCommand {
    key: String,
    #[clap(long = "default", about = "default value (file), '-' for stdin")]
    default: Option<String>,
}

#[derive(Copy, Clone)]
enum ConvertBools {
    No,
    OneZero,
    One,
}

impl std::str::FromStr for ConvertBools {
    type Err = ErrorKind;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "no" => Ok(ConvertBools::No),
            "onezero" => Ok(ConvertBools::OneZero),
            "one" => Ok(ConvertBools::One),
            _ => Err(ErrorKind::InvalidParameter),
        }
    }
}

#[derive(Clap)]
struct SourceCommand {
    key: String,
    #[clap(long, about = "value prefix")]
    prefix: Option<String>,
    #[clap(
        long,
        about = "convert booleans (no / onezero / one)",
        default_value = "no"
    )]
    convert_bool: ConvertBools,
}

#[derive(Clap)]
struct ServerPropCommand {
    prop: String,
    value: String,
}

#[derive(Clap)]
struct BenchmarkCommand {
    #[clap(long, default_value = "4")]
    workers: u32,
    #[clap(long, default_value = "10000")]
    iterations: u32,
}

#[derive(Clap)]
enum PropBool {
    True,
    False,
}

#[derive(Clap)]
struct KeyDstCommand {
    key: String,
    dst_key: String,
}

#[derive(Clap)]
struct KeyLsCommand {
    key: String,
    #[clap(short, long, about = "Include hidden")]
    all: bool,
}

#[derive(Clap)]
struct KeyRCommand {
    key: String,
    #[clap(short, long)]
    recursive: bool,
}

#[derive(Clap)]
struct GetCommand {
    key: String,
    #[clap(short, long)]
    recursive: bool,
    #[clap(
        long,
        about = "convert booleans (no / onezero / one)",
        default_value = "no"
    )]
    convert_bool: ConvertBools,
}

#[derive(Clap)]
struct GetFieldCommand {
    key: String,
    field: String,
    #[clap(
        long,
        about = "convert booleans (no / onezero / one)",
        default_value = "no"
    )]
    convert_bool: ConvertBools,
}

#[derive(Clap)]
struct SetCommand {
    key: String,
    #[clap(about = "Value to set, '-' for stdin")]
    value: String,
    #[clap(short = 'p', long, default_value = "string")]
    r#type: SetType,
}

#[derive(Clap)]
struct SetFieldCommand {
    key: String,
    field: String,
    #[clap(about = "Value to set, '-' for stdin")]
    value: String,
    #[clap(short = 'p', long, default_value = "string")]
    r#type: SetType,
}

#[derive(Clap)]
struct DeleteFieldCommand {
    key: String,
    field: String,
}
enum SetType {
    Num,
    Str,
    Bool,
    Json,
    Yaml,
}

#[derive(Clap)]
enum DumpCommands {
    Save(DumpSaveCommand),
    Load(DumpLoadCommand),
    View(DumpViewCommand),
}

#[derive(Clap)]
struct DumpSaveCommand {
    key: String,
    file: String,
}

#[derive(Clap)]
struct DumpLoadCommand {
    file: String,
}

#[derive(Clap)]
struct DumpViewCommand {
    #[clap(about = "'-' for stdin")]
    file: String,
    #[clap(short = 'y', long)]
    full: bool,
}

impl std::str::FromStr for SetType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "number" => Ok(SetType::Num),
            "string" => Ok(SetType::Str),
            "boolean" => Ok(SetType::Bool),
            "json" => Ok(SetType::Json),
            "yaml" => Ok(SetType::Yaml),
            _ => Err(Error::new(ErrorKind::Other, "Invalid type")),
        }
    }
}

async fn format_value(value: String, p: SetType) -> Result<Value, Error> {
    let s = match value.as_str() {
        "-" => {
            let mut buffer = String::new();
            tokio::io::stdin().read_to_string(&mut buffer).await?;
            buffer
        }
        _ => value,
    };
    match p {
        SetType::Num => match s.parse::<i64>() {
            Ok(v) => Ok(Value::from(v)),
            Err(_) => match s.parse::<f64>() {
                Ok(v) => Ok(Value::from(v)),
                Err(e) => Err(Error::new(ErrorKind::Other, e)),
            },
        },
        SetType::Str => Ok(Value::String(s)),
        SetType::Bool => match s.as_str() {
            "true" => Ok(Value::from(true)),
            "false" => Ok(Value::from(false)),
            _ => Err(Error::new(ErrorKind::Other, "value error")),
        },
        SetType::Json => match serde_json::from_str(&s) {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        },
        SetType::Yaml => match serde_yaml::from_str(&s) {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        },
    }
}

fn output_result<T: std::fmt::Display>(result: Result<T, Error>) -> i32 {
    match result {
        Ok(v) => {
            output_value(v);
            0
        }
        Err(e) => {
            output_error(e);
            1
        }
    }
}

fn output_result_bool(result: Result<Value, Error>, mode: ConvertBools) -> i32 {
    match result {
        Ok(value) => match value {
            Value::Bool(b) => {
                println!("{}", convert_bool(Some(b), mode));
                0
            }
            Value::Null => {
                println!("{}", convert_bool(None, mode));
                0
            }
            _ => {
                output_value(value);
                0
            }
        },
        Err(e) => {
            output_error(e);
            1
        }
    }
}

fn print_ok() {
    println!("{}", "OK".green().bold());
}

fn output_result_ok(result: Result<(), Error>) -> i32 {
    match result {
        Ok(_) => {
            print_ok();
            0
        }
        Err(e) => {
            output_error(e);
            1
        }
    }
}

fn output_value<T: std::fmt::Display>(value: T) {
    println!("{}", value);
}

fn output_error<T: std::fmt::Display>(err: T) {
    eprintln!("{}", err.to_string().red());
}

macro_rules! output_editor_error {
    ($e:expr) => {
        output_error(Error::new(ErrorKind::Other, $e));
        getch::Getch::new().getch().unwrap();
    };
}

async fn edit_key(db: &mut Box<dyn YedbClientAsyncExt>, key: &str, value: Option<&Value>) -> i32 {
    let mut code = 0;
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finish();
    let temp_file_name = format!(
        "{}/yedb-{}.yml",
        env::var("TEMP").unwrap_or_else(|_| env::var("TMP").unwrap_or_else(|_| "/tmp".to_owned())),
        hex::encode(&digest)
    );
    let editor = env::var("EDITOR").unwrap_or_else(|_| "vi".to_owned());
    let wres = match value {
        Some(v) => fs::write(&temp_file_name, serde_yaml::to_string(&v).unwrap()),
        None => Ok(()),
    };
    match wres {
        Ok(_) => loop {
            match process::Command::new(&editor).arg(&temp_file_name).spawn() {
                Ok(mut cmd) => {
                    cmd.wait().unwrap();
                    match fs::read_to_string(&temp_file_name) {
                        Ok(content) => match serde_yaml::from_str(&content) {
                            Ok(v) => {
                                if let Some(val) = value {
                                    if &v == val {
                                        break;
                                    }
                                };
                                match db.key_set(key, v).await {
                                    Ok(_) => {
                                        print_ok();
                                        break;
                                    }
                                    Err(e) => {
                                        output_editor_error!(e);
                                    }
                                }
                            }
                            Err(e) => {
                                output_editor_error!(e);
                            }
                        },
                        Err(e) => {
                            output_error(Error::new(ErrorKind::Other, e));
                            code = 3;
                            break;
                        }
                    }
                }
                Err(e) => {
                    output_error(Error::new(ErrorKind::Other, e));
                    code = 3;
                    break;
                }
            }
        },
        Err(e) => {
            output_error(Error::new(ErrorKind::Other, e));
            code = 3;
        }
    }
    let _r = fs::remove_file(&temp_file_name);
    code
}

macro_rules! format_bool {
    ($v:expr) => {
        match ($v.as_str()) {
            "true" => Value::from(true),
            "false" => Value::from(false),
            _ => {
                return Err(Error::new(ErrorKind::Other, "value error"));
            }
        }
    };
}

async fn server_set_prop(
    db: &mut Box<dyn YedbClientAsyncExt>,
    prop: &str,
    value: String,
) -> Result<(), Error> {
    let val = match prop {
        "auto_flush" | "repair_recommended" => format_bool!(value),
        "cache_size" | "auto_bak" => match value.parse::<usize>() {
            Ok(v) => Value::from(v),
            Err(e) => {
                return Err(Error::new(ErrorKind::Other, e));
            }
        },
        _ => {
            return Err(Error::new(ErrorKind::Other, "Option not supported"));
        }
    };
    db.server_set(prop, val).await
}

fn ctable(titles: Vec<&str>) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    let format = prettytable::format::FormatBuilder::new()
        .column_separator(' ')
        .borders(' ')
        .separators(
            &[prettytable::format::LinePosition::Title],
            prettytable::format::LineSeparator::new('-', '-', '-', '-'),
        )
        .padding(0, 1)
        .build();
    table.set_format(format);
    let mut titlevec: Vec<prettytable::Cell> = Vec::new();
    for t in titles {
        titlevec.push(prettytable::Cell::new(t).style_spec("Fb"));
    }
    table.set_titles(prettytable::Row::new(titlevec));
    table
}

fn _format_debug_value(value: &Value) -> String {
    let s: String = match value {
        Value::String(s) => s
            .to_string()
            .replace('\n', "")
            .replace('\r', "")
            .replace('\t', " "),
        _ => value.to_string(),
    };
    if s.len() > 79 {
        s[..76].to_owned() + "..."
    } else {
        s
    }
}

trait DisplayVerbose {
    fn type_as_str(self) -> String;
}

impl DisplayVerbose for Value {
    fn type_as_str(self) -> String {
        let tp = match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };
        tp.to_owned()
    }
}

fn display_obj(obj: &serde_json::map::Map<String, Value>) {
    let mut table = ctable(vec!["name", "value"]);
    for k in obj {
        let value = _format_debug_value(k.1);
        table.add_row(row![&k.0, value]);
    }
    table.printstd();
}

#[allow(clippy::cast_possible_truncation)]
async fn save_dump(
    db: &mut Box<dyn YedbClientAsyncExt>,
    key: &str,
    file_name: &str,
) -> Result<usize, Error> {
    let key_data: Vec<(String, Value)> = db.key_dump(key).await?;
    let mut f = tokio::fs::File::create(file_name).await?;
    let mut keys_dumped = 0;
    f.write_all(&[
        ENGINE_VERSION,
        SerializationEngine::from_string("msgpack")?.as_u8(),
    ])
    .await?;
    for kd in key_data {
        let buf = match rmp_serde::to_vec_named(&kd) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::new(ErrorKind::DataError, e));
            }
        };
        let data_len = (buf.len() as u32).to_le_bytes();
        f.write_all(&data_len).await?;
        f.write_all(&buf).await?;
        keys_dumped += 1;
    }
    Ok(keys_dumped)
}

#[derive(Eq, PartialEq)]
enum DumpLoadMode {
    Load,
    View,
    ViewFull,
}

#[allow(clippy::unnecessary_mut_passed)]
async fn load_dump(
    db: &mut Box<dyn YedbClientAsyncExt>,
    file_name: &str,
    mode: DumpLoadMode,
) -> Result<usize, Error> {
    macro_rules! process_data_buf {
        ($c:expr, $d:expr) => {
            if !$d.is_empty() {
                $d.reverse();
                if mode == DumpLoadMode::Load {
                    let mut data: Vec<(String, Value)> = Vec::new();
                    loop {
                        let kd = match $d.pop() {
                            Some(v) => v,
                            None => break,
                        };
                        data.push(kd);
                    }
                    db.key_load(data).await?;
                } else {
                    loop {
                        let kd = match $d.pop() {
                            Some(v) => v,
                            None => break,
                        };
                        match mode {
                            DumpLoadMode::ViewFull => {
                                let data_vec: Vec<Value> = vec![Value::from(kd.0), kd.1];
                                println!("{}", &Value::from(data_vec));
                            }
                            DumpLoadMode::View => println!("{}", kd.0),
                            _ => {}
                        }
                    }
                }
            }
        };
    }

    let mut f = tokio::fs::File::open(file_name).await?;
    let mut keys_loaded = 0;
    let mut buf = vec![0_u8; 2];
    f.read_exact(&mut buf).await?;
    if buf[0] != ENGINE_VERSION {
        return Err(Error::new(
            ErrorKind::UnsupportedVersion,
            format!("Unsupported engine version: {}", buf[0]),
        ));
    }
    if buf[1] != SerializationEngine::from_string("msgpack")?.as_u8() {
        return Err(Error::new(
            ErrorKind::UnsupportedFormat,
            "Unsupported dump format".to_owned(),
        ));
    }
    let mut data_buf: Vec<(String, Value)> = Vec::new();
    loop {
        let mut buf = vec![0_u8; 4];
        match f.read_exact(&mut buf).await {
            Ok(_) => {
                let data_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
                let mut buf = vec![0_u8; data_len as usize];
                f.read_exact(&mut buf).await?;
                data_buf.push(match rmp_serde::from_slice(&buf) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(Error::new(ErrorKind::DataError, e));
                    }
                });
                keys_loaded += 1;
                if std::mem::size_of_val(&data_buf) > DUMP_BUF_SIZE {
                    process_data_buf!(db, &mut data_buf);
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::new(ErrorKind::IOError, e)),
        }
    }
    process_data_buf!(db, &mut data_buf);
    Ok(keys_loaded)
}

fn format_time(obj: &mut serde_json::map::Map<String, Value>, fields: Vec<&str>) {
    for f in fields {
        if let Some(ts_ns) = obj.get(f).and_then(Value::as_u64) {
            let d: SystemTime = SystemTime::UNIX_EPOCH + Duration::from_nanos(ts_ns);
            let dt: DateTime<Local> = DateTime::from(d);
            obj.insert(f.to_owned(), Value::from(dt.to_rfc3339()));
        };
    }
}

fn convert_bool(value: Option<bool>, mode: ConvertBools) -> String {
    match mode {
        ConvertBools::No => value.map_or("null".to_owned(), |v| v.to_string()),
        ConvertBools::OneZero => value.map_or("0".to_owned(), |v| {
            if v {
                "1".to_owned()
            } else {
                "0".to_owned()
            }
        }),
        ConvertBools::One => value.map_or(<_>::default(), |v| {
            if v {
                "1".to_owned()
            } else {
                <_>::default()
            }
        }),
    }
}

#[allow(clippy::case_sensitive_file_extension_comparisons)]
async fn create_client(
    path: &str,
    id: u32,
) -> Result<Box<dyn YedbClientAsyncExt + Send + 'static>, Error> {
    if let Some(busrt_path) = path.strip_prefix("rt://") {
        let mut sp = busrt_path.rsplitn(2, ':');
        let target = sp.next().unwrap();
        let path = sp.next().expect("no busrt target specified");
        let me = format!("yedb-cli-{}-{}", std::process::id(), id);
        let client = busrt::ipc::Client::connect(&busrt::ipc::Config::new(path, &me))
            .await
            .unwrap();
        let rpc = busrt::rpc::RpcClient::new(client, busrt::rpc::DummyHandlers {});
        Ok(Box::new(YedbClientBusRtAsync::new(
            Arc::new(rpc),
            target,
            busrt::QoS::RealtimeProcessed,
        )))
    } else if path.starts_with("tcp://")
        || path.ends_with(".sock")
        || path.ends_with(".socket")
        || path.ends_with(".ipc")
    {
        Ok(Box::new(YedbClientAsync::new(path)))
    } else {
        Ok(Box::new(YedbClientLocalAsync::open(
            path,
            Duration::from_secs(5),
        )?))
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::main(worker_threads = 1)]
async fn main() {
    let opts: Opts = Opts::parse();
    let mut db: Box<dyn YedbClientAsyncExt> = create_client(&opts.path, 0).await.unwrap();
    let exit_code = match opts.cmd {
        Cmd::Version => {
            println!("{} : {}", "yedb-rs".blue().bold(), VERSION.yellow());
            println!(
                "{}  : {}",
                "engine".blue().bold(),
                ENGINE_VERSION.to_string().yellow()
            );
            0
        }
        Cmd::Test => output_result_ok(db.test().await),
        Cmd::Info => match db.info().await {
            Ok(db_info) => {
                let mut r = serde_json::to_value(db_info).unwrap();
                let o = r.as_object_mut().unwrap();
                format_time(o, vec!["created"]);
                display_obj(o);
                0
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Benchmark(c) => {
            benchmark(&mut db, &opts.path, c.workers, c.iterations)
                .await
                .unwrap();
            0
        }
        Cmd::Server(c) => output_result_ok(server_set_prop(&mut db, &c.prop, c.value).await),
        Cmd::Get(c) => {
            if c.recursive {
                match db.key_get_recursive(&c.key).await {
                    Ok(v) => {
                        let mut table = ctable(vec!["key", "type", "value"]);
                        for key in v {
                            let value = _format_debug_value(&key.1);
                            table.add_row(row![&key.0, &key.1.type_as_str(), value]);
                        }
                        table.printstd();
                        0
                    }
                    Err(e) => {
                        output_error(e);
                        1
                    }
                }
            } else {
                output_result_bool(
                    match c.key.find(':') {
                        Some(pos) => db.key_get_field(&c.key[..pos], &c.key[pos + 1..]).await,
                        None => db.key_get(&c.key).await,
                    },
                    c.convert_bool,
                )
            }
        }
        Cmd::GetField(c) => {
            output_result_bool(db.key_get_field(&c.key, &c.field).await, c.convert_bool)
        }
        Cmd::Source(c) => {
            let result = match c.key.find(':') {
                Some(pos) => db.key_get_field(&c.key[..pos], &c.key[pos + 1..]).await,
                None => db.key_get(&c.key).await,
            };
            match result {
                Ok(v) => {
                    let pfx = c.prefix.map_or(<_>::default(), |v| v + "_");
                    if let Value::Object(o) = v {
                        for (name, value) in o {
                            println!(
                                "{}{}={}",
                                &pfx,
                                name.replace('-', "_").replace('.', "_").to_uppercase(),
                                match value {
                                    Value::Array(a) => {
                                        let mut result = String::new();
                                        for val in a {
                                            let mut vv = val.to_string();
                                            if vv.starts_with('"') && vv.ends_with('"') {
                                                vv = vv[1..vv.len() - 1].to_owned();
                                            }
                                            if !result.is_empty() {
                                                result += " ";
                                            }
                                            result += &vv;
                                        }
                                        format!("\"{}\"", result)
                                    }
                                    Value::Bool(b) => convert_bool(Some(b), c.convert_bool),
                                    Value::Null => convert_bool(None, c.convert_bool),
                                    _ => value.to_string(),
                                }
                            );
                        }
                    }
                    0
                }
                Err(e) => {
                    output_error(e);
                    1
                }
            }
        }
        Cmd::Ls(c) => {
            let result = if c.all {
                db.key_list_all(&c.key).await
            } else {
                db.key_list(&c.key).await
            };
            match result {
                Ok(v) => {
                    let mut table = ctable(vec!["keys"]);
                    for key in v {
                        table.add_row(row![&key]);
                    }
                    table.printstd();
                    0
                }
                Err(e) => {
                    output_error(e);
                    1
                }
            }
        }
        Cmd::Delete(c) => match c.key.find(':') {
            Some(pos) => {
                output_result_ok(db.key_delete_field(&c.key[..pos], &c.key[pos + 1..]).await)
            }
            None => {
                if c.recursive {
                    output_result_ok(db.key_delete_recursive(&c.key).await)
                } else {
                    output_result_ok(db.key_delete(&c.key).await)
                }
            }
        },
        Cmd::Incr(c) => output_result(db.key_increment(&c.key).await),
        Cmd::Decr(c) => output_result(db.key_decrement(&c.key).await),
        Cmd::Explain(c) => match db.key_explain(&c.key).await {
            Ok(key_info) => {
                let mut r = serde_json::to_value(key_info).unwrap();
                let o = r.as_object_mut().unwrap();
                format_time(o, vec!["mtime", "stime"]);
                display_obj(o);
                0
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Edit(c) => match db.key_get(&c.key).await {
            Ok(v) => {
                if let Some(v) = c.default {
                    if v == "-" {
                        let mut buffer = String::new();
                        io::stdin().read_to_string(&mut buffer).unwrap();
                    }
                }
                edit_key(&mut db, &c.key, Some(&v)).await
            }
            Err(ref e) if e.kind() == ErrorKind::KeyNotFound => {
                let value: Option<Value> = match c.default {
                    Some(v) if v == "-" => {
                        let mut buffer = String::new();
                        io::stdin().read_to_string(&mut buffer).unwrap();
                        Some(serde_yaml::from_str(&buffer).unwrap())
                    }
                    Some(fname) => {
                        Some(serde_yaml::from_str(&fs::read_to_string(fname).unwrap()).unwrap())
                    }
                    None => None,
                };
                match value {
                    Some(v) => edit_key(&mut db, &c.key, Some(&v)).await,
                    None => edit_key(&mut db, &c.key, None).await,
                }
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Set(c) => match format_value(c.value, c.r#type).await {
            Ok(v) => output_result_ok(match c.key.find(':') {
                Some(pos) => db.key_set_field(&c.key[..pos], &c.key[pos + 1..], v).await,
                None => db.key_set(&c.key, v).await,
            }),
            Err(e) => {
                output_error(e);
                2
            }
        },
        Cmd::SetField(c) => match format_value(c.value, c.r#type).await {
            Ok(v) => output_result_ok(db.key_set_field(&c.key, &c.field, v).await),
            Err(e) => {
                output_error(e);
                2
            }
        },
        Cmd::DeleteField(c) => output_result_ok(db.key_delete_field(&c.key, &c.field).await),
        Cmd::r#Copy(c) => output_result_ok(db.key_copy(&c.key, &c.dst_key).await),
        Cmd::Rename(c) => output_result_ok(db.key_rename(&c.key, &c.dst_key).await),
        Cmd::Check => match db.check().await {
            Ok(keys) => {
                for key in &keys {
                    println!("{}", format!("Key is broken: {}", key).red());
                }
                if keys.is_empty() {
                    print_ok();
                    0
                } else {
                    println!();
                    println!("Run \"repair\" command to clean up and fix the database");
                    5
                }
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Repair => match db.repair().await {
            Ok(keys) => {
                for key_data in keys {
                    if key_data.1 {
                        println!("{}", format!("Key restored: {}", key_data.0).green().bold());
                    } else {
                        println!("{}", format!("Key removed: {}", key_data.0).red());
                    }
                }
                0
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Purge => match db.purge().await {
            Ok(keys) => {
                for key in &keys {
                    println!("{}", format!("Broken key REMOVED: {}", key).yellow().bold());
                }
                0
            }
            Err(e) => {
                output_error(e);
                1
            }
        },
        Cmd::Dump(cmd) => match cmd {
            DumpCommands::Save(c) => match save_dump(&mut db, &c.key, &c.file).await {
                Ok(n) => {
                    println!("{} subkey(s) of {} dumped", n, &c.key);
                    0
                }
                Err(e) => {
                    output_error(e);
                    2
                }
            },
            DumpCommands::Load(c) => match load_dump(&mut db, &c.file, DumpLoadMode::Load).await {
                Ok(n) => {
                    println!("{} key(s) loaded", n);
                    0
                }
                Err(e) => {
                    output_error(e);
                    2
                }
            },
            DumpCommands::View(c) => match load_dump(
                &mut db,
                &c.file,
                if c.full {
                    DumpLoadMode::ViewFull
                } else {
                    DumpLoadMode::View
                },
            )
            .await
            {
                Ok(_) => 0,
                Err(e) => {
                    output_error(e);
                    2
                }
            },
        },
    };
    std::process::exit(exit_code);
}
