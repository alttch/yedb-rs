use lazy_static::lazy_static;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;

use std::fmt;
use std::vec::Vec;

use yedb::common::JSONRpcRequest;
use yedb::{Database, Error, ErrorKind};

use serde_json::Value;

use log::LevelFilter;
use syslog::{BasicLogger, Facility, Formatter3164};

use chrono::prelude::*;
use colored::Colorize;

use clap::Clap;

use log::{debug, error, info, Level, Metadata, Record};

lazy_static! {
    pub static ref DBCELL: RwLock<Database> = RwLock::new(yedb::Database::new());
}

struct SimpleLogger;

#[derive(Debug, Eq, PartialEq)]
pub enum YedbServerErrorKind {
    Critical,
    #[allow(dead_code)]
    Other,
}

#[macro_export]
macro_rules! parse_jsonrpc_request_param {
    ($r:expr, $k:expr, $p:path) => {
        if let Some($p(v)) = $r.params.get($k) {
            Some(v)
        } else {
            None
        }
    };
}

#[macro_export]
macro_rules! encode_jsonrpc_response {
    ($v:expr) => {
        match rmp_serde::to_vec_named(&$v) {
            Ok(v) => v,
            Err(e) => {
                error!("Response encode error {}", e);
                return Err(YedbServerErrorKind::Critical);
            }
        }
    };
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let s = format!(
                "{}  {}",
                Local::now().to_rfc3339_opts(SecondsFormat::Secs, false),
                record.args()
            );
            println!(
                "{}",
                match record.level() {
                    Level::Debug => s.dimmed(),
                    Level::Warn => s.yellow().bold(),
                    Level::Error => s.red(),
                    _ => s.normal(),
                }
            );
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

pub struct ServerData {
    pub pid_path: String,
    pub socket_path: Option<String>,
}

lazy_static! {
    pub static ref SDATA: RwLock<ServerData> = RwLock::new(ServerData {
        pid_path: String::new(),
        socket_path: None
    });
}

macro_rules! handle_term {
    ($s:expr) => {
        loop {
            $s.recv().await;
            info!("terminating");
            let mut dbobj = DBCELL.write().await;
            if dbobj.is_open() {
                dbobj.close().unwrap();
            }
            let s = SDATA.read().await;
            let _r = std::fs::remove_file(&s.pid_path);
            match s.socket_path {
                Some(ref f) => {
                    let _r = std::fs::remove_file(f);
                }
                None => {}
            };
            std::process::exit(0);
        }
    };
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clap)]
struct Opts {
    #[clap(about = "database directory")]
    path: String,
    #[clap(short = 'B', long = "bind", default_value = "tcp://127.0.0.1:8870")]
    bind: String,
    #[clap(long, default_value = "/tmp/yedb-server.pid")]
    pid_file: String,
    #[clap(long)]
    lock_path: Option<String>,
    #[clap(long, default_value = "json")]
    default_fmt: SerializationFormat,
    #[clap(short = 'v', about = "Verbose logging")]
    verbose: bool,
    #[clap(long)]
    disable_auto_flush: bool,
    #[clap(long)]
    disable_auto_repair: bool,
    #[clap(long)]
    strict_schema: bool,
    #[clap(long, default_value = "1000")]
    cache_size: usize,
    #[clap(long, default_value = "0")]
    auto_bak: u64,
    #[clap(long, default_value = "2")]
    workers: usize,
}

enum SerializationFormat {
    Json,
    Yaml,
    Msgpack,
    Cbor,
}

impl std::str::FromStr for SerializationFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(SerializationFormat::Json),
            "yaml" => Ok(SerializationFormat::Yaml),
            "msgpack" => Ok(SerializationFormat::Msgpack),
            "cbor" => Ok(SerializationFormat::Cbor),
            _ => Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!("{}, valid values: json|yaml|msgpack|cbor", s),
            )),
        }
    }
}

impl fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SerializationFormat::Json => "json".to_owned(),
                SerializationFormat::Msgpack => "msgpack".to_owned(),
                SerializationFormat::Cbor => "cbor".to_owned(),
                SerializationFormat::Yaml => "yaml".to_owned(),
            }
        )
    }
}

fn set_verbose_logger(filter: LevelFilter) {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(filter))
        .unwrap();
}

fn main() {
    let opts: Opts = Opts::parse();
    if opts.verbose {
        set_verbose_logger(LevelFilter::Debug);
    } else if std::env::var("YEDB_DISABLE_SYSLOG").unwrap_or_else(|_| "0".to_owned()) == "1" {
        set_verbose_logger(LevelFilter::Info);
    } else {
        let formatter = Formatter3164 {
            facility: Facility::LOG_USER,
            hostname: None,
            process: "yedb-server".into(),
            pid: 0,
        };
        match syslog::unix(formatter) {
            Ok(logger) => {
                log::set_boxed_logger(Box::new(BasicLogger::new(logger)))
                    .map(|()| log::set_max_level(LevelFilter::Info))
                    .unwrap();
            }
            Err(_) => {
                set_verbose_logger(LevelFilter::Info);
            }
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(opts.workers)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut dbobj = DBCELL.write().await;
        dbobj.set_db_path(&opts.path).unwrap();
        if let Some(path) = opts.lock_path {
            dbobj.set_lock_path(&path).unwrap();
        }
        dbobj.auto_flush = !opts.disable_auto_flush;
        dbobj.auto_repair = !opts.disable_auto_repair;
        dbobj.strict_schema = opts.strict_schema;
        dbobj
            .set_default_fmt(&opts.default_fmt.to_string(), true)
            .unwrap();
        dbobj.set_cache_size(opts.cache_size);
        debug!("Auto bak: {}", opts.auto_bak);
        dbobj.auto_bak = opts.auto_bak as u64;
        debug!("Workers: {}", opts.workers);
        drop(dbobj);
        run_server(&opts.bind, &opts.pid_file).await;
    });
}

async fn run_server(bind_to: &str, pidfile: &str) {
    let mut dbobj = DBCELL.write().await;
    let _r = fs::remove_file(&bind_to).await;
    let listener = if bind_to.starts_with("tcp://") {
        Listener::Tcp(
            TcpListener::bind(bind_to.strip_prefix("tcp://").unwrap())
                .await
                .unwrap(),
        )
    } else {
        let _r = fs::remove_file(&bind_to).await;
        SDATA.write().await.socket_path = Some(bind_to.to_owned());
        Listener::Unix(UnixListener::bind(&bind_to).unwrap())
    };
    let server_info = dbobj.open().unwrap();
    debug!("Engine version: {}", server_info.version);
    let dbinfo = dbobj.info().unwrap();
    debug!("Library: {}, version {}", dbinfo.server.0, dbinfo.server.1);
    debug!("Database: {}, format: {}", dbinfo.path, dbinfo.fmt);
    tokio::spawn(async move { handle_term!(signal(SignalKind::interrupt()).unwrap()) });
    tokio::spawn(async move { handle_term!(signal(SignalKind::terminate()).unwrap()) });
    {
        let mut f = fs::File::create(&pidfile).await.unwrap();
        f.write_all(std::process::id().to_string().as_bytes())
            .await
            .unwrap();
        SDATA.write().await.pid_path = pidfile.to_owned();
    }
    drop(dbobj);
    info!("Started, listening at {}", bind_to);
    loop {
        match listener {
            Listener::Unix(ref socket) => match socket.accept().await {
                Ok((mut stream, _addr)) => {
                    tokio::spawn(async move {
                        unix_worker(&mut stream).await;
                    });
                }
                Err(e) => {
                    error!("API connect error {}", e);
                }
            },
            Listener::Tcp(ref socket) => match socket.accept().await {
                Ok((mut stream, _addr)) => {
                    stream.set_nodelay(true).unwrap();
                    tokio::spawn(async move {
                        tcp_worker(&mut stream).await;
                    });
                }
                Err(e) => {
                    error!("API connect error {}", e);
                }
            },
        };
    }
}

macro_rules! parse_request_meta {
    ($s:expr, $b:expr, $l:expr) => {
        match $s.read_exact(&mut $b).await {
            Ok(_) => $l = u32::from_le_bytes([$b[2], $b[3], $b[4], $b[5]]) as usize,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                debug!("API read error {}", e);
                break;
            }
        };
        if $b[0] != yedb::ENGINE_VERSION || $b[1] != 2 || $l == 0 {
            debug!("Invalid packet");
            break;
        };
    };
}

macro_rules! handle_request {
    ($s:expr, $b:expr) => {
        match $s.read_exact(&mut $b).await {
            Ok(_) => match process_request(&$b).await {
                Ok(response_buf) => {
                    let mut response_frame = vec![yedb::ENGINE_VERSION, 2_u8];
                    response_frame.extend(&(response_buf.len() as u32).to_le_bytes());
                    response_frame.extend(&response_buf);
                    match $s.write_all(&response_frame).await {
                        Ok(_) => {}
                        Err(e) => {
                            debug!("API write error {}", e);
                            break;
                        }
                    };
                }
                Err(e) if e == YedbServerErrorKind::Critical => break,
                Err(_) => continue,
            },
            Err(e) => {
                error!("Socket error {}", e);
                break;
            }
        }
    };
}

async fn unix_worker(stream: &mut UnixStream) {
    loop {
        let mut buf = [0_u8; 6];
        let frame_len: usize;
        parse_request_meta!(stream, buf, frame_len);
        let mut buf: Vec<u8> = vec![0; frame_len];

        handle_request!(stream, buf);
    }
}

async fn tcp_worker(stream: &mut TcpStream) {
    loop {
        let mut buf = [0_u8; 6];
        let frame_len: usize;
        parse_request_meta!(stream, buf, frame_len);
        let mut buf: Vec<u8> = vec![0; frame_len];

        handle_request!(stream, buf);
    }
}

#[allow(clippy::too_many_lines)]
async fn process_request(buf: &[u8]) -> Result<Vec<u8>, YedbServerErrorKind> {
    let request: JSONRpcRequest = match rmp_serde::from_read_ref(&buf) {
        Ok(v) => v,
        Err(e) => {
            error!("API decode error {}", e);
            return Err(YedbServerErrorKind::Critical);
        }
    };
    if !request.is_valid() {
        error!("API error: invalid request");
        return Err(YedbServerErrorKind::Critical);
    }
    macro_rules! invalid_param {
        () => {
            encode_jsonrpc_response!(request.error(Error::err_invalid_parameter()))
        };
    }
    macro_rules! run_request {
        ($params: expr, $then: block) => {
            if request.params_valid($params)
                $then
            else {
                invalid_param!()
            }
        }
    }
    macro_rules! respond {
        ($result: expr) => {
            match $result {
                Ok(v) => encode_jsonrpc_response!(request.respond(v)),
                Err(e) => encode_jsonrpc_response!(request.error(e)),
            }
        };
    }
    Ok(match request.method.as_str() {
        "test" => {
            debug!("API request: test");
            run_request!(vec![], { encode_jsonrpc_response!(request.respond_ok()) })
        }
        "info" => {
            debug!("API request: info");
            run_request!(vec![], { respond!(DBCELL.write().await.info()) })
        }
        "server_set" => run_request!(vec!["name", "value"], {
            match parse_jsonrpc_request_param!(request, "name", Value::String) {
                Some(name) => {
                    let value = request.params.get("value").unwrap();
                    debug!("API request: server_set {}={}", name, value);
                    respond!(DBCELL.write().await.server_set(name, value.clone()))
                }
                None => invalid_param!(),
            }
        }),
        "key_get" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    debug!("API request: key_get {}", v);
                    respond!(DBCELL.write().await.key_get(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_get_field" => run_request!(vec!["key", "field"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            if key.is_some() && field.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                debug!("API request: key_get_field {}:{}", k, f);
                respond!(DBCELL.write().await.key_get_field(k, f))
            } else {
                invalid_param!()
            }
        }),
        "key_get_recursive" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    debug!("API request: key_get_recursive {}", v);
                    respond!(DBCELL.write().await.key_get_recursive(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_explain" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    debug!("API request: key_explain {}", v);
                    respond!(DBCELL.write().await.key_explain(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_list" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    debug!("API request: key_list {}", v);
                    respond!(DBCELL.write().await.key_list(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_list_all" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    debug!("API request: key_list_all {}", v);
                    respond!(DBCELL.write().await.key_list_all(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_set" => run_request!(vec!["key", "value"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let value = request.params.get("value").cloned();
            if key.is_some() && value.is_some() {
                let k = key.unwrap();
                debug!("API request: key_set {}", k);
                respond!(DBCELL.write().await.key_set(k, value.unwrap()))
            } else {
                invalid_param!()
            }
        }),
        "key_set_field" => run_request!(vec!["key", "field", "value"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            let value = request.params.get("value").cloned();
            if key.is_some() && field.is_some() && value.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                debug!("API request: key_set_field {}:{}", k, f);
                respond!(DBCELL.write().await.key_set_field(k, f, value.unwrap()))
            } else {
                invalid_param!()
            }
        }),
        "key_delete_field" => run_request!(vec!["key", "field"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            if key.is_some() && field.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                debug!("API request: key_delete_field {}:{}", k, f);
                respond!(DBCELL.write().await.key_delete_field(k, f))
            } else {
                invalid_param!()
            }
        }),
        "key_increment" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                debug!("API request: key_get {}", v);
                respond!(DBCELL.write().await.key_increment(v))
            } else {
                invalid_param!()
            }
        }),
        "key_decrement" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                debug!("API request: key_get {}", v);
                respond!(DBCELL.write().await.key_decrement(v))
            } else {
                invalid_param!()
            }
        }),
        "key_copy" => run_request!(vec!["key", "dst_key"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let dst_key = parse_jsonrpc_request_param!(request, "dst_key", Value::String);
            if key.is_some() && dst_key.is_some() {
                let k = key.unwrap();
                let dk = dst_key.unwrap();
                debug!("API request: key_copy {} -> {}", k, dk);
                respond!(DBCELL.write().await.key_copy(k, dk))
            } else {
                invalid_param!()
            }
        }),
        "key_rename" => run_request!(vec!["key", "dst_key"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let dst_key = parse_jsonrpc_request_param!(request, "dst_key", Value::String);
            if key.is_some() && dst_key.is_some() {
                let k = key.unwrap();
                let dk = dst_key.unwrap();
                debug!("API request: key_rename {} -> {}", k, dk);
                respond!(DBCELL.write().await.key_rename(k, dk))
            } else {
                invalid_param!()
            }
        }),
        "key_delete" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                debug!("API request: key_delete {}", v);
                respond!(DBCELL.write().await.key_delete(v))
            } else {
                invalid_param!()
            }
        }),
        "key_delete_recursive" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                debug!("API request: key_delete_recursive {}", v);
                respond!(DBCELL.write().await.key_delete_recursive(v))
            } else {
                invalid_param!()
            }
        }),
        "check" => {
            debug!("API request: check");
            run_request!(vec![], { respond!(DBCELL.write().await.check()) })
        }
        "repair" => {
            debug!("API request: repair");
            run_request!(vec![], { respond!(DBCELL.write().await.repair()) })
        }
        "purge" => {
            debug!("API request: purge");
            run_request!(vec![], { respond!(DBCELL.write().await.purge()) })
        }
        "purge_cache" => {
            debug!("API request: purge_cache");
            run_request!(vec![], { respond!(DBCELL.write().await.purge_cache()) })
        }
        "safe_purge" => {
            debug!("API request: safe_purge");
            run_request!(vec![], { respond!(DBCELL.write().await.safe_purge()) })
        }
        "key_dump" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                debug!("API request: key_dump {}", v);
                respond!(DBCELL.write().await.key_dump(v))
            } else {
                invalid_param!()
            }
        }),
        "key_load" => run_request!(vec!["data"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "data", Value::Array) {
                debug!("API request: key_load");
                respond!(DBCELL.write().await.key_load_from_serialized(v))
            } else {
                invalid_param!()
            }
        }),
        _ => encode_jsonrpc_response!(request.error(Error::err_method_not_found())),
    })
}
