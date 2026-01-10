use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::RwLock;

use std::fmt;
use std::sync::{Arc, LazyLock};

use yedb::common::JSONRpcRequest;
use yedb::server::{YedbServerErrorKind, process_request};
use yedb::{Database, Error, ErrorKind};

use log::LevelFilter;
use syslog::{BasicLogger, Facility, Formatter3164};

use chrono::prelude::*;
use colored::Colorize;

use clap::{Parser, ValueEnum};

use log::{Level, Metadata, Record, debug, error, info};

struct SimpleLogger;

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

static SDATA: LazyLock<RwLock<ServerData>> = LazyLock::new(|| {
    RwLock::new(ServerData {
        pid_path: String::new(),
        socket_path: None,
    })
});
static DBCELL: LazyLock<Arc<RwLock<Database>>> = LazyLock::new(yedb::server::create_db);

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
#[derive(Parser)]
struct Opts {
    #[clap(help = "database directory")]
    path: String,
    #[clap(short = 'B', long = "bind", default_value = "tcp://127.0.0.1:8870")]
    bind: String,
    #[clap(long, default_value = "/tmp/yedb-server.pid")]
    pid_file: String,
    #[clap(long)]
    lock_path: Option<String>,
    #[clap(long, default_value = "json")]
    default_fmt: SerializationFormat,
    #[clap(short = 'v', help = "Verbose logging")]
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
    #[clap(long)]
    skip_bak: Option<String>,
    #[clap(long, default_value = "2")]
    workers: usize,
}

#[derive(ValueEnum, PartialEq, Clone)]
#[clap(rename_all = "lowercase")]
enum SerializationFormat {
    Json,
    Yaml,
    Msgpack,
}

impl std::str::FromStr for SerializationFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(SerializationFormat::Json),
            "yaml" => Ok(SerializationFormat::Yaml),
            "msgpack" => Ok(SerializationFormat::Msgpack),
            _ => Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!("{}, valid values: json|yaml|msgpack", s),
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
        dbobj.auto_bak = opts.auto_bak;
        if let Some(ref skip_bak) = opts.skip_bak {
            let skips = skip_bak.split(',').map(ToOwned::to_owned).collect();
            dbobj.skip_bak = skips;
            debug!("Skip bak: {}", dbobj.skip_bak.join(", "));
        }
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
        Listener::Unix(UnixListener::bind(bind_to).unwrap())
    };
    let server_info = dbobj.open().unwrap();
    debug!("Engine version: {}", server_info.version);
    let dbinfo = dbobj.info().unwrap();
    debug!("Library: {}, version {}", dbinfo.server.0, dbinfo.server.1);
    debug!("Database: {}, format: {}", dbinfo.path, dbinfo.fmt);
    if std::env::var("YEDB_DISABLE_CC").unwrap_or_else(|_| "0".to_owned()) != "1" {
        tokio::spawn(async move { handle_term!(signal(SignalKind::interrupt()).unwrap()) });
    }
    tokio::spawn(async move { handle_term!(signal(SignalKind::terminate()).unwrap()) });
    {
        let mut f = fs::File::create(&pidfile).await.unwrap();
        f.write_all(std::process::id().to_string().as_bytes())
            .await
            .unwrap();
        pidfile.clone_into(&mut SDATA.write().await.pid_path);
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
        }
    }
}

macro_rules! parse_request_meta {
    ($s:expr, $b:expr) => {{
        let frame_len = match $s.read_exact(&mut $b).await {
            Ok(_) => u32::from_le_bytes([$b[2], $b[3], $b[4], $b[5]]) as usize,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                debug!("API read error {}", e);
                break;
            }
        };
        if $b[0] != yedb::ENGINE_VERSION || $b[1] != 2 || frame_len == 0 {
            debug!("Invalid packet");
            break;
        };
        frame_len
    }};
}

macro_rules! handle_request {
    ($s:expr, $b:expr) => {
        match $s.read_exact(&mut $b).await {
            Ok(_) => {
                let request: JSONRpcRequest = match rmp_serde::from_slice(&$b) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("API decode error {}", e);
                        break;
                    }
                };
                if !request.is_valid() {
                    error!("API error: invalid request");
                    break;
                }
                match process_request(&DBCELL, request).await {
                    Ok(response) => {
                        let response_buf = match rmp_serde::to_vec_named(&response) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Response encode error {}", e);
                                break;
                            }
                        };
                        let mut response_frame = vec![yedb::ENGINE_VERSION, 2_u8];
                        let Ok(response_len) = u32::try_from(response_buf.len()) else {
                            error!("Response too large");
                            break;
                        };
                        response_frame.extend(&response_len.to_le_bytes());
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
                    Err(_) => {}
                }
            }
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
        let frame_len: usize = parse_request_meta!(stream, buf);
        let mut buf: Vec<u8> = vec![0; frame_len];

        handle_request!(stream, buf);
    }
}

async fn tcp_worker(stream: &mut TcpStream) {
    loop {
        let mut buf = [0_u8; 6];
        let frame_len: usize = parse_request_meta!(stream, buf);
        let mut buf: Vec<u8> = vec![0; frame_len];

        handle_request!(stream, buf);
    }
}
