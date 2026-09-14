use color_eyre::eyre::{Context, Result};
use std::path::Path;
#[cfg(target_os = "windows")]
use std::path::PathBuf;

#[cfg(not(target_os = "windows"))]
use swss_common::{link_to_swsscommon_logger, LoggerConfigChangeHandler};

use lazy_static::lazy_static;
use std::io::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    self, filter, fmt, fmt::format::FmtSpan, prelude::__tracing_subscriber_SubscriberExt, reload,
    util::SubscriberInitExt, Layer, Registry,
};

/// Configuration for logging specific module targets to an independent file.
pub struct FileLogConfig {
    /// Full path to the log file. Rotated files will be named `{path}.1`, `{path}.2`, etc.
    pub log_file_path: String,
    /// Maximum size in bytes before the log file is rotated.
    pub max_file_size_bytes: usize,
    /// Number of rotated files to keep (e.g. 5 means file, file.1, ..., file.5).
    pub max_file_count: usize,
    /// Module target prefixes to route to the file (e.g. `["hamgrd::actors", "swbus_core"]`).
    /// Log records whose target starts with any of these prefixes are written to the file.
    pub targets: Vec<String>,
}

lazy_static! {
    static ref LOG_FOR_TEST_INIT: Mutex<bool> = Mutex::new(false);
}

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "debug";

#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "info";

struct LoggerConfigHandler {
    level_reload_handle: reload::Handle<filter::LevelFilter, Registry>,
}

impl LoggerConfigChangeHandler for LoggerConfigHandler {
    fn on_log_level_change(&mut self, level: &str) {
        let level = match level {
            "ALERT" => filter::LevelFilter::WARN,
            "NOTICE" => filter::LevelFilter::INFO,
            "INFO" => filter::LevelFilter::INFO,
            "DEBUG" => filter::LevelFilter::DEBUG,
            _ => filter::LevelFilter::ERROR,
        };

        self.level_reload_handle.modify(|f| *f = level).unwrap();
    }

    fn on_log_output_change(&mut self, output: &str) {
        // Rust doesn't support dynamically changing log output. We will only support default output (syslog in linux, file in windows)
        if output != "SYSLOG" {
            info!(
                "Log output change to unsupported destination {}. Setting ignored",
                output
            );
        }
    }
}

/// log initialization
/// There are multiple options to initialize log:
/// * Set RUST_LOG or {program_name}_LOG_LEVEL env var to the desired log level. If the log env is not set and link_swsscommon_logger is false,
///   use DEFAULT_LOG_LEVEL. Otherwise, use swsscommon logger described next
/// * If link_swsscommon_logger is set, use swsscommon logger to get log settings from config_db, which supports dynamic log level change
///   by modifying config_db. The settings include
///   - log_level: emerg, alert, crit, error, warn, notice, info, debug
///   - output: stdout, stderr, syslog. Rust doesn't support dynamically changing log output. We will only support default output (syslog in linux, file in windows)
/// * If file_log is provided, an additional file logging layer is created that writes records matching the specified
///   module targets to a size-based, rotating log file (see FileLogConfig for size and count limits). This is independent
///   of the main syslog/file output.
pub fn init(program_name: &'static str, link_swsscommon_logger: bool, file_log: Option<FileLogConfig>) -> Result<()> {
    let log_level_env_var = format!("{}_LOG_LEVEL", program_name.to_uppercase());

    let mut log_env_set = true;
    // RUST_LOG env has the highest priority. If it is not set, get logger setting from config_db
    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG")
            .or_else(|_| std::env::var(log_level_env_var))
            .unwrap_or_else(|_| {
                log_env_set = false;
                DEFAULT_LOG_LEVEL.to_string()
            }),
    );

    let file_subscriber = new_file_subscriber(program_name).wrap_err("Unable to create file subscriber.")?;
    let file_log_layer = new_independent_file_layer(file_log)?;

    #[cfg(not(target_os = "windows"))]
    if link_swsscommon_logger && !log_env_set {
        let filter = filter::LevelFilter::INFO;
        let (level_layer, level_reload_handle) = reload::Layer::new(filter);

        let handler = LoggerConfigHandler { level_reload_handle };
        // connect to swsscommon logger
        let result = link_to_swsscommon_logger(program_name, handler);
        if result.is_ok() {
            println!("Link to swsscommon logger successfully.");
            let file_subscriber = file_subscriber
                .with_line_number(true)
                .with_target(false)
                .with_ansi(false);

            let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();
            layers.push(Box::new(level_layer.and_then(file_subscriber)));
            if let Some(fl) = file_log_layer {
                layers.push(fl);
            }
            layers.push(Box::new(ErrorLayer::default()));

            tracing_subscriber::registry().with(layers).init();
            return Ok(());
        } else {
            eprintln!("Unable to link to swsscommon logger: {}", result.unwrap_err());
            // fall back to EnvFilter
        }
    }

    let filter = tracing_subscriber::filter::EnvFilter::from_default_env();
    let file_subscriber = file_subscriber
        .with_line_number(true)
        .with_target(false)
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();
    layers.push(Box::new(filter.and_then(file_subscriber)));
    if let Some(fl) = file_log_layer {
        layers.push(fl);
    }
    layers.push(Box::new(ErrorLayer::default()));

    tracing_subscriber::registry().with(layers).init();

    Ok(())
}

/// Wraps the rotating writer so a rotation panic can't unwind through the shared `tracing`
/// writer `Mutex` (which would poison it and crash-loop logging). The panic can leave the
/// writer's rotation state out of sync with disk, so `rebuild` re-creates it to re-sync.
struct PanicSafeWriter<W: Write, F: FnMut() -> W> {
    writer: W,
    rebuild: F,
}

impl<W: Write, F: FnMut() -> W> PanicSafeWriter<W, F> {
    fn new(mut rebuild: F) -> Self {
        let writer = rebuild();
        Self { writer, rebuild }
    }
}

impl<W: Write, F: FnMut() -> W> Write for PanicSafeWriter<W, F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match catch_unwind(AssertUnwindSafe(|| self.writer.write(buf))) {
            Ok(res) => res,
            Err(_) => {
                self.writer = (self.rebuild)();
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match catch_unwind(AssertUnwindSafe(|| self.writer.flush())) {
            Ok(res) => res,
            Err(_) => {
                self.writer = (self.rebuild)();
                Ok(())
            }
        }
    }
}

/// Creates an optional independent file logging layer.
/// When `file_log` is `Some`, returns a boxed layer that writes log records whose target starts
/// with any of the configured prefixes to a size-rotated file (like syslog: file, file.1, file.2, etc.).
fn new_independent_file_layer(
    file_log: Option<FileLogConfig>,
) -> Result<Option<Box<dyn Layer<Registry> + Send + Sync>>> {
    use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};

    let config = match file_log {
        Some(c) => c,
        None => return Ok(None),
    };

    let log_path = Path::new(&config.log_file_path);
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent).wrap_err(format!("Unable to create log directory: {:?}", parent))?;
    }

    // Factory used by PanicSafeWriter to rebuild (and re-scan/re-sync) the writer after a panic.
    let log_file_path = config.log_file_path.clone();
    let max_file_count = config.max_file_count;
    let max_file_size_bytes = config.max_file_size_bytes;
    let build_file_rotate = move || {
        FileRotate::new(
            Path::new(&log_file_path),
            AppendCount::new(max_file_count),
            ContentLimit::Bytes(max_file_size_bytes),
            Compression::OnRotate(2),
            #[cfg(unix)]
            None,
        )
    };
    let file_rotate = Mutex::new(PanicSafeWriter::new(build_file_rotate));

    let targets = config.targets;
    let mut target_filter = filter::Targets::new();
    for prefix in &targets {
        target_filter = target_filter.with_target(prefix.clone(), filter::LevelFilter::TRACE);
    }

    let layer = fmt::layer()
        .with_writer(file_rotate)
        .with_line_number(true)
        .with_target(true)
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_filter(target_filter);

    Ok(Some(Box::new(layer)))
}

#[cfg(target_os = "windows")]
fn new_file_subscriber(
    program_name: &str,
) -> Result<
    tracing_subscriber::fmt::Layer<
        tracing_subscriber::Registry,
        tracing_subscriber::fmt::format::DefaultFields,
        tracing_subscriber::fmt::format::Format,
        std::fs::File,
    >,
> {
    let log_folder = PathBuf::from(std::env::var("LOCALAPPDATA").unwrap());
    let log_folder = log_folder.join("sonic").join("log");
    std::fs::create_dir_all(log_folder.clone()).wrap_err(format!("Unable to create log folder: {:?}", log_folder))?;

    let log_path = log_folder.join(program_name);
    let log_file =
        std::fs::File::create(log_path.clone()).wrap_err(format!("Unable to create log file: {:?}", log_path))?;
    Ok(tracing_subscriber::fmt::layer().with_writer(log_file))
}

#[cfg(not(target_os = "windows"))]
fn new_file_subscriber(
    program_name: &str,
) -> Result<
    tracing_subscriber::fmt::Layer<
        tracing_subscriber::Registry,
        tracing_subscriber::fmt::format::DefaultFields,
        tracing_subscriber::fmt::format::Format,
        syslog_tracing::Syslog,
    >,
> {
    use color_eyre::eyre::ContextCompat;

    let identity =
        std::ffi::CString::new(program_name).wrap_err(format!("Unable to create syslog identity: {program_name}"))?;
    let (options, facility) = Default::default();
    let syslog =
        syslog_tracing::Syslog::new(identity, options, facility).wrap_err("Unable to create syslog writer.")?;
    Ok(tracing_subscriber::fmt::layer().with_writer(syslog))
}

/// Init logger for tests only, which uses stdout/stderr as output, if ENABLE_TRACE is set to 1 or true
pub fn init_logger_for_test() {
    let trace_enabled: bool = std::env::var("ENABLE_TRACE")
        .map(|val| val == "1" || val.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if !trace_enabled {
        return;
    }

    let mut log_init_guard = LOG_FOR_TEST_INIT
        .lock()
        .unwrap_or_else(|err| panic!("Failed to lock: {err}"));
    if *log_init_guard {
        return;
    }
    *log_init_guard = true;

    let stdout_level = tracing::level_filters::LevelFilter::DEBUG;
    // Create a stdout logger for `info!` and lower severity levels
    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_filter(stdout_level);

    // Create a stderr logger for `error!` and higher severity levels
    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_filter(tracing::level_filters::LevelFilter::ERROR);

    // Combine the layers and set them as the global subscriber
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(stderr_layer)
        .init();
}
#[cfg(test)]
mod test {
    use std::io::{self, Write};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    struct TestWriter {
        panic_on_write: bool,
        panic_on_flush: bool,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        flush_count: Arc<AtomicUsize>,
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.panic_on_write {
                panic!("write failed");
            }
            self.writes.lock().unwrap().push(buf.to_vec());
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.panic_on_flush {
                panic!("flush failed");
            }
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn log_can_be_initialized() {
        let result = super::init("test", true, None);
        assert!(result.is_ok());
    }

    #[test]
    fn panic_safe_writer_recovers_from_write_panic() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let build_count_for_factory = Arc::clone(&build_count);
        let writes_for_factory = Arc::clone(&writes);
        let flush_count_for_factory = Arc::clone(&flush_count);

        let writer = Mutex::new(super::PanicSafeWriter::new(move || {
            let generation = build_count_for_factory.fetch_add(1, Ordering::SeqCst);
            TestWriter {
                panic_on_write: generation == 0,
                panic_on_flush: false,
                writes: Arc::clone(&writes_for_factory),
                flush_count: Arc::clone(&flush_count_for_factory),
            }
        }));

        let dropped = b"dropped";
        assert_eq!(writer.lock().unwrap().write(dropped).unwrap(), dropped.len());
        writer.lock().unwrap().write_all(b"written").unwrap();

        assert_eq!(build_count.load(Ordering::SeqCst), 2);
        assert_eq!(*writes.lock().unwrap(), vec![b"written".to_vec()]);
    }

    #[test]
    fn panic_safe_writer_recovers_from_flush_panic() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let build_count_for_factory = Arc::clone(&build_count);
        let writes_for_factory = Arc::clone(&writes);
        let flush_count_for_factory = Arc::clone(&flush_count);

        let writer = Mutex::new(super::PanicSafeWriter::new(move || {
            let generation = build_count_for_factory.fetch_add(1, Ordering::SeqCst);
            TestWriter {
                panic_on_write: false,
                panic_on_flush: generation == 0,
                writes: Arc::clone(&writes_for_factory),
                flush_count: Arc::clone(&flush_count_for_factory),
            }
        }));

        writer.lock().unwrap().flush().unwrap();
        writer.lock().unwrap().flush().unwrap();

        assert_eq!(build_count.load(Ordering::SeqCst), 2);
        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
    }
}
