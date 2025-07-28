use color_eyre::eyre::{Context, Result};
#[cfg(target_os = "windows")]
use std::path::PathBuf;
use sha1::{Sha1};

#[cfg(not(target_os = "windows"))]
use swss_common::{link_to_swsscommon_logger, LoggerConfigChangeHandler};

use lazy_static::lazy_static;
use std::sync::Mutex;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    self, filter, fmt, fmt::format::FmtSpan, prelude::__tracing_subscriber_SubscriberExt, reload,
    util::SubscriberInitExt, Layer, Registry,
};

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
pub fn init(program_name: &'static str, link_swsscommon_logger: bool) -> Result<()> {
    let log_level_env_var = format!("{}_LOG_LEVEL", program_name.to_uppercase());

    let mut log_env_set = true;
    let mut hasher = Sha1::new();
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
                .with_ansi(false)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

            tracing_subscriber::registry()
                .with(level_layer.and_then(file_subscriber))
                .with(ErrorLayer::default())
                .init();
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

    tracing_subscriber::registry()
        .with(filter.and_then(file_subscriber))
        .with(ErrorLayer::default())
        .init();

    Ok(())
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
mod test {
    #[test]
    fn log_can_be_initialized() {
        let result = super::init("test", true);
        assert!(result.is_ok());
    }
}
