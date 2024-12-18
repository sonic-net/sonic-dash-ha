use color_eyre::eyre::{Context, Result};
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    self, fmt::format::FmtSpan, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

#[cfg(target_os = "windows")]
use std::path::PathBuf;

#[cfg(debug_assertions)]
const DEFAULT_LOG_LEVEL: &str = "debug";

#[cfg(not(debug_assertions))]
const DEFAULT_LOG_LEVEL: &str = "info";

pub fn init(program_name: &'static str) -> Result<()> {
    let log_level_env_var = format!("{}_LOG_LEVEL", program_name.to_uppercase());

    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG")
            .or_else(|_| std::env::var(log_level_env_var))
            .unwrap_or_else(|_| DEFAULT_LOG_LEVEL.to_string()),
    );

    let file_subscriber = new_file_subscriber(program_name).wrap_err("Unable to create file subscriber.")?;

    let file_subscriber = file_subscriber
        .with_line_number(true)
        .with_target(false)
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env());

    tracing_subscriber::registry()
        .with(file_subscriber)
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
        std::ffi::CString::new(program_name).wrap_err(format!("Unable to create syslog identity: {}", program_name))?;
    let (options, facility) = Default::default();
    let syslog =
        syslog_tracing::Syslog::new(identity, options, facility).wrap_err("Unable to create syslog writer.")?;
    Ok(tracing_subscriber::fmt::layer().with_writer(syslog))
}

mod test {
    #[test]
    fn log_can_be_initialized() {
        let result = super::init("test");
        assert!(result.is_ok());
    }
}
