use color_eyre::eyre::{Context, Result};

pub fn init(program_name: &'static str) -> Result<()> {
    std::env::set_var("RUST_BACKTRACE", "full");

    let (panic_hook, eyre_hook) = color_eyre::config::HookBuilder::default()
        .panic_section(format!(
            "{} crashed. Consider reporting it at {}",
            program_name,
            env!("CARGO_PKG_REPOSITORY")
        ))
        .capture_span_trace_by_default(true)
        .display_location_section(true)
        .display_env_section(true)
        .into_hooks();

    eyre_hook
        .install()
        .wrap_err("Unable to install color-eyre panic hook")?;

    std::panic::set_hook(Box::new(move |panic_info| {
        let msg = format!("{}", panic_hook.panic_report(panic_info));
        tracing::error!("Error: {}", msg);

        #[cfg(not(debug_assertions))]
        eprintln!("{msg}");

        #[cfg(not(debug_assertions))]
        report_panic_as_dump(program_name, panic_info);

        std::process::exit(1);
    }));

    Ok(())
}

#[allow(dead_code)]
fn report_panic_as_dump(program_name: &'static str, panic_info: &std::panic::PanicHookInfo) {
    let meta = human_panic::Metadata::new(program_name, env!("CARGO_PKG_VERSION"))
        .authors(env!("CARGO_PKG_AUTHORS").replace(':', ", "))
        .homepage(env!("CARGO_PKG_HOMEPAGE"));

    let file_path = human_panic::handle_dump(&meta, panic_info);
    human_panic::print_msg(file_path, &meta).expect("human-panic: printing error message to console failed");
}

mod test {
    #[test]
    fn panic_hook_can_be_installed() {
        let result = super::init("test");
        assert!(result.is_ok());
    }
}
