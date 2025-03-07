use clap::{Parser, ValueEnum};
use container::Container;
mod container;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Action {
    Start,
    Stop,
    Kill,
    Wait,
    Id,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(value_enum)]
    /// The action to take for the container
    action: Action,

    /// The name of the container
    name: String,

    /// Timeout for the action to occur
    #[arg(short, long)]
    timeout: Option<i64>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let container = Container::new(&cli.name);

    match cli.action {
        Action::Start => container.start().await,
        Action::Wait => container.wait().await,
        Action::Stop => container.stop(cli.timeout).await,
        Action::Kill => container.kill().await,
        Action::Id => println!("{}", container.container_id()),
    };
}
