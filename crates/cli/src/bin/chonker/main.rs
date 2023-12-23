mod command;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxResult<T> = Result<T, BoxError>;

#[derive(clap::Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    Encode { path: String },
    Decode { path: String },
    Import,
    Export,
    Delete,
}

#[tokio::main]
async fn main() -> BoxResult<()> {
    use clap::Parser;
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        match command {
            Commands::Encode { path } => {
                crate::command::encode(std::path::Path::new(&path)).await?;
            },
            Commands::Decode { .. } => todo!(),
            Commands::Import => {},
            Commands::Export => {},
            Commands::Delete => {},
        }
    }
    Ok(())
}
