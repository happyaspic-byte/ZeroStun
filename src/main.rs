use std::path::PathBuf;
use std::process::ExitCode as StdExitCode;

use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use zerostun::codec::CompressionCodec;
use zerostun::config::{parse_byte_size, BackupConfig};
use zerostun::error::ExitCode;
use zerostun::repository::Repository;
use zerostun::telemetry::ProgressMode;

#[derive(Parser, Debug)]
#[command(
    name = "zerostun",
    about = "ZeroStun - Lightweight, bounded-I/O backup engine for edge and FT systems",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, global = true, env = "ZEROSTUN_JSON")]
    json: bool,

    #[arg(short, long, global = true, env = "ZEROSTUN_QUIET")]
    quiet: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(short, long)]
        repo: PathBuf,
    },
    Backup(BackupArgs),
    Verify {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(short, long)]
        backup_id: String,
    },
    Restore {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(short, long)]
        backup_id: String,
        #[arg(short, long)]
        target: PathBuf,
        #[arg(short, long)]
        force: bool,
    },
    Inspect {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(short, long)]
        backup_id: String,
    },
    List {
        #[arg(short, long)]
        repo: PathBuf,
    },
}

#[derive(Args, Debug)]
struct BackupArgs {
    #[arg(short, long)]
    repo: PathBuf,

    #[arg(short, long)]
    source: PathBuf,

    #[arg(long, default_value = "8KiB")]
    min_chunk: String,

    #[arg(long, default_value = "64KiB")]
    avg_chunk: String,

    #[arg(long, default_value = "256KiB")]
    max_chunk: String,

    #[arg(long, default_value = "zstd")]
    codec: String,

    #[arg(long)]
    read_rate: Option<String>,

    #[arg(long)]
    read_iops: Option<u64>,

    #[arg(long)]
    write_rate: Option<String>,

    #[arg(long, default_value_t = 2)]
    workers: usize,

    #[arg(long, default_value_t = 8)]
    queue_depth: usize,
}

#[tokio::main]
async fn main() -> StdExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => StdExitCode::from(code as u8),
        Err(e) => {
            eprintln!("Error: {e}");
            StdExitCode::from(e.exit_code() as u8)
        }
    }
}

async fn run(cli: Cli) -> zerostun::error::Result<ExitCode> {
    match cli.command {
        Commands::Init { repo } => {
            let _ = Repository::init(&repo)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({ "status": "initialized", "repo": repo.display().to_string() })
                );
            } else if !cli.quiet {
                println!("Repository initialized at {}", repo.display());
            }
            Ok(ExitCode::Success)
        }
        Commands::Backup(args) => {
            let repo = Repository::open(&args.repo)?;
            let codec: CompressionCodec = args.codec.parse()?;
            let min_c = parse_byte_size(&args.min_chunk)? as usize;
            let avg_c = parse_byte_size(&args.avg_chunk)? as usize;
            let max_c = parse_byte_size(&args.max_chunk)? as usize;

            let read_bps = match args.read_rate {
                Some(s) => Some(parse_byte_size(&s)?),
                None => None,
            };
            let write_bps = match args.write_rate {
                Some(s) => Some(parse_byte_size(&s)?),
                None => None,
            };

            let cfg = BackupConfig {
                min_chunk: min_c,
                avg_chunk: avg_c,
                max_chunk: max_c,
                codec,
                read_bytes_per_sec: read_bps,
                read_iops: args.read_iops,
                write_bytes_per_sec: write_bps,
                workers: args.workers,
                queue_depth: args.queue_depth,
                progress: if cli.quiet || cli.json {
                    ProgressMode::None
                } else {
                    ProgressMode::Auto
                },
            };

            let summary = zerostun::engine::backup(&repo, &args.source, &cfg).await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&summary)
                        .map_err(|e| zerostun::Error::InvalidConfig(e.to_string()))?
                );
            } else if !cli.quiet {
                println!("Backup completed successfully.");
                println!("  Backup ID:     {}", summary.backup_id);
                println!("  Original size: {} bytes", summary.original_bytes);
                println!("  Stored size:   {} bytes", summary.stored_bytes);
                println!("  Total chunks:  {}", summary.total_chunks);
                println!("  Unique chunks: {}", summary.unique_chunks);
                println!("  Reused chunks: {}", summary.reused_chunks);
                println!("  Root hash:     {}", summary.root_hash);
                println!("  Dedupe ratio:  {:.2}x", summary.dedupe_ratio);
            }
            Ok(ExitCode::Success)
        }
        Commands::Verify { repo, backup_id } => {
            let repo = Repository::open(&repo)?;
            let report = zerostun::engine::verify(&repo, &backup_id).await?;
            let is_ok = report.is_ok();
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .map_err(|e| zerostun::Error::InvalidConfig(e.to_string()))?
                );
            } else if !cli.quiet {
                if is_ok {
                    println!("Backup {} is VALID.", report.backup_id);
                    println!("  Total chunks: {}", report.total_chunks);
                    println!("  Total bytes:  {}", report.total_bytes);
                    println!("  Root hash:    {}", report.root_hash);
                } else {
                    eprintln!("Backup {} is CORRUPT / INVALID!", report.backup_id);
                    if let Some(err) = &report.error {
                        eprintln!("  Reason: {err}");
                    }
                }
            }
            if is_ok {
                Ok(ExitCode::Success)
            } else {
                Ok(ExitCode::Integrity)
            }
        }
        Commands::Restore {
            repo,
            backup_id,
            target,
            force,
        } => {
            let repo = Repository::open(&repo)?;
            zerostun::engine::restore(&repo, &backup_id, &target, force).await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({ "status": "restored", "backup_id": backup_id, "target": target.display().to_string() })
                );
            } else if !cli.quiet {
                println!("Restored backup {} to {}", backup_id, target.display());
            }
            Ok(ExitCode::Success)
        }
        Commands::Inspect { repo, backup_id } => {
            let repo = Repository::open(&repo)?;
            let report = zerostun::engine::inspect(&repo, &backup_id)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .map_err(|e| zerostun::Error::InvalidConfig(e.to_string()))?
                );
            } else if !cli.quiet {
                println!("Backup metadata for {}", report.backup_id);
                println!("  Source path:      {}", report.source_path);
                println!("  Logical size:     {} bytes", report.total_logical_bytes);
                println!("  Stored size:      {} bytes", report.stored_bytes);
                println!("  Total chunks:     {}", report.total_chunks);
                println!("  Unique chunks:    {}", report.unique_chunks);
                println!(
                    "  FastCDC min/avg/max: {}/{}/{}",
                    report.fastcdc_params.0, report.fastcdc_params.1, report.fastcdc_params.2
                );
                println!("  Root hash:        {}", report.root_hash);
            }
            Ok(ExitCode::Success)
        }
        Commands::List { repo } => {
            let repo = Repository::open(&repo)?;
            let list = repo.list_backup_summaries()?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&list)
                        .map_err(|e| zerostun::Error::InvalidConfig(e.to_string()))?
                );
            } else if !cli.quiet {
                println!("Backups ({}):", list.len());
                println!(
                    "{:<30} {:>12} {:>8}  SOURCE",
                    "BACKUP ID", "BYTES", "CHUNKS"
                );
                for item in list {
                    println!(
                        "{:<30} {:>12} {:>8}  {}",
                        item.backup_id,
                        item.total_logical_bytes,
                        item.total_chunks,
                        item.source_path
                    );
                }
            }
            Ok(ExitCode::Success)
        }
    }
}
