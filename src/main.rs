use std::path::PathBuf;
use std::process::ExitCode as StdExitCode;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tracing_subscriber::EnvFilter;

use zerostun::codec::CompressionCodec;
use zerostun::config::{parse_byte_size, BackupConfig};
use zerostun::daemon::{DaemonConfig, DaemonState, RunStatus};
use zerostun::error::{Error, ExitCode};
use zerostun::lifecycle::{evaluate_retention, FindingSeverity, PruneResult, RetentionPolicy};
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
    Delete {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(long)]
        backup_id: String,
        #[arg(long)]
        apply: bool,
    },
    Prune {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(long, default_value_t = 0)]
        keep_last: usize,
        #[arg(long, default_value_t = 0)]
        daily_days: u32,
        #[arg(long, default_value_t = 0)]
        weekly_weeks: u32,
        #[arg(long, default_value_t = 0)]
        monthly_months: u32,
        #[arg(long = "protect")]
        protect: Vec<String>,
        #[arg(long)]
        apply: bool,
    },
    Gc {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(long)]
        apply: bool,
    },
    Repair {
        #[arg(short, long)]
        repo: PathBuf,
        #[arg(long)]
        apply: bool,
    },
    #[command(subcommand)]
    Daemon(DaemonCommand),
    #[command(subcommand)]
    Jobs(JobsCommand),
    #[command(subcommand)]
    Runs(RunsCommand),
    Cancel {
        run_id: String,
        #[arg(long)]
        config: PathBuf,
    },
    Metrics {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum DaemonCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Status {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum JobsCommand {
    List {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum RunsCommand {
    List {
        #[arg(long)]
        config: PathBuf,
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();

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
        Commands::Delete {
            repo,
            backup_id,
            apply,
        } => run_delete(repo, backup_id, apply, cli.json, cli.quiet),
        Commands::Prune {
            repo,
            keep_last,
            daily_days,
            weekly_weeks,
            monthly_months,
            protect,
            apply,
        } => run_prune(
            PruneCommand {
                repo,
                keep_last,
                daily_days,
                weekly_weeks,
                monthly_months,
                protect,
                apply,
            },
            cli.json,
            cli.quiet,
        ),
        Commands::Gc { repo, apply } => run_gc(repo, apply, cli.json, cli.quiet),
        Commands::Repair { repo, apply } => run_repair(repo, apply, cli.json, cli.quiet),
        Commands::Daemon(DaemonCommand::Status { config }) => {
            let parsed = DaemonConfig::load(&config)?;
            let state = DaemonState::open(&parsed.state_db)?;
            let metrics = state.metrics()?;
            let jobs = state.jobs()?;
            let status = serde_json::json!({
                "jobs": jobs.len(),
                "runs_queued": metrics.runs_queued,
                "runs_running": metrics.runs_running,
                "runs_succeeded": metrics.runs_succeeded,
                "runs_failed": metrics.runs_failed,
                "runs_cancelled": metrics.runs_cancelled,
                "runs_recovering": metrics.runs_recovering,
            });
            emit_json(&status)?;
            Ok(ExitCode::Success)
        }
        Commands::Daemon(DaemonCommand::Run { config }) => {
            let parsed = DaemonConfig::load(&config)?;
            run_daemon(&parsed).await
        }
        Commands::Jobs(JobsCommand::List { config }) => {
            let parsed = DaemonConfig::load(&config)?;
            let state = DaemonState::open(&parsed.state_db)?;
            let jobs = state.jobs()?;
            emit_json(&jobs)?;
            Ok(ExitCode::Success)
        }
        Commands::Runs(RunsCommand::List { config }) => {
            let parsed = DaemonConfig::load(&config)?;
            let state = DaemonState::open(&parsed.state_db)?;
            let runs = state.runs()?;
            emit_json(&runs)?;
            Ok(ExitCode::Success)
        }
        Commands::Cancel { run_id, config } => {
            let parsed = DaemonConfig::load(&config)?;
            let state = DaemonState::open(&parsed.state_db)?;
            let run = state
                .get_run(&run_id)?
                .ok_or_else(|| zerostun::Error::RunNotActive {
                    run_id: run_id.clone(),
                })?;
            if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
                return Err(zerostun::Error::RunNotActive {
                    run_id: run_id.clone(),
                });
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let updated = state.transition(&run_id, RunStatus::Cancelled, now, None)?;
            emit_json(&updated)?;
            Ok(ExitCode::Success)
        }
        Commands::Metrics { config } => {
            let parsed = DaemonConfig::load(&config)?;
            let state = DaemonState::open(&parsed.state_db)?;
            let metrics = state.metrics()?;
            emit_json(&metrics)?;
            Ok(ExitCode::Success)
        }
    }
}

async fn run_daemon(config: &DaemonConfig) -> zerostun::error::Result<ExitCode> {
    use std::sync::Arc;
    use std::time::Duration;

    use zerostun::daemon::{DaemonRunner, ManualClock, Scheduler};
    use zerostun::snapshot::{FakeProvider, FakeRunner};

    let state = if config.state_db.exists() {
        DaemonState::open(&config.state_db)?
    } else {
        DaemonState::create(&config.state_db)?
    };
    for job in &config.jobs {
        state.put_job(job)?;
    }
    let now = unix_ms();
    let _ = state.recover_interrupted(now)?;
    let shutdown = zerostun::daemon::ShutdownController::new(Duration::from_millis(
        config.shutdown_deadline_ms,
    ));
    let token = shutdown.running_token();
    let clock = ManualClock::new(now);
    let scheduler = Scheduler::new(clock.clone());
    let runner = DaemonRunner::new(
        Arc::new(FakeProvider::new(FakeRunner::scripted([]))),
        clock.clone(),
    );

    #[cfg(target_os = "linux")]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).map_err(|error| {
            zerostun::Error::Daemon(format!("failed to install SIGTERM handler: {error}"))
        })?;
        loop {
            tokio::select! {
                _ = sigterm.recv() => {
                    shutdown.request_shutdown();
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    if !shutdown.admission_allowed() {
                        break;
                    }
                    clock.set(unix_ms());
                    let due = scheduler.due_jobs(&state, &config.jobs)?;
                    for due_job in due {
                        if !shutdown.admission_allowed() {
                            break;
                        }
                        if let Some(job) = config.jobs.iter().find(|job| job.id == due_job.job_id) {
                            let _ = runner.run_job(&state, job, &token).await;
                            state.set_last_scheduled(&job.id, clock.now_unix_ms())?;
                        }
                    }
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        wait_for_sigterm(token.clone()).await;
        shutdown.request_shutdown();
    }

    shutdown
        .wait_for_cleanup(async {
            let _ = state.recover_interrupted(unix_ms());
        })
        .await?;
    Ok(ExitCode::Success)
}

fn emit_json<T: Serialize>(value: &T) -> zerostun::error::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|e| Error::InvalidConfig(e.to_string()))?
    );
    Ok(())
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn map_gc_error(error: Error) -> Error {
    match error {
        Error::GarbageCollection(message) if message.contains("active reader") => {
            Error::ActiveReader(message)
        }
        Error::GarbageCollection(message) if message.contains("stale") => Error::StalePlan(message),
        other => other,
    }
}

fn run_delete(
    repo_path: PathBuf,
    backup_id: String,
    apply: bool,
    json: bool,
    quiet: bool,
) -> zerostun::error::Result<ExitCode> {
    let repo = Repository::open(&repo_path)?;
    let plan = repo.plan_delete(&backup_id)?;
    if !apply {
        if json {
            emit_json(&plan)?;
        } else if !quiet {
            println!("Dry-run delete plan");
            println!("  Backup ID: {}", plan.backup_id);
            println!("  Already hidden: {}", plan.already_deleted);
            println!("  No backup will be hidden until --apply.");
        }
        return Ok(ExitCode::Success);
    }

    let current = repo.plan_delete(&backup_id)?;
    if current != plan {
        return Err(Error::StalePlan(format!(
            "delete plan for {backup_id} is no longer current"
        )));
    }
    let result = repo.apply_delete(&plan)?;
    if json {
        emit_json(&result)?;
    } else if !quiet {
        println!("Applied delete plan.");
        println!("  Backup ID: {}", result.backup_id);
        println!("  Hidden: {}", result.tombstoned);
    }
    Ok(ExitCode::Success)
}

struct PruneCommand {
    repo: PathBuf,
    keep_last: usize,
    daily_days: u32,
    weekly_weeks: u32,
    monthly_months: u32,
    protect: Vec<String>,
    apply: bool,
}

fn run_prune(command: PruneCommand, json: bool, quiet: bool) -> zerostun::error::Result<ExitCode> {
    let repo = Repository::open(&command.repo)?;
    let policy = RetentionPolicy {
        keep_last: command.keep_last,
        daily_days: command.daily_days,
        weekly_weeks: command.weekly_weeks,
        monthly_months: command.monthly_months,
        protected_ids: command.protect.into_iter().collect(),
    };
    let plan = evaluate_retention(&repo.list_backup_summaries()?, &policy, unix_ms())?;
    if !command.apply {
        if json {
            emit_json(&plan)?;
        } else if !quiet {
            println!("Dry-run prune plan");
            println!("  Keep: {}", plan.keep.len());
            println!("  Would hide: {}", plan.delete.len());
            for warning in &plan.warnings {
                println!("  Warning: {warning}");
            }
            println!("  No backups will be hidden until --apply.");
        }
        return Ok(ExitCode::Success);
    }

    let _lock = repo.acquire_writer_lock()?;
    let current = evaluate_retention(
        &repo.list_backup_summaries()?,
        &policy,
        plan.evaluated_at_unix_ms,
    )?;
    if current.keep != plan.keep || current.delete != plan.delete {
        return Err(Error::StalePlan(
            "prune plan is no longer current".to_string(),
        ));
    }
    let mut deleted = Vec::new();
    for backup_id in &plan.delete {
        let delete_plan = repo.plan_delete(backup_id)?;
        let result = repo.apply_delete_locked(&delete_plan)?;
        if result.tombstoned {
            deleted.push(backup_id.clone());
        }
    }
    let result = PruneResult {
        keep: plan.keep.clone(),
        deleted,
    };
    if json {
        emit_json(&result)?;
    } else if !quiet {
        println!("Applied prune plan.");
        println!("  Keep: {}", result.keep.len());
        println!("  Hidden: {}", result.deleted.len());
    }
    Ok(ExitCode::Success)
}

fn run_gc(
    repo_path: PathBuf,
    apply: bool,
    json: bool,
    quiet: bool,
) -> zerostun::error::Result<ExitCode> {
    let repo = Repository::open(&repo_path)?;
    let plan = repo.plan_gc().map_err(map_gc_error)?;
    if !apply {
        if json {
            emit_json(&plan)?;
        } else if !quiet {
            println!("Dry-run garbage collection plan");
            println!("  GC ID: {}", plan.gc_id);
            println!("  Live chunks: {}", plan.live_chunks);
            println!("  Reclaim chunks: {}", plan.reclaim_chunks.len());
            println!("  Reclaim bytes: {}", plan.reclaim_bytes);
            println!("  No chunks will be removed until --apply.");
        }
        return Ok(ExitCode::Success);
    }

    let result = repo.apply_gc(&plan).map_err(map_gc_error)?;
    if json {
        emit_json(&result)?;
    } else if !quiet {
        println!("Applied garbage collection.");
        println!("  GC ID: {}", result.gc_id);
        println!("  Reclaimed chunks: {}", result.reclaimed_chunks);
        println!("  Reclaimed bytes: {}", result.reclaimed_bytes);
    }
    Ok(ExitCode::Success)
}

fn run_repair(
    repo_path: PathBuf,
    apply: bool,
    json: bool,
    quiet: bool,
) -> zerostun::error::Result<ExitCode> {
    let repo = Repository::open(&repo_path)?;
    let report = repo.inspect_repair()?;
    let plan = repo.plan_repair(&report)?;
    let critical = report
        .findings
        .iter()
        .find(|finding| finding.severity == FindingSeverity::Critical)
        .map(|finding| finding.detail.clone());
    if !apply {
        if json {
            emit_json(&plan)?;
        } else if !quiet {
            println!("Dry-run repair plan");
            println!("  Rebuild index: {}", plan.rebuild_index);
            println!("  Stale leases: {}", plan.stale_leases.len());
            println!("  GC recoveries: {}", plan.gc_recoveries.len());
            println!("  Findings: {}", report.findings.len());
            println!("  No repair mutations will run until --apply.");
        }
        if let Some(detail) = critical {
            return Err(Error::CriticalRepair(detail));
        }
        return Ok(ExitCode::Success);
    }

    if let Some(detail) = critical {
        if json {
            emit_json(&plan)?;
        }
        return Err(Error::CriticalRepair(detail));
    }

    let current_report = repo.inspect_repair()?;
    let current = repo.plan_repair(&current_report)?;
    if current != plan {
        return Err(Error::StalePlan(
            "repair plan is no longer current".to_string(),
        ));
    }
    let result = repo.apply_repair(&plan)?;
    if json {
        emit_json(&result)?;
    } else if !quiet {
        println!("Applied repair plan.");
        println!("  Rebuilt index: {}", result.rebuilt_index);
        println!("  Removed leases: {}", result.removed_leases.len());
        println!("  GC recoveries: {}", result.gc_recoveries.len());
    }
    Ok(ExitCode::Success)
}
