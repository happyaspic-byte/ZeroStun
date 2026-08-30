use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

pub const MAX_COMMAND_OUTPUT_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub secret_env: BTreeSet<String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            secret_env: BTreeSet::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn secret_env(mut self, key: impl Into<String>) -> Self {
        self.secret_env.insert(key.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: i32,
}

pub trait CommandRunner: Send + Sync {
    fn run(
        &self,
        spec: &CommandSpec,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<CommandOutput>> + Send;
}

#[derive(Debug, Clone)]
pub enum FakeRunnerScript {
    Ok {
        stdout: Vec<u8>,
    },
    Fail {
        code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    Hang {
        duration: Duration,
    },
}

impl FakeRunnerScript {
    pub fn ok(stdout: impl AsRef<[u8]>) -> Self {
        Self::Ok {
            stdout: stdout.as_ref().to_vec(),
        }
    }

    pub fn fail(code: i32, stdout: impl AsRef<[u8]>, stderr: impl AsRef<[u8]>) -> Self {
        Self::Fail {
            code,
            stdout: stdout.as_ref().to_vec(),
            stderr: stderr.as_ref().to_vec(),
        }
    }

    pub fn hang(duration: Duration) -> Self {
        Self::Hang { duration }
    }
}

#[derive(Debug, Clone)]
pub struct FakeRunner {
    scripts: Arc<Mutex<VecDeque<FakeRunnerScript>>>,
    recorded: Arc<Mutex<Vec<RecordedCommand>>>,
}

impl FakeRunner {
    pub fn scripted(scripts: impl IntoIterator<Item = FakeRunnerScript>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            recorded: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn recorded(&self) -> Vec<RecordedCommand> {
        self.recorded
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

impl CommandRunner for FakeRunner {
    async fn run(
        &self,
        spec: &CommandSpec,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<CommandOutput> {
        {
            let mut recorded = self
                .recorded
                .lock()
                .map_err(|_| Error::Snapshot("runner lock poisoned".to_string()))?;
            recorded.push(RecordedCommand {
                program: spec.program.clone(),
                args: spec.args.clone(),
            });
        }

        let script = {
            let mut scripts = self
                .scripts
                .lock()
                .map_err(|_| Error::Snapshot("runner lock poisoned".to_string()))?;
            scripts
                .pop_front()
                .ok_or_else(|| Error::Snapshot("no scripted command remaining".to_string()))?
        };

        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }

        match script {
            FakeRunnerScript::Hang { duration } => {
                tokio::select! {
                    _ = cancel.cancelled() => Err(Error::Cancelled),
                    _ = sleep(timeout) => Err(Error::Snapshot("snapshot command timed out".to_string())),
                    _ = sleep(duration) => Err(Error::Snapshot("snapshot command hung".to_string())),
                }
            }
            FakeRunnerScript::Ok { stdout } => finish_output(spec, 0, stdout, Vec::new()),
            FakeRunnerScript::Fail {
                code,
                stdout,
                stderr,
            } => finish_output(spec, code, stdout, stderr),
        }
    }
}

pub fn redact_text(spec: &CommandSpec, text: &str) -> String {
    let mut out = text.to_string();
    for key in &spec.secret_env {
        if let Some((_, value)) = spec.env.iter().find(|(candidate, _)| candidate == key) {
            if !value.is_empty() {
                out = out.replace(value, "[redacted]");
            }
        }
    }
    out
}

fn finish_output(
    spec: &CommandSpec,
    status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> Result<CommandOutput> {
    if stdout.len() > MAX_COMMAND_OUTPUT_BYTES || stderr.len() > MAX_COMMAND_OUTPUT_BYTES {
        return Err(Error::Snapshot(
            "snapshot command output exceeds bounded maximum".to_string(),
        ));
    }
    if status != 0 {
        let stdout_text = String::from_utf8_lossy(&stdout);
        let stderr_text = String::from_utf8_lossy(&stderr);
        let combined = format!("{stdout_text}{stderr_text}");
        return Err(Error::Snapshot(format!(
            "snapshot command failed with status {status}: {}",
            redact_text(spec, &combined)
        )));
    }
    Ok(CommandOutput {
        stdout,
        stderr,
        status,
    })
}
