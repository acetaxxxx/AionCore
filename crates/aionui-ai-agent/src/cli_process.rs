use std::collections::HashMap;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use aionui_common::AppError;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast, watch};
use tracing::{debug, error, trace, warn};

/// Wrapper to hold a pre-subscribed receiver from before background tasks start.
/// Ensures no events are lost between process spawn and consumer subscription.
type InitialReceiver = std::sync::Mutex<Option<broadcast::Receiver<serde_json::Value>>>;

/// Configuration for spawning a CLI agent subprocess.
#[derive(Debug, Clone)]
pub struct CliSpawnConfig {
    /// Path to the executable.
    pub command: String,
    /// Command-line arguments.
    pub args: Vec<String>,
    /// Additional environment variables (merged with inherited env).
    pub env: HashMap<String, String>,
    /// Working directory for the subprocess. `None` inherits from parent.
    pub cwd: Option<String>,
}

/// Default broadcast channel capacity for stdout events.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Manages a CLI subprocess with JSON-over-stdin/stdout communication.
///
/// Events are read from stdout as line-delimited JSON (`serde_json::Value`).
/// Messages are sent to the subprocess via stdin as JSON lines.
/// The specific Agent implementation (ACP, Gemini, etc.) is responsible for
/// interpreting the raw JSON into typed [`AgentStreamEvent`](crate::AgentStreamEvent) values.
pub struct CliAgentProcess {
    /// Stdin writer, wrapped in Mutex for concurrent send safety.
    /// Set to `None` once stdin is closed (process exited or explicit close).
    stdin: Mutex<Option<tokio::process::ChildStdin>>,
    /// OS-level process ID.
    pid: u32,
    /// Broadcast sender for parsed stdout events.
    event_tx: broadcast::Sender<serde_json::Value>,
    /// Watch channel that transitions from `None` → `Some(ExitStatus)` on exit.
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    /// Pre-subscribed receiver created before background tasks start.
    /// Take this via [`take_initial_receiver`] to guarantee no events are lost.
    initial_rx: InitialReceiver,
    /// Handle to the stdout reader task (for cleanup).
    _stdout_handle: Arc<tokio::task::JoinHandle<()>>,
    /// Handle to the stderr reader task (for cleanup).
    _stderr_handle: Arc<tokio::task::JoinHandle<()>>,
    /// Handle to the exit monitor task (for cleanup).
    _exit_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl CliAgentProcess {
    /// Spawn a new CLI subprocess.
    ///
    /// The child process is started with stdin, stdout, and stderr piped.
    /// Background tasks are spawned to:
    /// - Read stdout line-by-line and parse each line as JSON
    /// - Read stderr and log warnings
    /// - Monitor process exit
    pub async fn spawn(config: CliSpawnConfig) -> Result<Self, AppError> {
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args)
            .envs(&config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        if let Some(ref cwd) = config.cwd {
            cmd.current_dir(cwd);
        }

        let mut child: Child = cmd.spawn().map_err(|e| {
            error!(command = %config.command, error = %e, "Failed to spawn CLI process");
            AppError::Internal(format!(
                "Failed to spawn CLI process '{}': {}",
                config.command, e
            ))
        })?;

        let pid = child.id().ok_or_else(|| {
            AppError::Internal("Failed to obtain PID from spawned process".into())
        })?;
        debug!(pid, command = %config.command, "CLI process spawned");

        let stdout = child.stdout.take().ok_or_else(|| {
            AppError::Internal("Failed to capture stdout from child process".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            AppError::Internal("Failed to capture stderr from child process".into())
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            AppError::Internal("Failed to capture stdin for child process".into())
        })?;

        let (event_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        // Pre-subscribe before spawning background tasks to guarantee no events are lost
        let initial_rx = event_tx.subscribe();
        let (exit_tx, exit_rx) = watch::channel(None);

        // Background task: read stdout line-by-line → parse JSON → broadcast
        let stdout_tx = event_tx.clone();
        let stdout_handle = tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(value) => {
                        // Ignore send errors — no active subscribers is fine
                        let _ = stdout_tx.send(value);
                    }
                    Err(e) => {
                        trace!(line = trimmed, error = %e, "Non-JSON line from stdout, skipping");
                    }
                }
            }

            debug!(pid, "Stdout reader finished");
        });

        // Background task: read stderr → log warnings
        let stderr_handle = tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    warn!(pid, stderr = trimmed, "CLI process stderr");
                }
            }

            debug!(pid, "Stderr reader finished");
        });

        // Background task: monitor process exit
        let exit_handle = tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    debug!(pid, ?status, "CLI process exited");
                    let _ = exit_tx.send(Some(status));
                }
                Err(e) => {
                    error!(pid, error = %e, "Failed to wait on CLI process");
                    // Signal exit even on error so callers don't hang
                    let _ = exit_tx.send(None);
                }
            }
        });

        Ok(Self {
            stdin: Mutex::new(Some(stdin)),
            pid,
            event_tx,
            exit_rx,
            initial_rx: std::sync::Mutex::new(Some(initial_rx)),
            _stdout_handle: Arc::new(stdout_handle),
            _stderr_handle: Arc::new(stderr_handle),
            _exit_handle: Arc::new(exit_handle),
        })
    }

    /// Send a JSON message to the subprocess via stdin.
    ///
    /// The message is serialized as a single line followed by a newline.
    /// Returns an error if stdin has been closed (process exited).
    pub async fn send(&self, message: &serde_json::Value) -> Result<(), AppError> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or_else(|| {
            AppError::Internal("Cannot send: stdin is closed (process exited)".into())
        })?;

        let mut buf = serde_json::to_vec(message)
            .map_err(|e| AppError::Internal(format!("Failed to serialize message: {e}")))?;
        buf.push(b'\n');

        stdin.write_all(&buf).await.map_err(|e| {
            error!(pid = self.pid, error = %e, "Failed to write to stdin");
            AppError::Internal(format!("Failed to write to stdin: {e}"))
        })?;

        stdin.flush().await.map_err(|e| {
            error!(pid = self.pid, error = %e, "Failed to flush stdin");
            AppError::Internal(format!("Failed to flush stdin: {e}"))
        })?;

        Ok(())
    }

    /// Subscribe to the event stream from stdout.
    ///
    /// Returns a broadcast receiver that yields raw `serde_json::Value` events
    /// as they are parsed from the subprocess stdout.
    pub fn subscribe(&self) -> broadcast::Receiver<serde_json::Value> {
        self.event_tx.subscribe()
    }

    /// Take the pre-subscribed receiver created before background tasks started.
    ///
    /// This receiver captures all events from the very first output line.
    /// Can only be called once; subsequent calls return `None`.
    pub fn take_initial_receiver(&self) -> Option<broadcast::Receiver<serde_json::Value>> {
        self.initial_rx.lock().unwrap().take()
    }

    /// Close stdin, signaling the subprocess that no more input will arrive.
    pub async fn close_stdin(&self) {
        let mut guard = self.stdin.lock().await;
        if guard.take().is_some() {
            debug!(pid = self.pid, "Stdin closed");
        }
    }

    /// Gracefully terminate the subprocess.
    ///
    /// 1. Close stdin
    /// 2. Wait up to `grace_period` for the process to exit on its own
    /// 3. If still running after grace period, send SIGKILL
    pub async fn kill(&self, grace_period: Duration) -> Result<(), AppError> {
        // Close stdin first to signal the child
        self.close_stdin().await;

        // Wait for graceful exit within the grace period
        let mut rx = self.exit_rx.clone();
        let exited = tokio::time::timeout(grace_period, async {
            // If already exited, return immediately
            if rx.borrow().is_some() {
                return;
            }
            // Wait for state change
            let _ = rx.changed().await;
        })
        .await;

        if exited.is_ok() && self.exit_rx.borrow().is_some() {
            debug!(pid = self.pid, "CLI process exited gracefully");
            return Ok(());
        }

        // Force kill
        warn!(pid = self.pid, "Grace period expired, sending SIGKILL");
        force_kill(self.pid)
    }

    /// Check whether the subprocess is still running.
    pub fn is_running(&self) -> bool {
        self.exit_rx.borrow().is_none()
    }

    /// Get the exit status if the process has exited.
    pub fn exit_status(&self) -> Option<ExitStatus> {
        *self.exit_rx.borrow()
    }

    /// Get the OS process ID.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Wait for the process to exit (blocks until exit or cancellation).
    pub async fn wait_for_exit(&self) -> Option<ExitStatus> {
        let mut rx = self.exit_rx.clone();
        // If already exited, return immediately
        if let Some(status) = *rx.borrow() {
            return Some(status);
        }
        // Wait for state change
        let _ = rx.changed().await;
        *rx.borrow()
    }
}

/// Send SIGKILL to a process by PID.
///
/// Uses the system `kill` command to avoid a `libc` dependency.
/// If the process has already exited, this is a no-op.
fn force_kill(pid: u32) -> Result<(), AppError> {
    #[cfg(unix)]
    {
        let result = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .output();

        match result {
            Ok(output) if output.status.success() => {
                debug!(pid, "SIGKILL sent successfully");
                Ok(())
            }
            Ok(_output) => {
                // Non-zero exit likely means process already exited — acceptable
                debug!(pid, "Process already exited before SIGKILL");
                Ok(())
            }
            Err(e) => {
                error!(pid, error = %e, "Failed to execute kill command");
                Err(AppError::Internal(format!(
                    "Failed to kill process {pid}: {e}"
                )))
            }
        }
    }
    #[cfg(not(unix))]
    {
        Err(AppError::Internal(format!(
            "Force kill not supported on this platform for pid {pid}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::time::timeout;

    fn echo_json_config(json_str: &str) -> CliSpawnConfig {
        CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), format!("echo '{json_str}'")],
            env: HashMap::new(),
            cwd: None,
        }
    }

    #[tokio::test]
    async fn spawn_and_receive_event() {
        let config = echo_json_config(r#"{"type":"text","data":{"content":"hello"}}"#);
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("Timed out waiting for event")
            .expect("Channel closed");

        assert_eq!(event["type"], "text");
        assert_eq!(event["data"]["content"], "hello");

        // Process should exit after echo
        let status = timeout(Duration::from_secs(5), proc.wait_for_exit())
            .await
            .expect("Timed out waiting for exit");
        assert!(status.is_some());
    }

    #[tokio::test]
    async fn spawn_multiple_events() {
        let script = r#"echo '{"type":"start","data":{}}' && echo '{"type":"text","data":{"content":"line1"}}' && echo '{"type":"finish","data":{}}'  "#;
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), script.into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let mut events = Vec::new();
        for _ in 0..3 {
            let event = timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("Timed out")
                .expect("Channel closed");
            events.push(event);
        }

        assert_eq!(events[0]["type"], "start");
        assert_eq!(events[1]["type"], "text");
        assert_eq!(events[2]["type"], "finish");
    }

    #[tokio::test]
    async fn non_json_lines_are_skipped() {
        let script = r#"echo 'not json' && echo '{"type":"ok","data":{}}' && echo 'also not json'"#;
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), script.into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");

        // Only the valid JSON line should come through
        assert_eq!(event["type"], "ok");

        // Process exits, no more events
        proc.wait_for_exit().await;
    }

    #[tokio::test]
    async fn empty_lines_are_skipped() {
        let script = "echo '' && echo '  ' && echo '{\"type\":\"data\",\"data\":{}}' && echo ''";
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), script.into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");
        assert_eq!(event["type"], "data");
    }

    #[tokio::test]
    async fn send_json_to_stdin() {
        // `cat` echoes stdin to stdout
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec![
                "-c".into(),
                // Read one line from stdin, echo it, then exit
                "read line && echo \"$line\"".into(),
            ],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let msg = json!({"type": "sendMessage", "data": {"content": "test"}});
        proc.send(&msg).await.unwrap();
        proc.close_stdin().await;

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");
        assert_eq!(event["type"], "sendMessage");
        assert_eq!(event["data"]["content"], "test");
    }

    #[tokio::test]
    async fn send_after_exit_returns_error() {
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), "true".into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();

        // Wait for process to exit
        proc.wait_for_exit().await;
        // Close stdin since process exited
        proc.close_stdin().await;

        let result = proc.send(&json!({"type":"test"})).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn is_running_reflects_process_state() {
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), "sleep 10".into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();

        assert!(proc.is_running());
        assert!(proc.exit_status().is_none());

        proc.kill(Duration::from_millis(100)).await.unwrap();

        // After kill, process should no longer be running
        // Give a moment for exit status to propagate
        timeout(Duration::from_secs(5), proc.wait_for_exit())
            .await
            .unwrap();
        assert!(!proc.is_running());
        assert!(proc.exit_status().is_some());
    }

    #[tokio::test]
    async fn kill_with_grace_period_exits_cleanly() {
        // Process that exits quickly when stdin closes
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), "read line".into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        assert!(proc.is_running());

        // kill with generous grace period — process should exit when stdin closes
        proc.kill(Duration::from_secs(5)).await.unwrap();
        assert!(!proc.is_running());
    }

    #[tokio::test]
    async fn kill_force_kills_after_grace_period() {
        // Process that ignores stdin close (trap + infinite loop)
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec![
                "-c".into(),
                "trap '' TERM; while true; do sleep 1; done".into(),
            ],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        assert!(proc.is_running());

        // Very short grace period → should force kill
        let result = proc.kill(Duration::from_millis(100)).await;
        assert!(result.is_ok());

        timeout(Duration::from_secs(5), proc.wait_for_exit())
            .await
            .unwrap();
        assert!(!proc.is_running());
    }

    #[tokio::test]
    async fn spawn_with_env_and_cwd() {
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec![
                "-c".into(),
                "echo \"{\\\"val\\\":\\\"$MY_TEST_VAR\\\"}\"".into(),
            ],
            env: HashMap::from([("MY_TEST_VAR".into(), "hello_env".into())]),
            cwd: Some("/tmp".into()),
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        let mut rx = proc.subscribe();

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");
        assert_eq!(event["val"], "hello_env");
    }

    #[tokio::test]
    async fn spawn_invalid_command_returns_error() {
        let config = CliSpawnConfig {
            command: "/nonexistent/binary/that/does/not/exist".into(),
            args: vec![],
            env: HashMap::new(),
            cwd: None,
        };
        let result = CliAgentProcess::spawn(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pid_is_nonzero_for_valid_process() {
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), "sleep 10".into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();
        assert!(proc.pid() > 0);
        proc.kill(Duration::from_millis(100)).await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_exit_returns_immediately_if_already_exited() {
        let config = CliSpawnConfig {
            command: "sh".into(),
            args: vec!["-c".into(), "true".into()],
            env: HashMap::new(),
            cwd: None,
        };
        let proc = CliAgentProcess::spawn(config).await.unwrap();

        // Wait for first exit
        let status1 = timeout(Duration::from_secs(5), proc.wait_for_exit())
            .await
            .expect("Timed out");
        assert!(status1.is_some());

        // Calling again should return immediately
        let status2 = timeout(Duration::from_millis(100), proc.wait_for_exit())
            .await
            .expect("Should return immediately");
        assert!(status2.is_some());
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_same_events() {
        let config = echo_json_config(r#"{"type":"broadcast","data":{"msg":"all"}}"#);
        let proc = CliAgentProcess::spawn(config).await.unwrap();

        let mut rx1 = proc.subscribe();
        let mut rx2 = proc.subscribe();

        let e1 = timeout(Duration::from_secs(5), rx1.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");
        let e2 = timeout(Duration::from_secs(5), rx2.recv())
            .await
            .expect("Timed out")
            .expect("Channel closed");

        assert_eq!(e1, e2);
        assert_eq!(e1["type"], "broadcast");
    }
}
