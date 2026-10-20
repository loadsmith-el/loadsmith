/// Spawns a plugin process with proper fd wiring.
///
/// Channel layout:
///   fd0 (stdin)  ← control JSONL  (core → plugin)
///   fd1 (stdout) → control JSONL  (plugin → core)
///   fd2 (stderr) → copied to the engine's stderr, line by line, masked
///   fd3          Arrow IPC data: source writes, destination reads (shared pipe)
///   fd4          → event JSONL    (plugin → core, write end in child)
///
/// Data flow: caller creates ONE pipe with `create_data_pipe()`, passes write
/// end to source and read end to destination. Core never touches the data pipe.
///
/// This module contains the only `unsafe` code in the project. The `pre_exec`
/// closure runs in the forked child between fork(2) and execve(2), so only
/// async-signal-safe functions are called: dup2(2) and close(2) via libc.
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::Path;

use anyhow::Result;
use loadsmith_config::MaskList;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

use std::process::Stdio;

pub struct SpawnedPlugin {
    pub child: Child,
    /// Core's write end → plugin's stdin (control in).
    pub control_in: ChildStdin,
    /// Plugin's stdout → core's read end (control out).
    pub control_out: ChildStdout,
    /// Core's read end of the event pipe (fd4). Taken once for draining.
    event_fd: Option<std::fs::File>,
    /// With [`StderrRoute::Hold`]: the plugin's stderr lines, once it closed
    /// the pipe. Hand it to [`release_held_stderr`].
    pub held_stderr: Option<tokio::task::JoinHandle<Vec<String>>>,
}

impl SpawnedPlugin {
    /// Takes ownership of the event-channel (fd4) reader. Panics if called twice.
    pub fn event_fd_take(&mut self) -> std::fs::File {
        self.event_fd.take().expect("event_fd already taken")
    }
}

/// Creates a pipe for the Arrow data channel.
/// Returns `(read_end, write_end)`, each already wrapped as an owned `File` —
/// the fds were just created by `pipe2(2)`, so ownership is unambiguous right
/// here, which is the natural place to retire the `unsafe` `from_raw_fd` calls.
/// The end given to a plugin is converted back to a `RawFd` via the safe
/// `into_raw_fd()` before being passed to `spawn_plugin`.
pub fn create_data_pipe() -> Result<(std::fs::File, std::fs::File)> {
    let (read_raw, write_raw) = create_cloexec_pipe()?;
    // SAFETY: `pipe2` just created these two fds; each is wrapped exactly once
    // and ownership is unambiguous.
    let read = unsafe { std::fs::File::from_raw_fd(read_raw) };
    let write = unsafe { std::fs::File::from_raw_fd(write_raw) };
    Ok((read, write))
}

/// Spawns a plugin process.
///
/// `child_data_fd`: the fd to expose as fd3 (the Arrow data plane) in the child.
///   - For source plugins: pass `Some(WRITE end)` of the data pipe.
///   - For destination plugins: pass `Some(READ end)` of the data pipe.
///   - For sink plugins: pass `None` — a sink is a delivery stage with no data
///     plane, so fd3 is left unwired.
///
/// The `child_data_fd` is closed in the parent process after fork.
/// Call `create_data_pipe()` to get a (read, write) pair.
///
/// The plugin's stderr is piped through the core (see [`StderrRoute`]) and
/// masked, so a plugin that prints a resolved value (in a panic, a
/// fatal-error line) does not leak it.
pub fn spawn_plugin(
    binary: &Path,
    child_data_fd: Option<RawFd>,
    stderr_route: StderrRoute,
) -> Result<SpawnedPlugin> {
    let (event_read_raw, event_write_raw) = create_cloexec_pipe()?;

    let mut cmd = Command::new(binary);
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    // Each plugin leads its own process group, so a terminal's Ctrl+C (sent
    // to the foreground group) reaches only the core, which then cancels the
    // plugins gracefully instead of having them die mid-write. The abort path
    // kills the whole group, grandchildren included.
    cmd.process_group(0);
    // Any path that drops a plugin without reaping it (an early return, a
    // panic) still kills it.
    cmd.kill_on_drop(true);

    // SAFETY: Only async-signal-safe calls (dup2, close via libc) are made
    // inside pre_exec, which runs in the forked child before exec.
    unsafe {
        cmd.pre_exec(move || {
            if let Some(fd) = child_data_fd {
                dup2_or_err(fd, 3)?;
            }
            dup2_or_err(event_write_raw, 4)?;
            Ok(())
        });
    }

    let mut child = cmd.spawn()?;

    // Close child-side fds in the parent — must happen after spawn.
    unsafe {
        if let Some(fd) = child_data_fd {
            libc::close(fd);
        }
        libc::close(event_write_raw);
    }

    let control_in = child.stdin.take().expect("stdin was piped");
    let control_out = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    let held_stderr = match stderr_route {
        StderrRoute::Forward(mask) => {
            tokio::spawn(read_stderr(stderr, move |line| write_stderr(&mask.apply(line))));
            None
        }
        StderrRoute::Hold => {
            let held = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink = std::sync::Arc::clone(&held);
            let task = tokio::spawn(async move {
                read_stderr(stderr, move |line| {
                    let mut lines = sink.lock().unwrap_or_else(|e| e.into_inner());
                    if lines.len() < MAX_HELD_LINES {
                        lines.push(line.to_string());
                    }
                })
                .await;
                std::mem::take(&mut *held.lock().unwrap_or_else(|e| e.into_inner()))
            });
            Some(task)
        }
    };

    let core_event = unsafe { std::fs::File::from_raw_fd(event_read_raw) };

    Ok(SpawnedPlugin { child, control_in, control_out, event_fd: Some(core_event), held_stderr })
}

/// Where a plugin's stderr goes.
pub enum StderrRoute {
    /// Copied to the engine's stderr as it arrives, line by line, masked.
    Forward(MaskList),
    /// Kept until [`release_held_stderr`]: for a provider, whose values the
    /// core learns only from its replies — a line it prints before the core
    /// read the matching `resolved` could not be masked yet.
    Hold,
}

/// At most this many held stderr lines are kept (the rest are dropped).
const MAX_HELD_LINES: usize = 10_000;

/// Writes a plugin's held stderr lines to the engine's stderr, masked. Waits
/// briefly for the pipe to close (the plugin has exited by now).
pub async fn release_held_stderr(held: Option<tokio::task::JoinHandle<Vec<String>>>, mask: &MaskList) {
    let Some(task) = held else { return };
    let lines = match tokio::time::timeout(std::time::Duration::from_secs(2), task).await {
        Ok(Ok(lines)) => lines,
        _ => return,
    };
    for line in lines {
        write_stderr(&mask.apply(&line));
    }
}

/// Reads a plugin's stderr line by line until every holder of the pipe's
/// write end (the plugin, anything it spawned) has closed it. Bytes that are
/// not UTF-8 are replaced.
async fn read_stderr(stderr: ChildStderr, mut each: impl FnMut(&str)) {
    use tokio::io::AsyncBufReadExt;

    let mut reader = tokio::io::BufReader::new(stderr);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => each(String::from_utf8_lossy(&line).trim_end_matches('\n')),
        }
    }
}

fn write_stderr(line: &str) {
    use std::io::Write as _;
    let mut out = std::io::stderr().lock();
    let _ = writeln!(out, "{line}");
}

/// Creates a pipe with O_CLOEXEC on both ends.
fn create_cloexec_pipe() -> Result<(RawFd, RawFd)> {
    let mut fds: [RawFd; 2] = [-1, -1];
    let ret = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((fds[0], fds[1]))
}

/// SAFETY: must only be called in pre_exec (async-signal-safe context).
/// Redirects `src` to `dst_fd_number` and closes the original `src`.
unsafe fn dup2_or_err(src: RawFd, dst: RawFd) -> std::io::Result<()> {
    if libc::dup2(src, dst) == -1 {
        return Err(std::io::Error::last_os_error());
    }
    libc::close(src);
    Ok(())
}
