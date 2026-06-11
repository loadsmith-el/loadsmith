/// Spawns a plugin process with proper fd wiring.
///
/// Channel layout:
///   fd0 (stdin)  ← control JSONL  (core → plugin)
///   fd1 (stdout) → control JSONL  (plugin → core)
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
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use std::process::Stdio;

pub struct SpawnedPlugin {
    pub child: Child,
    /// Core's write end → plugin's stdin (control in).
    pub control_in: ChildStdin,
    /// Plugin's stdout → core's read end (control out).
    pub control_out: ChildStdout,
    /// Core's read end of the event pipe (fd4). Taken once for draining.
    event_fd: Option<std::fs::File>,
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
pub fn spawn_plugin(binary: &Path, child_data_fd: Option<RawFd>) -> Result<SpawnedPlugin> {
    let (event_read_raw, event_write_raw) = create_cloexec_pipe()?;

    let mut cmd = Command::new(binary);
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());

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

    let core_event = unsafe { std::fs::File::from_raw_fd(event_read_raw) };

    Ok(SpawnedPlugin { child, control_in, control_out, event_fd: Some(core_event) })
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
