// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::process::Stdio;

use bytes::BytesMut;
use clap::Parser;
use futures::StreamExt;
use shlex;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::LinesStream;

const DEFAULT_CONCURRENT_LIMIT: usize = 16;
const DEFAULT_BUFFER_SIZE: usize = 1 << 30; // 1Gb

const CHUNK_BUFFER_SIZE: usize = 2 * 1024 * 1024; // 2mb

/// Backend for bazel remote execution / cache API.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of commands to run in parallel
    #[clap(short, long, value_parser, default_value_t = DEFAULT_CONCURRENT_LIMIT)]
    parallel_count: usize,

    /// Size in bytes of the stdout buffer for reach command.
    #[clap(short, long, value_parser, default_value_t = DEFAULT_BUFFER_SIZE)]
    buffer_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let stdin = stdin();
    let buf = BufReader::new(stdin);
    let stdin_commands = LinesStream::new(buf.lines());
    let mut process_stream = stdin_commands
        .map(|maybe_command| async move {
            let full_command = maybe_command.unwrap();
            let split_commands = shlex::split(&full_command).unwrap();
            let mut command_builder = Command::new(&split_commands[0]);
            command_builder
                .args(&split_commands[1..])
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::piped());
            let mut child_process = match command_builder.spawn() {
                Ok(child_process) => child_process,
                Err(e) => {
                    println!("Could not run command '{}'", full_command);
                    panic!("{}", e);
                }
            };
            let (tx, rx) = channel(args.buffer_size / CHUNK_BUFFER_SIZE);
            tokio::spawn(async move {
                let mut stdout = child_process.stdout.take().unwrap();
                loop {
                    let mut buffer = BytesMut::with_capacity(CHUNK_BUFFER_SIZE);
                    unsafe {
                        buffer.set_len(CHUNK_BUFFER_SIZE);
                    }
                    let mut bytes_read = 0;
                    while bytes_read < CHUNK_BUFFER_SIZE {
                        let len = stdout
                            .read(&mut buffer[bytes_read..CHUNK_BUFFER_SIZE])
                            .await
                            .unwrap();
                        if len == 0 {
                            break;
                        }
                        bytes_read += len;
                    }
                    buffer.truncate(bytes_read);
                    if bytes_read == 0 {
                        let exit_code = child_process.wait().await.unwrap();
                        assert!(exit_code.success());
                        break;
                    }
                    tx.send(buffer).await.unwrap();
                }
            });
            rx
        })
        .buffered(args.parallel_count);

    let mut stdout = stdout();
    while let Some(mut rx) = process_stream.next().await {
        while let Some(chunk) = rx.recv().await {
            stdout.write_all(&chunk).await?;
        }
    }
    Ok(())
}
