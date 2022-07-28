// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::{spawn, JoinHandle};

use async_std::task;
use clap::Parser;
use futures::future::ready;
use futures::{stream, StreamExt};
use shlex;

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

    /// Size in bytes of the stdout buffer for reach command
    #[clap(short, long, value_parser, default_value_t = DEFAULT_BUFFER_SIZE)]
    buffer_size: usize,

    /// Path to write file. Prints to stdout if not set. Using a file can be faster than stdout
    #[clap(value_parser)]
    output_file: Option<String>,
}

fn collect_and_write_data<S, T>(mut process_stream: S, mut writer: T)
where
    S: futures::Stream<Item = (Receiver<Vec<u8>>, JoinHandle<()>)> + std::marker::Unpin,
    T: Write,
{
    task::block_on(async {
        while let Some((rx, thread_join)) = process_stream.next().await {
            while let Ok(chunk) = rx.recv() {
                writer.write_all(&chunk).unwrap();
            }
            thread_join.join().unwrap();
        }
    })
}

fn main() {
    let args = Args::parse();

    let stdin = stdin();
    let stdin_buf = BufReader::new(stdin);
    let process_stream = stream::iter(stdin_buf.lines())
        .map(move |maybe_command| {
            let full_command = maybe_command.unwrap();
            let split_commands = shlex::split(&full_command).unwrap();
            let mut command_builder = Command::new(&split_commands[0]);
            command_builder
                .args(&split_commands[1..])
                .stdin(Stdio::null())
                .stdout(Stdio::piped());
            let mut child_process = match command_builder.spawn() {
                Ok(child_process) => child_process,
                Err(e) => {
                    println!("Could not run command '{}'", full_command);
                    panic!("{}", e);
                }
            };
            let (tx, rx) = sync_channel(args.buffer_size / CHUNK_BUFFER_SIZE);

            let thread_join = spawn(move || {
                let mut stdout = child_process.stdout.take().unwrap();
                loop {
                    let mut buffer = Vec::with_capacity(CHUNK_BUFFER_SIZE);
                    unsafe {
                        buffer.set_len(CHUNK_BUFFER_SIZE);
                    }
                    let mut bytes_read = 0;
                    while bytes_read <= CHUNK_BUFFER_SIZE {
                        let len = stdout
                            .read(&mut buffer[bytes_read..CHUNK_BUFFER_SIZE])
                            .unwrap();
                        if len == 0 {
                            break;
                        }
                        bytes_read += len;
                    }
                    buffer.truncate(bytes_read);
                    if bytes_read == 0 {
                        let exit_code = child_process.wait().unwrap();
                        assert!(exit_code.success());
                        break;
                    }
                    tx.send(buffer).unwrap();
                }
            });
            ready((rx, thread_join))
        })
        .buffered(args.parallel_count);

    if let Some(output_file) = args.output_file {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(output_file)
            .unwrap();
        collect_and_write_data(process_stream, file);
    } else {
        collect_and_write_data(process_stream, stdout().lock());
    }
}
