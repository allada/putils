// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::fs::OpenOptions;
use std::io::{stdin, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::sleep;
use std::thread::{spawn, JoinHandle};
use std::time::Duration;

use clap::Parser;
use semaphore::{Semaphore, SemaphoreGuard};
use shlex;

const CHUNK_BUFFER_SIZE: usize = 2 * 1024 * 1024; // 2mb

const DEFAULT_CONCURRENT_LIMIT: usize = 16;
const DEFAULT_BYTES_PER_SPLIT: u64 = 1 << 30; // 1Gb

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Input file to split. If not set, uses stdin.
    #[clap(short, long, value_parser)]
    input_file: Option<String>,

    /// Number of bytes per output file
    #[clap(short, long, value_parser, default_value_t = DEFAULT_BYTES_PER_SPLIT)]
    bytes: u64,

    /// Number of commands allowed to run in parallel
    #[clap(short, long, value_parser, default_value_t = DEFAULT_CONCURRENT_LIMIT)]
    parallel_count: usize,

    /// Command to run on each output. An environmental variable "SEQ" will be
    /// set containing the sequence number of the slice. Data will be sent to stdin of command.
    #[clap(value_parser)]
    output_command: String,
}

fn spawn_child(full_command: &str, seq: u64) -> Child {
    let split_commands = shlex::split(full_command).unwrap();
    let mut command_builder = Command::new(&split_commands[0]);
    command_builder
        .args(&split_commands[1..])
        .env("SEQ", format!("{}", seq))
        .stdin(Stdio::piped());
    let child_process = match command_builder.spawn() {
        Ok(child_process) => child_process,
        Err(e) => {
            println!("Could not run command '{}'", full_command);
            panic!("{}", e);
        }
    };
    child_process
}

fn create_reader_thread(
    rx: Receiver<Vec<u8>>,
    output_command: String,
    seq: u64,
    permit: SemaphoreGuard<()>,
) -> JoinHandle<()> {
    spawn(move || {
        let _permit = permit;
        let mut chunk = rx.recv().unwrap();
        if chunk.len() == 0 {
            // This is a fence post protection. There is a chance that if our data is exactly
            // our chunk size we might create a process with no data to send it. This protects
            // us by not spawning a process if our first chunk is an EOF.
            return;
        }
        let mut child_process = spawn_child(&output_command, seq);
        let mut child_stdin = child_process.stdin.take().unwrap();
        loop {
            child_stdin.write_all(&chunk).unwrap();
            chunk = rx.recv().unwrap();
            if chunk.len() == 0 {
                break; // EOF.
            }
        }
        drop(child_stdin);
        child_process.wait().unwrap();
    })
}

fn semaphore_block(semaphore: &mut Semaphore<()>) -> SemaphoreGuard<()> {
    loop {
        match semaphore.try_access() {
            Ok(permit) => return permit,
            // TODO(allada) This is a very lazy implementation. We should use a conditional variable.
            _ => sleep(Duration::from_millis(1)),
        };
    }
}

fn split_input(args: Args, fd: impl Read) {
    let mut slice_reader = fd.take(args.bytes);
    let mut semaphore = Semaphore::new(args.parallel_count, ());
    let mut thread_handles = Vec::<Option<JoinHandle<()>>>::with_capacity(args.parallel_count);
    let mut seq = 0;
    loop {
        // Wait for one of a slot to become available. This limits the number of threads
        // at a given time.
        let permit = semaphore_block(&mut semaphore);

        // Cleanup any threads that have finished.
        thread_handles.retain_mut(|handle| {
            if let Some(handle_ref) = handle {
                if !handle_ref.is_finished() {
                    return true;
                }
                handle.take().unwrap().join().unwrap();
            }
            return false;
        });

        // Prepare our new thread.
        let chan_size = cmp::max(10, (args.bytes as usize / CHUNK_BUFFER_SIZE) + 1);
        let (tx, rx) = sync_channel(chan_size);
        thread_handles.push(Some(create_reader_thread(
            rx,
            args.output_command.clone(),
            seq,
            permit,
        )));
        seq += 1;

        // Pump data into our channel for our thread to consume.
        let mut total_bytes_read = 0;
        loop {
            let mut buffer = Vec::with_capacity(CHUNK_BUFFER_SIZE);
            unsafe {
                buffer.set_len(CHUNK_BUFFER_SIZE);
            }
            let mut bytes_read = 0;
            while bytes_read <= CHUNK_BUFFER_SIZE {
                let len = slice_reader
                    .read(&mut buffer[bytes_read..CHUNK_BUFFER_SIZE])
                    .unwrap();
                if len == 0 {
                    break;
                }
                bytes_read += len;
                total_bytes_read += len;
            }
            buffer.truncate(bytes_read);
            // Ensure we send our EOF.
            tx.send(buffer).unwrap();
            if bytes_read == 0 {
                break; // EOF.
            }
        }
        if total_bytes_read < args.bytes as usize {
            // Note: There's a fence post here. If the stream input is an exact multiple of
            // `args.bytes` we may have a thread with zero bytes received total.
            break; // Our entire stream is done.
        }

        // Grab a new slice of `args.bytes`. This makes the next iteration stop when N bytes are
        // received.
        slice_reader = slice_reader.into_inner().take(args.bytes);
    }

    for handle in thread_handles {
        handle.unwrap().join().unwrap();
    }
}

fn main() {
    let args = Args::parse();

    if let Some(input_file) = &args.input_file {
        let fd = OpenOptions::new().read(true).open(input_file).unwrap();
        split_input(args, fd);
    } else {
        let stdin = stdin().lock();
        split_input(args, stdin);
    }
}
