// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader, Read, IoSlice, Write};
use std::os::unix::prelude::AsRawFd;
use std::process::{Command, Stdio};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::{spawn, JoinHandle};

use async_std::task;
use clap::Parser;
use futures::{future::ready, stream, StreamExt};
use nix::fcntl::{vmsplice, SpliceFFlags};
use shlex;
use lazy_static::lazy_static;

mod aligned_buffer_pool;
use crate::aligned_buffer_pool::{AlignedBufferPool, Buffer};

const DEFAULT_CONCURRENT_LIMIT: usize = 16;
const DEFAULT_BUFFER_SIZE: usize = 1 << 30; // 1Gb

/// Number of cooldown buffers. We share some buffers with the kernel when we pipe data to another
/// process and we don't get any notifications telling us when it's been used, so we need to rely
/// on the blocking action of the kernel calls and the known size of the internal kernel pipe
/// buffers. To simplify this we keep items in a cooldown queue to prevent us from writing to it
/// while the kernel might be reading from it.
const MAX_COOLDOWN_BUFFERS: usize = 4;

const TWO_MB_IN_BITS: usize = 21;
const TWO_MB: usize = 1 << TWO_MB_IN_BITS;
lazy_static! {
    static ref BUFFER_POOL: AlignedBufferPool<TWO_MB_IN_BITS> = AlignedBufferPool::<TWO_MB_IN_BITS>::new(MAX_COOLDOWN_BUFFERS);
}

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

fn collect_and_write_data<S, F, const BIT_ALIGNMENT: usize>(mut process_stream: S, mut write_fn: F)
where
    S: futures::Stream<Item = (Receiver<Buffer<BIT_ALIGNMENT>>, JoinHandle<()>)> + std::marker::Unpin,
    F: FnMut(Buffer<BIT_ALIGNMENT>),
{
    task::block_on(async {
        while let Some((rx, thread_join)) = process_stream.next().await {
            while let Ok(chunk) = rx.recv() {
                (write_fn)(chunk);
            }
            thread_join.join().unwrap();
        }
    })
}

fn main() {
    let args = Args::parse();

    let stdin = stdin().lock();
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
                    eprintln!("Could not run command '{}'", full_command);
                    panic!("{}", e);
                }
            };
            let (tx, rx) = sync_channel(args.buffer_size / TWO_MB);

            let thread_join = spawn(move || {
                let mut stdout = child_process.stdout.take().unwrap();
                loop {
                    let mut buffer = BUFFER_POOL.get();
                    unsafe {
                        buffer.set_len(buffer.capacity());
                    }
                    let mut bytes_read = 0;
                    while bytes_read <= TWO_MB {
                        let len = stdout
                            .read(&mut buffer[bytes_read..TWO_MB])
                            .unwrap();
                        if len == 0 {
                            break;
                        }
                        bytes_read += len;
                    }
                    unsafe {
                        buffer.set_len(bytes_read);
                    }
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
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(output_file)
            .unwrap();
        collect_and_write_data(process_stream, move |chunk| file.write_all(&chunk).unwrap());
    } else {
        let stdout = stdout().lock();
        // This is an optimization to make writing to a pipe significantly faster.
        collect_and_write_data(process_stream, move |chunk| {
            let chunk_size = chunk.len();
            let mut bytes_written = 0;
            while bytes_written < chunk_size {
                let iov = [IoSlice::new(&chunk[bytes_written..])];
                match vmsplice(stdout.as_raw_fd(), &iov, SpliceFFlags::SPLICE_F_GIFT) {
                    Ok(sz) => bytes_written += sz,
                    Err(e) => panic!("{}", e),
                }
            }
        });
    }
}
