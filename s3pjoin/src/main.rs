// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use aws_config::SdkConfig;
use aws_sdk_s3::RetryConfig;
use std::time::Duration;
use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader};
use std::io::{IoSlice, Write};
use std::os::unix::prelude::AsRawFd;
use std::sync::mpsc::{sync_channel, Receiver};

use aws_sdk_s3::model::RequestPayer;
use async_std::task;
use clap::Parser;
use futures::future::ready;
use futures::{stream, StreamExt};
// use bytes::Bytes;
use nix::fcntl::{vmsplice, SpliceFFlags};
use tokio::runtime;
use tokio_util::io::StreamReader;
use tokio_util::io::SyncIoBridge;

const DEFAULT_CONCURRENT_LIMIT: usize = 16;
const DEFAULT_BUFFER_SIZE: usize = 1 << 30; // 1Gb

const CHUNK_BUFFER_SIZE: usize = 2 * 1024 * 1024; // 2mb

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

fn collect_and_write_data<S, F>(mut process_stream: S, mut write_fn: F)
where
    S: futures::Stream<Item = (Receiver<Vec<u8>>, runtime::Runtime)> + std::marker::Unpin,
    F: FnMut(Vec<u8>),
{
    task::block_on(async {
        while let Some((rx, _rt)) = process_stream.next().await {
            while let Ok(chunk) = rx.recv() {
                // for chunk in chunks {
                    (write_fn)(chunk);
                // }
            }
        }
    })
}

fn get_client(config: &SdkConfig) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::from(config)
        .retry_config(
            RetryConfig::new()
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_attempts(8) // This has 25.5s time limit.
        )
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

fn main() {
    let args = Args::parse();

    let rt = runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let shared_config = rt.block_on(async {
        aws_config::load_from_env().await
    });
    drop(rt);

    let stdin = stdin().lock();
    let stdin_buf = BufReader::new(stdin);
    let process_stream = stream::iter(stdin_buf.lines())
        .map(move |maybe_s3_path| {
            let s3_path = maybe_s3_path.unwrap();

            let (tx, rx) = sync_channel(args.buffer_size / CHUNK_BUFFER_SIZE);

            let shared_config = shared_config.clone();
            let rt = runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
            rt.block_on(async move {
                // use tokio_stream::StreamExt;
                use std::io::Read;
                use zstd::stream::read::Decoder;

                let (bucket, key) = s3_path.split_once('/').expect("Bucket was not present in s3 path");

                let s3_client = get_client(&shared_config);
                let get_object_builder = s3_client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .request_payer(RequestPayer::Requester);

                // let mut byte_slices = vec![];
                let response = get_object_builder.send().await.expect(&format!("Failed at {}", s3_path));
                let reader = SyncIoBridge::new(StreamReader::new(response.body));
                let mut decoder = Decoder::new(BufReader::with_capacity(CHUNK_BUFFER_SIZE, reader)).unwrap();

                tokio::task::spawn_blocking(move || {
                    loop {
                        let mut buffer = Vec::with_capacity(CHUNK_BUFFER_SIZE);
                        unsafe {
                            buffer.set_len(CHUNK_BUFFER_SIZE);
                        }
                        let mut bytes_read = 0;
                        while bytes_read < CHUNK_BUFFER_SIZE {
                            let len = decoder
                                .read(&mut buffer[bytes_read..CHUNK_BUFFER_SIZE])
                                .unwrap();
                            if len == 0 {
                                break;
                            }
                            bytes_read += len;
                        }
                        buffer.truncate(bytes_read);
                        if bytes_read == 0 {
                            break; // EOF.
                        }
                        tx.send(buffer).unwrap();
                    }
                });






                // while let Some(bytes) = response.body.try_next().await.unwrap() {
                //     if bytes_read >= CHUNK_BUFFER_SIZE {
                //         tx.send(byte_slices).unwrap();
                //         byte_slices = vec![];
                //         bytes_read = 0;
                //     }
                //     bytes_read += bytes.len();
                //     byte_slices.push(bytes);
                // }
                // tx.send(byte_slices).unwrap();
            });
            ready((rx, rt))
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
