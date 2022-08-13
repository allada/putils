// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use bytes::Bytes;
use bytes::BytesMut;
use zstd::stream::raw::Operation;
use std::io::Read;

use aws_sdk_s3::presigning::config::PresigningConfig;
use aws_config::SdkConfig;
use aws_sdk_s3::RetryConfig;
use std::time::Duration;
use std::fs::OpenOptions;
use std::io::{stdin, stdout, BufRead, BufReader};
use std::io::{IoSlice, Write};
use std::os::unix::prelude::AsRawFd;
use std::sync::mpsc::{sync_channel, Receiver};



use clap::Parser;
use futures::future::ready;
use futures::{stream, StreamExt};
// use bytes::Bytes;
use nix::fcntl::{vmsplice, SpliceFFlags};
use tokio::runtime;



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

async fn collect_and_write_data<S, F>(mut process_stream: S, mut write_fn: F)
where
    S: futures::Stream<Item = Receiver<Bytes>> + std::marker::Unpin,
    F: FnMut(Bytes),
{
    while let Some(rx) = process_stream.next().await {
        while let Ok(chunk) = rx.recv() {
            // for chunk in chunks {
                (write_fn)(chunk);
            // }
        }
    }
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

    let rt = runtime::Builder::new_multi_thread()
        // .worker_threads(args.parallel_count)
        .enable_all()
        .build().unwrap();
    rt.block_on(async move {
        let shared_config = aws_config::load_from_env().await;

        let stdin = stdin().lock();
        let stdin_buf = BufReader::new(stdin);
        let process_stream = stream::iter(stdin_buf.lines())
            .map(move |maybe_s3_path| {
                let s3_path = maybe_s3_path.unwrap();

                let (tx, rx) = sync_channel(args.buffer_size / CHUNK_BUFFER_SIZE);

                let shared_config = shared_config.clone();
                // let rt = runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
                tokio::spawn(async move {
                    
                    
                    

                    let (bucket, key) = s3_path.split_once('/').expect("Bucket was not present in s3 path");

                    let s3_client = get_client(&shared_config);


                    let get_object_builder = s3_client
                        .get_object()
                        .bucket(bucket)
                        .key(key)
                        // .request_payer(RequestPayer::Requester)
                        ;
                    let get_object_builder1 = get_object_builder.clone();
                    let signed_req = get_object_builder1.presigned(
                        PresigningConfig::expires_in(Duration::from_secs(1200)).unwrap()
                    ).await.unwrap();

                    let key = key.to_string();
                    eprintln!("{}", key);

                    tokio::task::spawn_blocking(move || {
                        let mut uri: String = format!("{}", signed_req.uri());
                        uri = uri.replace("https://", "http://");
                        let resp = ureq::get(&uri).call().unwrap();
                        let mut reader = resp.into_reader();

                        let mut decoder = zstd::stream::raw::Decoder::new().unwrap();

                        let mut buffer = BytesMut::with_capacity(CHUNK_BUFFER_SIZE);
                        loop {
                            unsafe {
                                buffer.set_len(CHUNK_BUFFER_SIZE);
                            }
                            let mut bytes_read = 0;
                            while bytes_read < CHUNK_BUFFER_SIZE {
                                let len = reader
                                    .read(&mut buffer[bytes_read..CHUNK_BUFFER_SIZE])
                                    .unwrap();
                                if len == 0 {
                                    break;
                                }
                                bytes_read += len;
                            }
                            if bytes_read == 0 {
                                break; // EOF.
                            }
                            buffer.truncate(bytes_read);

                            // let mut outputs = vec![];
                            const MIN_BUFFER_SIZE: usize = CHUNK_BUFFER_SIZE * 4 / 16;
                            let mut output = BytesMut::with_capacity(CHUNK_BUFFER_SIZE * 10);
                            unsafe {
                                output.set_len(output.capacity());
                            }
                            let mut total_output_bytes = 0;
                            let mut bytes_read = 0;
                            // let i = 0;
                            loop {
                                // if buffer.len() != 1 {
                                //     break;
                                // }
                                let status = decoder.run_on_buffers(
                                    &buffer[bytes_read..],
                                    &mut output[total_output_bytes..]
                                ).unwrap();
                                bytes_read += status.bytes_read;
                                // eprintln!("remaining {}", status.remaining);
                                // eprintln!("bytes_read {}", status.bytes_read);
                                // eprintln!("-- i {}", i);
                                if status.bytes_read == 0 {
                                    assert!(status.bytes_written == 0);
                                    break;
                                }
                                // let _ = buffer.split_to(status.bytes_read);
                                total_output_bytes += status.bytes_written;
                                if total_output_bytes + MIN_BUFFER_SIZE > output.len() {
                                    output.truncate(total_output_bytes);
                                    // tx.send(output.freeze()).unwrap();
                                    output = BytesMut::with_capacity(CHUNK_BUFFER_SIZE);
                                    unsafe {
                                        output.set_len(output.capacity());
                                    }
                                    total_output_bytes = 0;
                                }
                            }
                            output.truncate(total_output_bytes);
                            
                            if buffer.len() == 1 {
                                tx.send(output.freeze()).unwrap();
                            }
                            buffer.reserve(CHUNK_BUFFER_SIZE - buffer.capacity());
                            // mmap_file_ref.flush_async().unwrap();
                        }
                        eprintln!("Done {}", key);
                    });
                });
                ready(rx)
            })
            .buffer_unordered(args.parallel_count);

        if let Some(output_file) = args.output_file {
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(output_file)
                .unwrap();
            collect_and_write_data(process_stream, move |chunk| file.write_all(&chunk).unwrap()).await;
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
            }).await;
        }
    });
}
