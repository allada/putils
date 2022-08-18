use log::{error, info, warn};
use std::io::Result;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use env_logger::Env;
use fuse_backend_rs::api::{server::Server, Vfs, VfsOptions};
use fuse_backend_rs::passthrough::{Config, PassthroughFs};
use fuse_backend_rs::transport::{FuseChannel, FuseSession};

use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};

/// A fusedev daemon example
pub struct Daemon {
    mountpoint: String,
    server: Arc<Server<Arc<Vfs>>>,
    thread_cnt: u32,
    session: Option<FuseSession>,
}

impl Daemon {
    /// Creates a fusedev daemon instance
    pub fn new(src: &str, mountpoint: &str, thread_cnt: u32) -> Result<Self> {
        // create vfs
        let vfs = Vfs::new(VfsOptions::default());

        // create passthrough fs
        let mut cfg = Config::default();
        cfg.root_dir = src.to_string();
        cfg.do_import = false;
        let fs = PassthroughFs::<()>::new(cfg).unwrap();
        fs.import().unwrap();

        // attach passthrough fs to vfs root
        vfs.mount(Box::new(fs), "/").unwrap();

        Ok(Daemon {
            mountpoint: mountpoint.to_string(),
            server: Arc::new(Server::new(Arc::new(vfs))),
            thread_cnt,
            session: None,
        })
    }

    /// Mounts a fusedev daemon to the mountpoint, then start service threads to handle
    /// FUSE requests.
    pub fn mount(&mut self) -> Result<Vec<thread::JoinHandle<()>>> {
        let mut se =
            FuseSession::new(Path::new(&self.mountpoint), "passthru_example", "", false).unwrap();
        se.mount().unwrap();
        let mut threads = Vec::with_capacity(self.thread_cnt as usize);
        for _ in 0..self.thread_cnt {
            let mut server = FuseServer {
                server: self.server.clone(),
                ch: se.new_channel().unwrap(),
            };
            let thread = thread::Builder::new()
                .name("fuse_server".to_string())
                .spawn(move || {
                    info!("new fuse thread");
                    let _ = server.svc_loop();
                    warn!("fuse service thread exits");
                })
                .unwrap();
            threads.push(thread);
        }
        self.session = Some(se);
        Ok(threads)
    }

    /// Umounts and destroies a fusedev daemon
    pub fn umount(&mut self) -> Result<()> {
        if let Some(mut se) = self.session.take() {
            se.umount().unwrap();
            se.wake().unwrap();
        }
        Ok(())
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.umount();
    }
}

struct FuseServer {
    server: Arc<Server<Arc<Vfs>>>,
    ch: FuseChannel,
}

impl FuseServer {
    fn svc_loop(&mut self) -> Result<()> {
        // Given error EBADF, it means kernel has shut down this session.
        let _ebadf = std::io::Error::from_raw_os_error(libc::EBADF);
        loop {
            if let Some((reader, writer)) = self
                .ch
                .get_request()
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?
            {
                let resp = self.server.handle_message(reader, writer.into(), None, None);
                if let Err(e) = resp {
                    match e {
                        fuse_backend_rs::Error::EncodeMessage(_ebadf) => {
                            break;
                        }
                        _ => {
                            error!("Handling fuse message failed");
                            continue;
                        }
                    }
                }
            } else {
                info!("fuse server exits");
                break;
            }
        }
        Ok(())
    }
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut daemon = Daemon::new("/test_data".into(), "/fuse_test".into(), 2).unwrap();
    let threads = daemon.mount().unwrap();
    let mut signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    thread::spawn(move || {
        for _ in signals.forever() {
            daemon.umount().unwrap();
        }
    });
    for thread in threads {
        thread.join().unwrap();
    }
}