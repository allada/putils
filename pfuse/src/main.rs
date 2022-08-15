#![warn(clippy::unimplemented, clippy::todo)]

use std::os::unix::prelude::RawFd;
use core::sync::atomic::AtomicU64;
use tokio::io::AsyncWriteExt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use fast_async_mutex::mutex::MutexGuard;
// use std::io::Read;
// use std::io::Seek;
mod fs;
use std::sync::Weak;
use std::os::unix::fs::MetadataExt;

use fast_async_mutex::mutex::Mutex;
use polyfuse::{
    op,
    reply::{
        AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut, Statfs, StatfsOut, WriteOut, XattrOut,
    },
    KernelConfig, Operation, Session,
};

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use anyhow::Result;
use either::Either;
// use pico_args::Arguments;
use slab::Slab;
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::{OsStr, OsString},
    fmt::Debug,
    io::{self, BufRead},
    path::PathBuf,
    // sync::{Arc, Mutex},
    time::Duration,
};

use crate::fs::{FileDesc, ReadDir};

const SLICE_MASK: usize = 14; // 16k.

const INODE_INDEX_FOLDER: &str = "/erigon_data_clone/inode_indexs";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mountpoint: PathBuf = "/fuse_erigon_data/chaindata".into();
    let source: PathBuf = "/erigon_data_clone/chaindata".into();

    // TODO: splice read/write
    let session = Session::mount(mountpoint, {
        let mut config = KernelConfig::default();
        config.mount_option("default_permissions");
        config.mount_option("fsname=passthrough");
        config.export_support(true);
        config.flock_locks(true);
        config
    })?;

    let fs = Arc::new(Passthrough::new(source, INODE_INDEX_FOLDER.into())?);

    while let Some(req) = session.next_request()? {
        let fs = fs.clone();
        tokio::spawn(async move {
            let span = tracing::debug_span!("handle_request", unique = req.unique());
            let _enter = span.enter();

            let op = req.operation().unwrap();
            tracing::debug!(?op);

            macro_rules! try_reply {
                ($e:expr) => {
                    match $e {
                        Ok(data) => {
                            tracing::debug!(?data);
                            req.reply(data).unwrap();
                        }
                        Err(err) => {
                            let errno = io_to_errno(err);
                            tracing::debug!(errno = errno);
                            req.reply_error(errno).unwrap();
                        }
                    }
                };
            }

            match op {
                Operation::Lookup(op) => try_reply!(fs.do_lookup(op.parent(), op.name()).await),
                Operation::Forget(forgets) => {
                    for forget in forgets.as_ref() {
                        fs.forget_one(forget.ino(), forget.nlookup());
                    }
                }
                Operation::Getattr(op) => try_reply!(fs.do_getattr(&op).await),
                Operation::Setattr(op) => try_reply!(fs.do_setattr(&op).await),
                Operation::Readlink(op) => try_reply!(fs.do_readlink(&op).await),
                Operation::Link(op) => try_reply!(fs.do_link(&op).await),

                Operation::Mknod(op) => {
                    try_reply!(fs.make_node(
                        op.parent(),
                        op.name(),
                        op.mode(),
                        Some(op.rdev()),
                        None
                    ).await)
                }
                Operation::Mkdir(op) => try_reply!(fs.make_node(
                    op.parent(),
                    op.name(),
                    libc::S_IFDIR | op.mode(),
                    None,
                    None
                ).await),
                Operation::Symlink(op) => try_reply!(fs.make_node(
                    op.parent(),
                    op.name(),
                    libc::S_IFLNK,
                    None,
                    Some(op.link())
                ).await),

                Operation::Unlink(op) => try_reply!(fs.do_unlink(&op).await),
                Operation::Rmdir(op) => try_reply!(fs.do_rmdir(&op).await),
                Operation::Rename(op) => try_reply!(fs.do_rename(&op).await),

                Operation::Opendir(op) => try_reply!(fs.do_opendir(&op).await),
                Operation::Readdir(op) => try_reply!(fs.do_readdir(&op).await),
                Operation::Fsyncdir(op) => try_reply!(fs.do_fsyncdir(&op).await),
                Operation::Releasedir(op) => try_reply!(fs.do_releasedir(&op).await),

                Operation::Open(op) => try_reply!(fs.do_open(&op).await),
                Operation::Read(op) => try_reply!(fs.do_read(&op).await),
                Operation::Write(op, data) => try_reply!(fs.do_write(&op, data).await),
                Operation::Flush(op) => try_reply!(fs.do_flush(&op).await),
                Operation::Fsync(op) => try_reply!(fs.do_fsync(&op).await),
                Operation::Flock(op) => try_reply!(fs.do_flock(&op).await),
                Operation::Fallocate(op) => try_reply!(fs.do_fallocate(&op).await),
                Operation::Release(op) => try_reply!(fs.do_release(&op).await),

                Operation::Getxattr(op) => try_reply!(fs.do_getxattr(&op).await),
                Operation::Listxattr(op) => try_reply!(fs.do_listxattr(&op).await),
                Operation::Setxattr(op) => try_reply!(fs.do_setxattr(&op).await),
                Operation::Removexattr(op) => try_reply!(fs.do_removexattr(&op).await),

                Operation::Statfs(op) => try_reply!(fs.do_statfs(&op).await),

                _ => req.reply_error(libc::ENOSYS).unwrap(),
            }
        });
    }

    Ok(())
}

type Ino = u64;

struct ReadStats {
    ino: Ino,
    parent_map: Weak<Mutex<HashMap<Ino, Weak<ReadStats>>>>,
    stats: Option<Mutex<Vec<u16>>>,
}

impl ReadStats {
    fn new(ino: Ino, parent_map: Weak<Mutex<HashMap<Ino, Weak<ReadStats>>>>, file_size: usize) -> Self {
        println!("Creating");
        let mut stats = Vec::with_capacity(file_size >> SLICE_MASK);
        stats.resize(file_size >> SLICE_MASK, 0);
        Self {
            ino,
            parent_map,
            stats: Some(Mutex::new(stats)),
        }
    }

    async fn touch(self: Arc<Self>, index_start: u64, index_end: u64) {
        let index_start = (index_start >> SLICE_MASK) as usize;
        let index_end = (index_end >> SLICE_MASK) as usize;
        let mut stats = self.stats.as_ref().unwrap().lock().await;
        if stats.len() <= index_end {
            stats.resize(index_end, 0);
        }
        for i in index_start..index_end {
            unsafe {
                *stats.get_unchecked_mut(i) += 1;
            }
        }
    }
}

impl Drop for ReadStats {
    fn drop(&mut self) {
        println!("Dropping");
        let ino = self.ino;
        let map = self.parent_map.upgrade();
        let stats = self.stats.take();
        tokio::spawn(async move {
            let stats = stats.as_ref().unwrap().lock().await;
            if let Some(map) = map {
                map.lock().await.remove(&ino);
            }
            let mut path: PathBuf = INODE_INDEX_FOLDER.into();
            println!("Starting writing {:?}", path);
            path.push(PathBuf::from(format!("{}", ino)));
            let mut file_write = tokio::fs::OpenOptions::new().create(true).write(true).open(path.clone()).await.unwrap();
            let file_read = tokio::fs::OpenOptions::new().read(true).open(path.clone()).await.unwrap();
            let file_size = file_read.metadata().await.unwrap().len();
            file_write.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let mut write_buf = tokio::io::BufWriter::new(file_write);
            let mut read_buf = tokio::io::BufReader::new(file_read);
            for i in 0..stats.len() as u64 {
                let old_value = if file_size >= (i + 1) * std::mem::size_of::<u16>() as u64 {
                    read_buf.read_u16_le().await.unwrap()
                } else {
                    0
                };
                let new_value = old_value + unsafe {
                    *stats.get_unchecked(i as usize)
                };
                if new_value != old_value {
                    write_buf.seek(std::io::SeekFrom::Start(i * std::mem::size_of::<u16>() as u64)).await.unwrap();
                    write_buf.write_u16_le(new_value).await.unwrap();
                }
            }
            println!("Done writing {:?}", path);
        });
    }
}

type SrcId = (u64, libc::dev_t);

struct WrappedFile {
    file: File,
    read_stats: Arc<ReadStats>,
}

impl WrappedFile {
    async fn new(file: File, read_stats: Arc<ReadStats>) -> io::Result<Self> {
        Ok(Self {
            file,
            read_stats,
        })
    }
}

struct Passthrough {
    inodes: INodeTable,
    opened_dirs: HandlePool<ReadDir>,
    // opened_files: HandlePool<WrappedFile>,
    file_stats_map: Arc<Mutex<HashMap<Ino, Weak<ReadStats>>>>,

    open_files: Mutex<HashMap<RawFd, File>>,
}

impl Passthrough {
    fn new(source: PathBuf, inode_index_folder: PathBuf) -> io::Result<Self> {
        let source = source.canonicalize()?;
        tracing::debug!("source={:?}", source);
        let fd = FileDesc::open(&source, libc::O_PATH)?;
        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut inodes = INodeTable::new();
        let entry = inodes.vacant_entry();
        debug_assert_eq!(entry.ino(), 1);
        entry.insert(INode {
            ino: 1,
            fd,
            refcount: AtomicU64::new(u64::max_value() / 2), // the root node's cache is never removed.
            src_id: (stat.st_ino, stat.st_dev),
            is_symlink: false,
        });

        Ok(Self {
            inodes,
            opened_dirs: HandlePool::new(inode_index_folder.clone()),
            // opened_files: HandlePool::new(inode_index_folder),
            file_stats_map: Arc::new(Mutex::new(HashMap::new())),
            open_files: Mutex::new(HashMap::new()),
        })
    }

    async fn do_lookup(&self, parent: Ino, name: &OsStr) -> io::Result<EntryOut> {
        let parent = self.inodes.get(parent).ok_or_else(no_entry)?;

        let fd = parent.fd.openat(name, libc::O_PATH | libc::O_NOFOLLOW)?;

        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;
        let src_id = (stat.st_ino, stat.st_dev);
        let is_symlink = stat.st_mode & libc::S_IFMT == libc::S_IFLNK;

        let ino;
        match self.inodes.get_src(src_id) {
            Some(inode) => {
                ino = inode.ino;
                let refcount = inode.refcount.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "update the lookup count: ino={}, refcount={}",
                    inode.ino,
                    refcount
                );
            }
            None => {
                let entry = self.inodes.vacant_entry();
                ino = entry.ino();
                tracing::debug!("create a new inode cache: ino={}", ino);
                entry.insert(INode {
                    ino,
                    fd,
                    refcount: AtomicU64::new(1),
                    src_id,
                    is_symlink,
                });
            }
        }

        Ok(make_entry_param(ino, stat))
    }

    fn forget_one(&self, ino: Ino, nlookup: u64) {
        if let Entry::Occupied(mut entry) = self.inodes.map.entry(ino) {
            let refcount = {
                let mut inode = entry.get_mut();
                let old_value = inode.refcount.fetch_sub(nlookup, Ordering::Relaxed);
                old_value + nlookup
            };

            if refcount == 0 {
                tracing::debug!("remove ino={}", entry.key());
                entry.remove();
            }
        }
    }

    async fn do_getattr(&self, op: &op::Getattr<'_>) -> io::Result<AttrOut> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        let stat = inode.fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut out = AttrOut::default();
        fill_attr(out.attr(), &stat);

        Ok(out)
    }

    #[allow(clippy::cognitive_complexity)]
    async fn do_setattr(&self, op: &op::Setattr<'_>) -> io::Result<AttrOut> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;
        let fd = &inode.fd;

        let mut wrapped_file = if let Some(fh) = op.fh() {
            Some(self.opened_files.get(fh).await.ok_or_else(no_entry)?)
        } else {
            None
        };
        let mut wrapped_file = if let Some(ref mut wrapped_file) = wrapped_file {
            Some(wrapped_file)
        } else {
            None
        };

        // chmod
        if let Some(mode) = op.mode() {
            if let Some(wrapped_file) = wrapped_file.as_mut() {
                fs::fchmod(&wrapped_file.file, mode)?;
            } else {
                fs::chmod(fd.procname(), mode)?;
            }
        }

        // chown
        match (op.uid(), op.gid()) {
            (None, None) => (),
            (uid, gid) => {
                fd.fchownat("", uid, gid, libc::AT_SYMLINK_NOFOLLOW)?;
            }
        }

        // truncate
        if let Some(size) = op.size() {
            if let Some(wrapped_file) = wrapped_file.as_mut() {
                fs::ftruncate(&wrapped_file.file, size as libc::off_t)?;
            } else {
                fs::truncate(fd.procname(), size as libc::off_t)?;
            }
        }

        // utimens
        fn make_timespec(t: Option<op::SetAttrTime>) -> libc::timespec {
            match t {
                Some(op::SetAttrTime::Now) => libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_NOW,
                },
                Some(op::SetAttrTime::Timespec(ts)) => libc::timespec {
                    tv_sec: ts.as_secs() as i64,
                    tv_nsec: ts.subsec_nanos() as u64 as i64,
                },
                _ => libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
            }
        }
        match (op.atime(), op.mtime()) {
            (None, None) => (),
            (atime, mtime) => {
                let tv = [make_timespec(atime), make_timespec(mtime)];
                if let Some(wrapped_file) = wrapped_file.as_mut() {
                    fs::futimens(&wrapped_file.file, tv)?;
                } else if inode.is_symlink {
                    // According to libfuse/examples/passthrough_hp.cc, it does not work on
                    // the current kernels, but may in the future.
                    fd.futimensat("", tv, libc::AT_SYMLINK_NOFOLLOW)
                        .map_err(|err| match err.raw_os_error() {
                            Some(libc::EINVAL) => io::Error::from_raw_os_error(libc::EPERM),
                            _ => err,
                        })?;
                } else {
                    fs::utimens(fd.procname(), tv)?;
                }
            }
        }

        // finally, acquiring the latest metadata from the source filesystem.
        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut out = AttrOut::default();
        fill_attr(out.attr(), &stat);

        Ok(out)
    }

    async fn do_readlink(&self, op: &op::Readlink<'_>) -> io::Result<OsString> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;
        inode.fd.readlinkat("")
    }

    async fn do_link(&self, op: &op::Link<'_>) -> io::Result<EntryOut> {
        let parent_fd = self.inodes.get(op.newparent()).ok_or_else(no_entry)?.fd.clone();

        let source = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        if source.is_symlink {
            source
                .fd
                .linkat("", &parent_fd, op.newname(), 0)
                .map_err(|err| match err.raw_os_error() {
                    Some(libc::ENOENT) | Some(libc::EINVAL) => {
                        // no race-free way to hard-link a symlink.
                        io::Error::from_raw_os_error(libc::EOPNOTSUPP)
                    }
                    _ => err,
                })?;
        } else {
            fs::link(
                source.fd.procname(),
                &parent_fd,
                op.newname(),
                libc::AT_SYMLINK_FOLLOW,
            )?;
        }

        let stat = source.fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;
        let entry = make_entry_param(source.ino, stat);

        source.refcount.fetch_add(1, Ordering::Relaxed);

        Ok(entry)
    }

    async fn make_node(
        &mut self,
        parent: Ino,
        name: &OsStr,
        mode: u32,
        rdev: Option<u32>,
        link: Option<&OsStr>,
    ) -> io::Result<EntryOut> {
        {
            let parent = self.inodes.get(parent).ok_or_else(no_entry)?;

            match mode & libc::S_IFMT {
                libc::S_IFDIR => {
                    parent.fd.mkdirat(name, mode)?;
                }
                libc::S_IFLNK => {
                    let link = link.expect("missing 'link'");
                    parent.fd.symlinkat(name, link)?;
                }
                _ => {
                    parent
                        .fd
                        .mknodat(name, mode, rdev.unwrap_or(0) as libc::dev_t)?;
                }
            }
        }
        self.do_lookup(parent, name).await
    }

    async fn do_unlink(&self, op: &op::Unlink<'_>) -> io::Result<()> {
        let parent = self.inodes.get(op.parent()).ok_or_else(no_entry)?;
        parent.fd.unlinkat(op.name(), 0)?;
        Ok(())
    }

    async fn do_rmdir(&self, op: &op::Rmdir<'_>) -> io::Result<()> {
        let parent = self.inodes.get(op.parent()).ok_or_else(no_entry)?;
        parent.fd.unlinkat(op.name(), libc::AT_REMOVEDIR)?;
        Ok(())
    }

    async fn do_rename(&self, op: &op::Rename<'_>) -> io::Result<()> {
        if op.flags() != 0 {
            // rename2 is not supported.
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }

        let parent = self.inodes.get(op.parent()).ok_or_else(no_entry)?;
        let newparent = self.inodes.get(op.newparent()).ok_or_else(no_entry)?;

        if op.parent() == op.newparent() {
            parent
                .fd
                .renameat(op.name(), None::<&FileDesc>, op.newname())?;
        } else {
            parent
                .fd
                .renameat(op.name(), Some(&newparent.fd), op.newname())?;
        }

        Ok(())
    }

    async fn do_opendir(&self, op: &op::Opendir<'_>) -> io::Result<OpenOut> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;
        let dir = inode.fd.read_dir()?;
        let fh = self.opened_dirs.insert(dir).await;

        let mut out = OpenOut::default();
        out.fh(fh);

        Ok(out)
    }

    async fn do_readdir(&self, op: &op::Readdir<'_>) -> io::Result<ReaddirOut> {
        if op.mode() == op::ReaddirMode::Plus {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }

        let mut read_dir = self
            .opened_dirs
            .get(op.fh())
            .await
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        read_dir.seek(op.offset());

        let mut out = ReaddirOut::new(op.size() as usize);
        for entry in &mut *read_dir {
            let entry = entry?;
            if out.entry(&entry.name, entry.ino, entry.typ, entry.off) {
                break;
            }
        }

        Ok(out)
    }

    async fn do_fsyncdir(&self, op: &op::Fsyncdir<'_>) -> io::Result<()> {
        let read_dir = self.opened_dirs.get(op.fh()).await.ok_or_else(no_entry)?;

        if op.datasync() {
            read_dir.sync_data()?;
        } else {
            read_dir.sync_all()?;
        }

        Ok(())
    }

    async fn do_releasedir(&self, op: &op::Releasedir<'_>) -> io::Result<()> {
        self.opened_dirs.remove(op.fh());
        Ok(())
    }

    async fn do_open(&self, op: &op::Open<'_>) -> io::Result<OpenOut> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        let mut options = OpenOptions::new();
        match (op.flags() & 0x03) as i32 {
            libc::O_RDONLY => {
                options.read(true);
            }
            libc::O_WRONLY => {
                options.write(true);
            }
            libc::O_RDWR => {
                options.read(true).write(true);
            }
            _ => (),
        }
        options.custom_flags(op.flags() as i32 & !libc::O_NOFOLLOW);

        let file = options.open(&inode.fd.procname()).await?;
        let read_stats = {
            let mut file_stats_map = self.file_stats_map.lock().await;
            let metadata = file.metadata().await?;
            let ino = metadata.ino();
            let maybe_read_stats = match file_stats_map.get(&ino) {
                Some(weak_stats) => {
                    match weak_stats.upgrade() {
                        None => {
                            file_stats_map.remove(&ino);
                            None
                        }
                        v => v,
                    }
                },
                None => None,
            };
            match maybe_read_stats {
                Some(v) => {
                    println!("Found!");
                    v
                },
                None => {
                    let len = metadata.len() as usize;
                    let file_stats = Arc::new(ReadStats::new(ino, Arc::downgrade(&self.file_stats_map), len));
                    println!("Create!");
                    file_stats_map.insert(ino, Arc::downgrade(&file_stats));
                    file_stats
                }
            }
        };
        // let len = file.metadata().await?.len() as usize;
        let fh = self.opened_files.insert(WrappedFile::new(file, read_stats).await?).await;

        let mut out = OpenOut::default();
        out.fh(fh);

        Ok(out)
    }

    async fn do_read(&self, op: &op::Read<'_>) -> io::Result<Vec<u8>> {
        let mut wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        wrapped_file.file.seek(io::SeekFrom::Start(op.offset())).await?;

        tokio::spawn(wrapped_file.read_stats.clone().touch(op.offset(), op.offset() + op.size() as u64));

        let mut buf = Vec::<u8>::with_capacity(op.size() as usize);
        (&mut wrapped_file.file).take(op.size() as u64).read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn do_write<T>(&self, op: &op::Write<'_>, mut data: T) -> io::Result<WriteOut>
    where
        T: BufRead + Unpin,
    {
        let mut wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        wrapped_file.file.seek(io::SeekFrom::Start(op.offset())).await?;

        // At here, the data is transferred via the temporary buffer due to
        // the incompatibility between the I/O abstraction in `futures` and
        // `tokio`.
        //
        // In order to efficiently transfer the large files, both of zero
        // copying support in `polyfuse` and resolution of impedance mismatch
        // between `futures::io` and `tokio::io` are required.
        let mut buf = Vec::with_capacity(op.size() as usize);
        data.read_to_end(&mut buf)?;

        // let mut buf = std::io::Read::take(&mut buf, op.size() as u64);
        let mut cursor = std::io::Cursor::new(&buf[..op.size() as usize]);
        let written = tokio::io::copy(&mut cursor, &mut wrapped_file.file).await?;

        let mut out = WriteOut::default();
        out.size(written as u32);

        Ok(out)
    }

    async fn do_flush(&self, op: &op::Flush<'_>) -> io::Result<()> {
        let wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        wrapped_file.file.sync_all().await?;

        Ok(())
    }

    async fn do_fsync(&self, op: &op::Fsync<'_>) -> io::Result<()> {
        let wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        if op.datasync() {
            wrapped_file.file.sync_data().await?;
        } else {
            wrapped_file.file.sync_all().await?;
        }

        Ok(())
    }

    async fn do_flock(&self, op: &op::Flock<'_>) -> io::Result<()> {
        let wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        let op = op.op().expect("invalid lock operation") as i32;

        fs::flock(&wrapped_file.file, op)?;

        Ok(())
    }

    async fn do_fallocate(&self, op: &op::Fallocate<'_>) -> io::Result<()> {
        if op.mode() != 0 {
            return Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        }

        let wrapped_file = self.opened_files.get(op.fh()).await.ok_or_else(no_entry)?;

        fs::posix_fallocate(&wrapped_file.file, op.offset() as i64, op.length() as i64)?;

        Ok(())
    }

    async fn do_release(&self, op: &op::Release<'_>) -> io::Result<()> {
        self.opened_files.remove(op.fh());
        Ok(())
    }

    async fn do_getxattr(
        &mut self,
        op: &op::Getxattr<'_>,
    ) -> io::Result<impl polyfuse::bytes::Bytes + Debug> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        match op.size() {
            0 => {
                let size = fs::getxattr(inode.fd.procname(), op.name(), None)?;
                let mut out = XattrOut::default();
                out.size(size as u32);
                Ok(Either::Left(out))
            }
            size => {
                let mut value = vec![0u8; size as usize];
                let n = fs::getxattr(inode.fd.procname(), op.name(), Some(&mut value[..]))?;
                value.resize(n as usize, 0);
                Ok(Either::Right(value))
            }
        }
    }

    async fn do_listxattr(
        &mut self,
        op: &op::Listxattr<'_>,
    ) -> io::Result<impl polyfuse::bytes::Bytes + Debug> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        match op.size() {
            0 => {
                let size = fs::listxattr(inode.fd.procname(), None)?;
                let mut out = XattrOut::default();
                out.size(size as u32);
                Ok(Either::Left(out))
            }
            size => {
                let mut value = vec![0u8; size as usize];
                let n = fs::listxattr(inode.fd.procname(), Some(&mut value[..]))?;
                value.resize(n as usize, 0);
                Ok(Either::Right(value))
            }
        }
    }

    async fn do_setxattr(&self, op: &op::Setxattr<'_>) -> io::Result<()> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        fs::setxattr(
            inode.fd.procname(),
            op.name(),
            op.value(),
            op.flags() as libc::c_int,
        )?;

        Ok(())
    }

    async fn do_removexattr(&self, op: &op::Removexattr<'_>) -> io::Result<()> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        fs::removexattr(inode.fd.procname(), op.name())?;

        Ok(())
    }

    async fn do_statfs(&self, op: &op::Statfs<'_>) -> io::Result<StatfsOut> {
        let inode = self.inodes.get(op.ino()).ok_or_else(no_entry)?;

        let st = fs::fstatvfs(&inode.fd)?;

        let mut out = StatfsOut::default();
        fill_statfs(out.statfs(), &st);

        Ok(out)
    }
}

fn fill_attr(attr: &mut FileAttr, st: &libc::stat) {
    attr.ino(st.st_ino);
    attr.size(st.st_size as u64);
    attr.mode(st.st_mode);
    attr.nlink(st.st_nlink as u32);
    attr.uid(st.st_uid);
    attr.gid(st.st_gid);
    attr.rdev(st.st_rdev as u32);
    attr.blksize(st.st_blksize as u32);
    attr.blocks(st.st_blocks as u64);
    attr.atime(Duration::new(st.st_atime as u64, st.st_atime_nsec as u32));
    attr.mtime(Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32));
    attr.ctime(Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32));
}

fn fill_statfs(statfs: &mut Statfs, st: &libc::statvfs) {
    statfs.bsize(st.f_bsize as u32);
    statfs.frsize(st.f_frsize as u32);
    statfs.blocks(st.f_blocks);
    statfs.bfree(st.f_bfree);
    statfs.bavail(st.f_bavail);
    statfs.files(st.f_files);
    statfs.ffree(st.f_ffree);
    statfs.namelen(st.f_namemax as u32);
}

// ==== HandlePool ====

struct HandlePool<T> {
    slab: Mutex<Slab<Arc<Mutex<T>>>>,
    inode_index_folder: PathBuf,
}

impl<T> HandlePool<T> {
    fn new(inode_index_folder: PathBuf) -> Self {
        Self {
            slab: Default::default(),
            inode_index_folder,
        }
    }

    async fn get(&self, fh: u64) -> Option<Arc<MutexGuard<T>>> {
        let slab = self.slab.lock().await;
        match slab.get(fh as usize) {
            Some(v) => Some(v.lock().await.clone()),
            _ => None
        }
    }

    async fn get_mut(&mut self, fh: u64) -> Option<MutexGuard<T>> {
        let slab = self.slab.lock().await;
        match slab.get_mut(fh as usize) {
            Some(v) => Some(v.lock().await),
            _ => None
        }
    }

    async fn remove(&mut self, fh: u64) {
        self.slab.lock().await.remove(fh as usize);
    }

    async fn insert(&mut self, entry: T) -> u64 {
        self.slab.lock().await.insert(Mutex::new(entry)) as u64
    }
}

// ==== INode ====

struct INode {
    ino: Ino,
    src_id: SrcId,
    is_symlink: bool,
    fd: FileDesc,
    refcount: AtomicU64,
}

// ==== INodeTable ====

struct INodeTable {
    map: HashMap<Ino, INode>,
    src_to_ino: HashMap<SrcId, Ino>,
    next_ino: AtomicU64,
}

impl INodeTable {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            src_to_ino: HashMap::new(),
            next_ino: AtomicU64::new(1), // the ino is started with 1 and the first entry is mapped to the root.
        }
    }

    fn get(&self, ino: Ino) -> Option<&INode> {
        self.map.get(&ino)
    }

    fn get_mut(&mut self, ino: Ino) -> Option<&mut INode> {
        self.map.get_mut(&ino)
    }

    fn get_src(&mut self, src_id: SrcId) -> Option<&mut INode> {
        let ino = self.src_to_ino.get(&src_id)?;
        self.map.get_mut(ino)
    }

    fn vacant_entry(&self) -> VacantEntry<'_> {
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        VacantEntry { table: self, ino }
    }
}

struct VacantEntry<'a> {
    table: &'a INodeTable,
    ino: Ino,
}

impl VacantEntry<'_> {
    fn ino(&self) -> Ino {
        self.ino
    }

    fn insert(self, inode: INode) {
        let src_id = inode.src_id;
        self.table.map.insert(self.ino, inode);
        self.table.src_to_ino.insert(src_id, self.ino);
    }
}

#[inline]
fn no_entry() -> io::Error {
    io::Error::from_raw_os_error(libc::ENOENT)
}

#[inline]
fn io_to_errno(err: io::Error) -> i32 {
    err.raw_os_error().unwrap_or(libc::EIO)
}

#[inline]
fn make_entry_param(ino: u64, attr: libc::stat) -> EntryOut {
    let mut reply = EntryOut::default();
    reply.ino(ino);
    fill_attr(reply.attr(), &attr);
    reply
}