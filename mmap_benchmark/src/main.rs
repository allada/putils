use std::time::Duration;
use std::os::unix::prelude::RawFd;
use std::ffi::c_void;
use std::fs::{File, OpenOptions};
use std::num::NonZeroUsize;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::slice::{from_raw_parts, from_raw_parts_mut};

use microbench::{self, Options};
use libc::{posix_memalign, pread};
use nix::fcntl::PosixFadviseAdvice;
use nix::fcntl::posix_fadvise;
use nix::sys::mman::{mmap, munmap, madvise, MapFlags, ProtFlags, MmapAdvise};
use rand::Rng;

static FILE_FULL_PATH: &str = "/data/data.tar.zstd";

fn make_random_locations(file_len: usize) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut random_locations = Vec::with_capacity(10_000);
    unsafe {
        random_locations.set_len(random_locations.capacity());
    }
    for i in 0..random_locations.len() {
        random_locations[i] = rng.gen_range(0..file_len);
    }
    return random_locations;
}

fn init_mmap() -> (&'static [u8], *mut c_void, RawFd) {
    let file = OpenOptions::new()
        .read(true)
        .open(FILE_FULL_PATH)
        .unwrap();
    let file_len = file.metadata().unwrap().len() as usize;
    let raw_fd = file.into_raw_fd();

    return unsafe {
        let raw_ptr: *mut c_void = mmap(
            None,
            NonZeroUsize::new(file_len).unwrap(),
            ProtFlags::PROT_READ,
            MapFlags::MAP_PRIVATE,
            raw_fd,
            0
        ).unwrap();
        madvise(raw_ptr, file_len, MmapAdvise::MADV_HUGEPAGE).unwrap();
        madvise(raw_ptr, file_len, MmapAdvise::MADV_DONTNEED).unwrap();
        madvise(raw_ptr, file_len, MmapAdvise::MADV_RANDOM).unwrap();
        (from_raw_parts::<u8>(raw_ptr as *mut u8, file_len), raw_ptr, raw_fd)
    };
}

fn cleanup_mmap(raw_fd: RawFd, raw_ptr: *mut c_void, file_len: usize) {
    unsafe {
        munmap(raw_ptr, file_len).unwrap();
        // This will trigger a close of the file.
        File::from_raw_fd(raw_fd);
    }
}


fn read_file_mmap(ro_slice: &'static [u8], random_locations: &[usize], operations: &mut u64) {
    // Now read the random spots in the mmap.
    for pos in random_locations {
        unsafe {
            let val = ro_slice.get_unchecked(*pos);
            if *val == 184 {
                *operations += 1;
            }
        }
    }

}

fn read_file(raw_fd: RawFd, buffer: &mut [u8], random_locations: &[usize], operations: &mut u64) {
    // Now read the random spots in the mmap.
    for pos in random_locations {
        unsafe {
            pread(raw_fd, buffer.as_mut_ptr() as *mut c_void, 1, *pos as i64);
            if buffer[0] == 184 {
                *operations += 1;
            }
        }
    }
}

fn main() {
    let mut mmap_operations = 0;
    let mut pread_operations = 0;
    let options = Options::default().time(Duration::new(10, 0));
    let file_len = std::fs::metadata(FILE_FULL_PATH).unwrap().len();
    // Fill in some random locations to read from into a vector.

    let random_locations = make_random_locations(file_len as usize);
    {
        let (ro_slice, raw_ptr, raw_fd) = init_mmap();
        microbench::bench(&options, "mmap", || read_file_mmap(ro_slice, &random_locations, &mut mmap_operations));
        cleanup_mmap(raw_fd, raw_ptr, ro_slice.len());
    }
    let random_locations = make_random_locations(file_len as usize);
    {
        let file = unsafe {
            let file_full_path_null_terminated: String = format!("{}\0", FILE_FULL_PATH);
            let raw_fd = libc::open(file_full_path_null_terminated.as_ptr().cast(), libc::O_DIRECT | libc::O_NOATIME);
            assert!(raw_fd != -1, "bad fd");
            File::from_raw_fd(raw_fd)
        };
        let buffer = unsafe {
            const TWO_MB: usize = 1 << 21;
            let mut data_ptr = std::ptr::null_mut::<libc::c_void>();
            let result = posix_memalign(&mut data_ptr, TWO_MB, TWO_MB);
            assert!(result == 0, "posix_memalign failed");
            from_raw_parts_mut::<u8>(data_ptr as *mut u8, TWO_MB)
        };
        posix_fadvise(file.as_raw_fd(), 0, file_len as i64, PosixFadviseAdvice::POSIX_FADV_NOREUSE).unwrap();
        posix_fadvise(file.as_raw_fd(), 0, file_len as i64, PosixFadviseAdvice::POSIX_FADV_RANDOM).unwrap();
        microbench::bench(&options, "pread", || read_file(file.as_raw_fd(), buffer, &random_locations, &mut pread_operations));
    }

    println!("mmap reads {}", mmap_operations);
    println!("file reads {}", pread_operations);
}
