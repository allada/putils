// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::ptr::{null_mut, NonNull};
use std::slice;

use parking_lot::Mutex;
use libc::{posix_memalign, madvise, memset};

pub struct Buffer<const BIT_ALIGNMENT: usize> {
    data_ptr: NonNull<u8>,
    length: usize,
    pool: Arc<Mutex<AlignedBufferPoolImpl<BIT_ALIGNMENT>>>,
}

impl <const BIT_ALIGNMENT: usize> Buffer<BIT_ALIGNMENT> {
    fn new(pool: Arc<Mutex<AlignedBufferPoolImpl<BIT_ALIGNMENT>>>) -> Self {
        let mut data_ptr = null_mut::<libc::c_void>();
        unsafe {
            let res = posix_memalign(&mut data_ptr, Self::max_size(), Self::max_size());
            assert!(res == 0, "Could not call posix_memalign([redacted], {}, {})", Self::max_size(), Self::max_size());
            let res = madvise(data_ptr, Self::max_size(), libc::MADV_HUGEPAGE);
            assert!(res == 0, "Could not call madvise([redacted], {}, MADV_HUGEPAGE)", Self::max_size());
            // We need to set 1 byte of our buffer to allocate our page.
            memset(data_ptr, 0, 1); // Result is not important.
        }
        Self {
            data_ptr: NonNull::new(data_ptr as *mut u8).expect("posix_memalign() did not populate our pointer"),
            length: 0,
            pool,
        }
    }

    #[inline]
    pub const fn max_size() -> usize {
        1 << BIT_ALIGNMENT
    }

    #[inline]
    pub const fn capacity(&self) -> usize {
        1 << BIT_ALIGNMENT
    }

    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity(), "Tried set_len beyond capacity");
        self.length = len;
    }
}

unsafe impl<const BIT_ALIGNMENT: usize> Send for Buffer<BIT_ALIGNMENT> {}

impl<const BIT_ALIGNMENT: usize> Deref for Buffer<BIT_ALIGNMENT> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data_ptr.as_ptr(), self.length) }
    }
}

impl<const BIT_ALIGNMENT: usize> DerefMut for Buffer<BIT_ALIGNMENT> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.data_ptr.as_ptr(), self.length) }
    }
}

impl <const BIT_ALIGNMENT: usize> Drop for Buffer<BIT_ALIGNMENT> {
    fn drop(&mut self) {
        let mut pool = self.pool.lock();
        // Put data back into the pool for reuse.
        pool.add(Self {
            data_ptr: self.data_ptr,
            length: 0,
            pool: self.pool.clone(),
        });
    }
}

#[derive(Default)]
struct AlignedBufferPoolImpl<const BIT_ALIGNMENT: usize> {
    cooldown_buffers: usize,
    buffers: Vec<Buffer<BIT_ALIGNMENT>>,
    cooling_buffers: VecDeque<Buffer<BIT_ALIGNMENT>>,
}

impl <const BIT_ALIGNMENT: usize> AlignedBufferPoolImpl<BIT_ALIGNMENT> {
    fn new(cooldown_buffers: usize) -> Self {
        Self {
            cooldown_buffers,
            ..Default::default()
        }
    }
    /// Place buffer into `cooling_buffer` and promote the buffer in `cooling_buffer`
    /// back into the `buffers` pool.
    fn add(&mut self, buffer: Buffer<BIT_ALIGNMENT>) {
        self.cooling_buffers.push_back(buffer);
        while self.cooling_buffers.len() > self.cooldown_buffers {
            self.buffers.push(self.cooling_buffers.pop_front().unwrap());
        }
    }
}

/// Struct used to manage a large buffer pool where items are never deallocated.
/// Buffer objects are guaranteed to be memory aligned and marked as HUGE_PAGE
/// in the kernel.
pub struct AlignedBufferPool<const BIT_ALIGNMENT: usize> {
    inner: Arc<Mutex<AlignedBufferPoolImpl<BIT_ALIGNMENT>>>,
}

impl <const BIT_ALIGNMENT: usize> AlignedBufferPool<BIT_ALIGNMENT> {
    pub fn new(cooldown_buffers: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AlignedBufferPoolImpl::new(cooldown_buffers)))
        }
    }

    pub fn get(&self) -> Buffer<BIT_ALIGNMENT> {
        let mut pool = self.inner.lock();
        if !pool.buffers.is_empty() {
            pool.buffers.pop().unwrap()
        } else {
            Buffer::new(self.inner.clone())
        }
    }
}