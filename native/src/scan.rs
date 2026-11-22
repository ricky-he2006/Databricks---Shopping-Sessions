//! Reads one block of a CSV file and hands out its well-formed lines. Every
//! byte is classified once, 64 at a time, and the positions of separators and
//! newlines written out; a line is its newline and the separators before it.
//!
//! A line belongs to the block that holds the newline before it. So block 0
//! never yields the header, and a line crossing a block edge is read once.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

const PAD: usize = 64;
const EXTRA: usize = 4096;
const SLACK: usize = 1 << 16;

unsafe extern "C" {
    fn mmap(
        address: *mut std::ffi::c_void,
        length: usize,
        protection: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    #[cfg(target_os = "linux")]
    fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
    #[cfg(target_os = "linux")]
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

/// An input file, mapped when it can be.
pub struct Source {
    file: File,
    pub size: u64,
    map: *const u8,
    guarded_tail: bool,
}

unsafe impl Send for Source {}
unsafe impl Sync for Source {}

impl Source {
    pub fn open(path: &std::path::Path, map: bool) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        use std::os::fd::AsRawFd;
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        // Workers read consecutive regions from each file.
        #[cfg(target_os = "linux")]
        if size > 0 {
            unsafe { posix_fadvise(file.as_raw_fd(), 0, 0, 2) };
        }
        let mut source = Self { file, size, map: std::ptr::null(), guarded_tail: false };
        if map {
            source.map_input();
        }
        Ok(source)
    }

    pub fn map_input(&mut self) {
        use std::os::fd::AsRawFd;
        if self.size != 0 && self.map.is_null() {
            // Reserve our own virtual range and replace only its file prefix.
            // The anonymous tail keeps SIMD overreads valid even at a page boundary.
            #[cfg(target_os = "linux")]
            unsafe {
                unsafe extern "C" {
                    fn getpagesize() -> i32;
                    fn munmap(address: *mut std::ffi::c_void, length: usize) -> i32;
                }
                let page = getpagesize() as usize;
                let rounded = (self.size as usize).div_ceil(page) * page;
                let length = rounded + page;
                let region = mmap(std::ptr::null_mut(),length,1,2 | 0x20,-1,0);
                if region as usize != usize::MAX {
                    let file_map = mmap(region,self.size as usize,1,2 | 0x10,self.file.as_raw_fd(),0);
                    if file_map == region {
                        self.map = region.cast();
                        self.guarded_tail = true;
                        madvise(region, length, 3);
                    } else {
                        munmap(region,length);
                    }
                }
            }
            if self.map.is_null() {
                let address = unsafe { mmap(std::ptr::null_mut(), self.size as usize, 1, 2, self.file.as_raw_fd(), 0) };
                if address as usize != usize::MAX {
                    self.map = address as *const u8;
                }
            }
        }
    }

    /// Unmaps the pages of `[start, end)` now, from whichever thread calls. Left
    /// mapped, the kernel would unmap the whole file on one thread at exit,
    /// inside the timed run.
    pub fn release(&self, start: u64, end: u64) {
        #[cfg(target_os = "linux")]
        if !self.map.is_null() {
            let from = (self.map as usize + start as usize + 4095) & !4095;
            let to = (self.map as usize + end as usize) & !4095;
            if to > from {
                unsafe { madvise(from as *mut _, to - from, 4) };
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (start, end);
    }
}

/// How many bytes ahead of the scan to prefetch input. The input is mapped in
/// 4 KB pages and hardware prefetchers stop at each page's end; on the Intel
/// machines this was measured on, reading ahead by hand made the scan up to
/// twice as fast, and it cost nothing where it did not help.
const READ_AHEAD: usize = 1024;

#[inline(always)]
fn fetch(target: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch(target as *const i8, _MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = target;
}

pub struct Block {
    buf: Vec<u8>,
    base: *const u8,
    own_end: usize,
    /// One past the newline that ends our last line; 128 bytes after it are readable.
    end: usize,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64) {
    unsafe {
        use std::arch::x86_64::*;
        let low = _mm256_loadu_si256(p as *const __m256i);
        let high = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let (comma, newline, quote) = (
            _mm256_set1_epi8(b',' as i8),
            _mm256_set1_epi8(b'\n' as i8),
            _mm256_set1_epi8(b'"' as i8),
        );
        let ends = |v: __m256i| _mm256_or_si256(_mm256_cmpeq_epi8(v, comma), _mm256_cmpeq_epi8(v, newline));
        let bits = |v: __m256i| _mm256_movemask_epi8(v) as u32 as u64;
        (
            bits(ends(low)) | (bits(ends(high)) << 32),
            bits(_mm256_cmpeq_epi8(low, quote)) | (bits(_mm256_cmpeq_epi8(high, quote)) << 32),
        )
    }
}

/// Separators (commas and newlines), newlines and quotes of the 64 bytes at
/// `p`, one bit per byte.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn classes(p: *const u8) -> (u64, u64, u64) {
    unsafe {
        use std::arch::x86_64::*;
        #[cfg(target_feature = "avx512bw")]
        {
            let data = _mm512_loadu_si512(p.cast());
            let newline = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b'\n' as i8));
            let comma = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b',' as i8));
            let quote = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b'"' as i8));
            return (comma | newline, newline, quote);
        }
        #[allow(unreachable_code)]
        let low = _mm256_loadu_si256(p as *const __m256i);
        let high = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let find = |byte: u8| {
            let wanted = _mm256_set1_epi8(byte as i8);
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(low, wanted)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(high, wanted)) as u32 as u64;
            a | (b << 32)
        };
        let (comma, newline) = (find(b','), find(b'\n'));
        (comma | newline, newline, find(b'"'))
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn classes(p: *const u8) -> (u64, u64, u64) {
    unsafe {
        use std::arch::aarch64::*;
        let chunks = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        // Each matching byte contributes its bit within a group of eight.
        // Pairwise addition concatenates the four vectors into one byte mask.
        let weights = vreinterpretq_u8_u64(vdupq_n_u64(0x8040_2010_0804_0201));
        let find = |byte: u8| -> u64 {
            let wanted = vdupq_n_u8(byte);
            let lanes = chunks.map(|chunk| vandq_u8(vceqq_u8(chunk, wanted), weights));
            let pairs = vpaddq_u8(vpaddq_u8(lanes[0], lanes[1]), vpaddq_u8(lanes[2], lanes[3]));
            vget_lane_u64(vreinterpret_u64_u8(vpadd_u8(vget_low_u8(pairs), vget_high_u8(pairs))), 0)
        };
        let (comma, newline) = (find(b','), find(b'\n'));
        (comma | newline, newline, find(b'"'))
    }
}

#[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), target_arch = "aarch64")))]
#[inline(always)]
unsafe fn classes(p: *const u8) -> (u64, u64, u64) {
    unsafe {
        let (mut commas, mut newlines, mut quotes) = (0u64, 0u64, 0u64);
        for at in 0..64 {
            let byte = *p.add(at);
            commas |= u64::from(byte == b',') << at;
            newlines |= u64::from(byte == b'\n') << at;
            quotes |= u64::from(byte == b'"') << at;
        }
        (commas | newlines, newlines, quotes)
    }
}

/// Writes the position of every set bit of `bits`, plus `base`, at `out`,
/// and returns how many. Up to `UNROLL` are written without a branch; `out`
/// needs room for 64 + `UNROLL`.
#[inline(always)]
unsafe fn flatten<const UNROLL: usize>(mut bits: u64, base: u32, out: *mut u32) -> usize {
    unsafe {
        let count = bits.count_ones() as usize;
        for at in 0..UNROLL {
            *out.add(at) = base + bits.trailing_zeros();
            bits &= bits.wrapping_sub(1);
        }
        if count > UNROLL {
            let mut at = UNROLL;
            while bits != 0 {
                *out.add(at) = base + bits.trailing_zeros();
                bits &= bits.wrapping_sub(1);
                at += 1;
            }
        }
        count
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512vbmi2")]
unsafe fn wide_classes(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let data = _mm512_loadu_si512(p.cast());
    let newline = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b'\n' as i8));
    let commas = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b',' as i8));
    let quotes = _mm512_cmpeq_epi8_mask(data, _mm512_set1_epi8(b'"' as i8));
    (commas | newline, newline, quotes)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512vbmi2")]
unsafe fn wide_positions(bits: u64, base: u32, out: *mut u32) -> usize {
    use std::arch::x86_64::*;
    const OFFSETS: [u8; 64] = {
        let mut a = [0; 64];
        let mut i = 0;
        while i < 64 { a[i] = i as u8; i += 1; }
        a
    };
    let n = bits.count_ones() as usize;
    if n > 16 {
        return flatten::<10>(bits, base, out);
    }
    let packed = _mm512_maskz_compress_epi8(bits, _mm512_loadu_si512(OFFSETS.as_ptr().cast()));
    let positions = _mm512_cvtepu8_epi32(_mm512_castsi512_si128(packed));
    _mm512_storeu_si512(out.cast(), _mm512_add_epi32(_mm512_set1_epi32(base as i32), positions));
    n
}

/// Chunks of 64 bytes indexed before their lines are handed out.
const WINDOW: usize = 64;

/// Field `at` of a line from `raw_lines`, without its quotes: (from, to).
#[inline(always)]
pub unsafe fn bounds<const N: usize>(buf: *const u8, start: u32, seps: &[u32; N], at: usize) -> (u32, u32) {
    unsafe {
        let from = if at == 0 { start } else { *seps.get_unchecked(at - 1) + 1 };
        let to = *seps.get_unchecked(at);
        // In a well-formed line a field that starts with a quote ends with one.
        let quoted = u32::from(*buf.add(from as usize) == b'"');
        (from + quoted, to - quoted)
    }
}

/// Field `at` of a line from `raw_lines`, without its quotes: (first byte, length).
#[inline(always)]
pub unsafe fn field<const N: usize>(buf: *const u8, start: u32, seps: &[u32; N], at: usize) -> (*const u8, usize) {
    unsafe {
        let (from, to) = bounds(buf, start, seps, at);
        (buf.add(from as usize), (to - from) as usize)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64) {
    unsafe {
        use std::arch::aarch64::*;
        let bits: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bit = vld1q_u8(bits.as_ptr());
        let chunks = [
            vld1q_u8(p),
            vld1q_u8(p.add(16)),
            vld1q_u8(p.add(32)),
            vld1q_u8(p.add(48)),
        ];
        let (comma, newline, quote) = (vdupq_n_u8(b','), vdupq_n_u8(b'\n'), vdupq_n_u8(b'"'));
        let pack = |m: [uint8x16_t; 4]| -> u64 {
            let s0 = vpaddq_u8(vandq_u8(m[0], bit), vandq_u8(m[1], bit));
            let s1 = vpaddq_u8(vandq_u8(m[2], bit), vandq_u8(m[3], bit));
            let s = vpaddq_u8(s0, s1);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        let ends = |v: uint8x16_t| vorrq_u8(vceqq_u8(v, comma), vceqq_u8(v, newline));
        (
            pack([ends(chunks[0]), ends(chunks[1]), ends(chunks[2]), ends(chunks[3])]),
            pack([
                vceqq_u8(chunks[0], quote),
                vceqq_u8(chunks[1], quote),
                vceqq_u8(chunks[2], quote),
                vceqq_u8(chunks[3], quote),
            ]),
        )
    }
}

#[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), target_arch = "aarch64")))]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64) {
    unsafe {
        let (mut ends, mut quotes) = (0u64, 0u64);
        for at in 0..64 {
            let byte = *p.add(at);
            ends |= u64::from(byte == b',' || byte == b'\n') << at;
            quotes |= u64::from(byte == b'"') << at;
        }
        (ends, quotes)
    }
}

#[cold]
#[inline(never)]
unsafe fn long_field(buf: *const u8, start: usize) -> (usize, bool, usize) {
    unsafe {
        let mut end = start + 64;
        loop {
            let (ends, _) = window(buf.add(end));
            if ends != 0 {
                end += ends.trailing_zeros() as usize;
                break;
            }
            end += 64;
        }
        let bytes = std::slice::from_raw_parts(buf.add(start), end - start);
        let quotes = bytes.iter().filter(|&&byte| byte == b'"').count();
        let opens = usize::from(bytes[0] == b'"');
        let ok = if opens == 1 {
            quotes == 2 && bytes[bytes.len() - 1] == b'"'
        } else {
            quotes == 0
        };
        (end, ok, opens)
    }
}

impl Block {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            base: std::ptr::null(),
            own_end: 0,
            end: 0,
        }
    }

    fn reserve(&mut self, data: usize) {
        let need = PAD + data + 1 + 4 * PAD;
        if self.buf.len() < need {
            // load() may already have read a prefix on a previous iteration.
            self.buf.resize(need, 0);
        }
    }

    pub fn load(&mut self, source: &Source, start: u64, end: u64) -> io::Result<()> {
        let own = (end - start) as usize;
        self.own_end = own;
        if !source.map.is_null() && end + SLACK as u64 <= source.size {
            self.base = unsafe { source.map.add(start as usize) };
            #[cfg(target_os = "linux")]
            unsafe {
                let from = self.base as usize & !4095;
                madvise(from as *mut _, self.base as usize + own + 4096 - from, 22);
            }
            let tail = unsafe { std::slice::from_raw_parts(self.base.add(own), SLACK - 4 * PAD) };
            if let Some(at) = tail.iter().position(|&byte| byte == b'\n') {
                self.end = own + at + 1;
                return Ok(());
            }
        }
        if source.guarded_tail && end + SLACK as u64 > source.size {
            let base = unsafe { source.map.add(start as usize) };
            let remaining = (source.size - end) as usize;
            let tail = unsafe { std::slice::from_raw_parts(base.add(own), remaining) };
            let length = tail.iter().position(|&b| b == b'\n').map(|i| own + i + 1)
                .or_else(|| {
                    let len = own + remaining;
                    (len > 0 && unsafe { *base.add(len - 1) } == b'\n').then_some(len)
                });
            if let Some(length) = length {
                self.base = base;
                self.end = length;
                return Ok(());
            }
        }
        let size = source.size;
        let mut have = 0usize;
        let mut extra = EXTRA;
        loop {
            let want = (own + extra).min((size - start) as usize);
            self.reserve(want);
            source
                .file
                .read_exact_at(&mut self.buf[PAD + have..PAD + want], start + have as u64)?;
            let at_end = start + want as u64 == size;
            let from = PAD + have.max(own);
            let found = self.buf[from..PAD + want].iter().position(|&byte| byte == b'\n');
            have = want;
            if let Some(at) = found {
                have = from - PAD + at + 1;
                break;
            }
            if at_end {
                break;
            }
            extra *= 8;
        }
        if have > 0 && self.buf[PAD + have - 1] != b'\n' {
            self.buf[PAD + have] = b'\n';
            have += 1;
        }
        self.buf[PAD + have..PAD + have + 4 * PAD].fill(0);
        self.base = unsafe { self.buf.as_ptr().add(PAD) };
        self.end = have;
        Ok(())
    }

    /// The separators of the line starting at `start`, the old way: its
    /// bytes in 64-byte windows. `None` if the line is malformed.
    #[inline(never)]
    unsafe fn split_line<const N: usize>(buf: *const u8, start: usize) -> Option<[u32; N]> {
        unsafe {
            let mut seps = [0u32; N];
            let mut count = 0usize;
            let mut ok = true;
            let mut start = start;
            let mut at = start;
            let (mut ends, mut quotes) = window(buf.add(at));
            loop {
                let mut shift = start - at;
                let mut rest = if shift < 64 { ends >> shift } else { 0 };
                if rest == 0 {
                    at = start;
                    (ends, quotes) = window(buf.add(at));
                    shift = 0;
                    rest = ends;
                }
                let (stop, fine) = if rest != 0 {
                    let length = rest.trailing_zeros();
                    let inside = (quotes >> shift) & ((1u64 << length) - 1);
                    let opens = inside & 1;
                    let wanted = opens * (1 | 1u64.wrapping_shl(length.wrapping_sub(1)));
                    (start + length as usize, inside == wanted && (opens == 0 || length >= 2))
                } else {
                    ends = 0;
                    let (stop, fine, _) = long_field(buf, start);
                    (stop, fine)
                };
                ok &= fine;
                if count < N {
                    seps[count] = stop as u32;
                }
                count += 1;
                if *buf.add(stop) == b'\n' {
                    return if ok && count == N { Some(seps) } else { None };
                }
                start = stop + 1;
            }
        }
    }

    /// Hands every well-formed line of the block to `visit` as where it starts
    /// and its `N` separators: `N - 1` commas, then its newline. Fields still
    /// have their quotes; `field` takes them off.
    ///
    /// The bytes are classified 64 at a time and the positions of separators
    /// and newlines written out a window at a time; a line is well formed when
    /// its `N`th separator is its newline. Quote rules are checked on the
    /// masks: every quote must start or end a field, a field that starts with
    /// one must end with one, and a lone `"` is no field. A 64-byte chunk that
    /// breaks one of these sends its lines down the old path, which says
    /// exactly which are wrong.
    #[inline(always)]
    pub fn raw_lines<const N: usize>(&self, visit: impl FnMut(*const u8, u32, &[u32; N])) {
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("avx512vbmi2") && std::arch::is_x86_feature_detected!("avx512bw") {
            return unsafe { self.raw_wide(visit) };
        }
        self.raw_indexed::<N, false>(visit)
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512bw,avx512vbmi2")]
    unsafe fn raw_wide<const N: usize>(&self, visit: impl FnMut(*const u8, u32, &[u32; N])) {
        self.raw_indexed::<N, true>(visit)
    }

    #[inline(always)]
    fn raw_indexed<const N: usize, const WIDE: bool>(&self, mut visit: impl FnMut(*const u8, u32, &[u32; N])) {
        let buf = self.base;
        let (own_end, end) = (self.own_end, self.end);
        unsafe {
            let head = std::slice::from_raw_parts(buf, own_end.min(end));
            let Some(first) = head.iter().position(|&byte| byte == b'\n') else {
                return;
            };
            if first + 1 >= end {
                return;
            }
            let end = end as u32;
            let mut seps: Vec<u32> = Vec::with_capacity(WINDOW * 64 + 256);
            let mut newlines: Vec<u32> = Vec::with_capacity(WINDOW * 64 + 64);
            // Starts of chunks that break a quote rule; the last is a sentinel.
            let mut bad: Vec<u32> = Vec::new();
            let mut bad_at = 0usize;
            let mut kept = 0usize;
            let mut line = first as u32 + 1;
            let mut at = line;
            // 1 while a field that opened with a quote in an earlier chunk has
            // its closing separator still to come.
            let mut carry = 0u64;
            while at < end {
                if seps.capacity() < kept + WINDOW * 64 + 128 {
                    // The separators kept from the last window move with the buffer.
                    seps.set_len(kept);
                    seps.reserve(WINDOW * 64 + 128);
                }
                let mut count = kept;
                let mut lines = 0usize;
                let out = seps.as_mut_ptr();
                let ends = newlines.as_mut_ptr();
                let stop_at = end.min(at.saturating_add((WINDOW * 64) as u32));
                while at < stop_at {
                    let p = buf.add(at as usize);
                    fetch(p.wrapping_add(READ_AHEAD));
                    #[cfg(target_arch = "x86_64")]
                    let classified = if WIDE { wide_classes(p) } else { classes(p) };
                    #[cfg(not(target_arch = "x86_64"))]
                    let classified = classes(p);
                    let (mut sep, mut newline, mut quote) = classified;
                    let left = end - at;
                    if left < 64 {
                        let valid = (1u64 << left) - 1;
                        sep &= valid;
                        newline &= valid;
                        quote &= valid;
                    }
                    let before = *p.sub(1);
                    // A quote just before the chunk can close a field at its first byte.
                    if (quote | carry) != 0 || before == b'"' {
                        let after = *p.add(64);
                        let sep_before = (sep << 1) | u64::from(before == b',' || before == b'\n');
                        let sep_after = (sep >> 1) | (u64::from(after == b',' || after == b'\n') << 63);
                        let quote_before = (quote << 1) | u64::from(before == b'"');
                        let opening = quote & sep_before;
                        let closing = quote & sep_after;
                        // Both masks are subsets of quote. A quote with
                        // neither boundary or both boundaries is invalid.
                        let misplaced = quote ^ opening ^ closing;
                        // Each opening seeds a carry into the following
                        // delimiter. Bit zero is also seeded if the field
                        // was opened in the preceding 64-byte chunk.
                        let (sum, over) = (!sep).overflowing_add(opening | carry);
                        let wrong = misplaced | ((sum ^ quote_before) & sep);
                        carry = u64::from(over);
                        if wrong != 0 {
                            bad.push(at);
                        }
                    }
                    #[cfg(target_arch = "x86_64")]
                    let positions = if WIDE { wide_positions(sep, at, out.add(count)) } else { flatten::<10>(sep, at, out.add(count)) };
                    #[cfg(not(target_arch = "x86_64"))]
                    let positions = flatten::<10>(sep, at, out.add(count));
                    count += positions;
                    lines += flatten::<2>(newline, at, ends.add(lines));
                    at += 64;
                }
                // Sentinels: a line's separators are found by position.
                for extra in 0..N {
                    *out.add(count + extra) = u32::MAX;
                }
                bad.push(u32::MAX - 64);
                let mut sep_at = 0usize;
                for &stop in std::slice::from_raw_parts(ends, lines) {
                    let start = line;
                    line = stop + 1;
                    while *bad.get_unchecked(bad_at) + 64 <= start {
                        bad_at += 1;
                    }
                    let clean = *bad.get_unchecked(bad_at) > stop;
                    if *out.add(sep_at + N - 1) == stop && clean {
                        visit(buf, start, &*(out.add(sep_at) as *const [u32; N]));
                        sep_at += N;
                        continue;
                    }
                    while *out.add(sep_at) < stop {
                        sep_at += 1;
                    }
                    sep_at += 1;
                    if !clean {
                        if let Some(found) = Self::split_line::<N>(buf, start as usize) {
                            visit(buf, start, &found);
                        }
                    }
                }
                bad.pop();
                // Separators of the line still open move to the front.
                kept = count - sep_at;
                std::ptr::copy(out.add(sep_at), out, kept);
            }
        }
    }

    /// `raw_lines`, with every field's bounds worked out and unquoted.
    #[inline(always)]
    pub fn lines<const N: usize>(&self, mut visit: impl FnMut(*const u8, &[(u32, u32); N])) {
        self.raw_lines::<N>(|buf, start, seps| unsafe {
            let mut fields = [(0u32, 0u32); N];
            for at in 0..N {
                let (from, to) = bounds(buf, start, seps, at);
                fields[at] = (from, to);
            }
            visit(buf, &fields);
        });
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn lines_slow<const N: usize>(&self, mut visit: impl FnMut(*const u8, &[(u32, u32); N])) {
        let buf = self.base;
        let (own_end, end) = (self.own_end, self.end);
        unsafe {
            let head = std::slice::from_raw_parts(buf, own_end.min(end));
            let Some(mut newline) = head.iter().position(|&byte| byte == b'\n') else {
                return;
            };
            while newline < own_end && newline + 1 < end {
                fetch(buf.wrapping_add(newline + READ_AHEAD));
                fetch(buf.wrapping_add(newline + READ_AHEAD + 64));
                let mut fields = [(0u32, 0u32); N];
                let mut count = 0usize;
                let mut ok = true;
                let mut start = newline + 1;
                let mut at = start;
                let (mut ends, mut quotes) = window(buf.add(at));
                loop {
                    let mut shift = start - at;
                    let mut rest = if shift < 64 { ends >> shift } else { 0 };
                    if rest == 0 {
                        at = start;
                        (ends, quotes) = window(buf.add(at));
                        shift = 0;
                        rest = ends;
                    }
                    let (stop, fine, opens) = if rest != 0 {
                        let length = rest.trailing_zeros();
                        let inside = (quotes >> shift) & ((1u64 << length) - 1);
                        let opens = inside & 1;
                        let wanted = opens * (1 | 1u64.wrapping_shl(length.wrapping_sub(1)));
                        (
                            start + length as usize,
                            inside == wanted && (opens == 0 || length >= 2),
                            opens as usize,
                        )
                    } else {
                        ends = 0;
                        long_field(buf, start)
                    };
                    ok &= fine;
                    if count < N {
                        fields[count] = ((start + opens) as u32, (stop - opens) as u32);
                    }
                    count += 1;
                    if *buf.add(stop) == b'\n' {
                        newline = stop;
                        break;
                    }
                    start = stop + 1;
                }
                if ok && count == N {
                    visit(buf, &fields);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<const N: usize>(block: &Block, slow: bool) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut take = |buf: *const u8, fields: &[(u32, u32); N]| {
            out.push(
                fields
                    .iter()
                    .map(|&(a, b)| unsafe { std::slice::from_raw_parts(buf.add(a as usize), (b - a) as usize).to_vec() })
                    .collect(),
            );
        };
        if slow {
            block.lines_slow::<N>(&mut take);
        } else {
            block.lines::<N>(&mut take);
        }
        out
    }

    fn check<const N: usize>(text: &[u8], own: usize) {
        // As `load` leaves a buffer: data, a final newline, zero padding.
        let mut data = text.to_vec();
        if data.last() != Some(&b'\n') {
            data.push(b'\n');
        }
        let own = own.min(data.len());
        let end = match data[own..].iter().position(|&byte| byte == b'\n') {
            Some(at) => own + at + 1,
            None => data.len(),
        };
        let mut buf = vec![0u8; PAD + data.len() + 4 * PAD + 64];
        buf[PAD..PAD + data.len()].copy_from_slice(&data);
        let block = Block {
            buf,
            base: std::ptr::null(),
            own_end: own,
            end,
        };
        let block = Block {
            base: unsafe { block.buf.as_ptr().add(PAD) },
            ..block
        };
        let fast = collect::<N>(&block, false);
        let slow = collect::<N>(&block, true);
        assert_eq!(fast, slow, "{:?}", String::from_utf8_lossy(&data));
    }

    #[test]
    fn growing_a_block_preserves_the_partial_row() {
        let mut block = Block::new();
        block.reserve(EXTRA);
        block.buf[PAD..PAD + 4].copy_from_slice(b"row,");
        let previous_capacity = block.buf.capacity();
        block.reserve(previous_capacity + EXTRA);
        assert_eq!(&block.buf[PAD..PAD + 4], b"row,");
    }

    #[test]
    fn matches_line_by_line() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..30_000 {
            let mut text = Vec::new();
            let lines = 1 + next() % 40;
            for _ in 0..lines {
                let fields = if next() % 8 == 0 { next() % 9 } else { 7 };
                for field in 0..fields {
                    if field > 0 {
                        text.push(b',');
                    }
                    let len = match next() % 10 {
                        0 => 0,
                        1 => 60 + next() % 90,
                        _ => next() % 14,
                    };
                    let rare = round % 3 == 0;
                    let mut value: Vec<u8> = (0..len)
                        .map(|_| match next() % if rare { 3000 } else { 30 } {
                            0 => b'"',
                            1 => b'\r',
                            _ => b'a' + (next() % 26) as u8,
                        })
                        .collect();
                    match next() % if rare { 60 } else { 6 } {
                        0 | 1 => {
                            value.insert(0, b'"');
                            value.push(b'"');
                        }
                        2 if next() % 4 == 0 => value.insert(0, b'"'),
                        3 if next() % 4 == 0 => value.push(b'"'),
                        _ => {}
                    }
                    text.extend_from_slice(&value);
                }
                text.push(b'\n');
            }
            let own = (next() as usize) % (text.len() + 1);
            check::<7>(&text, own);
            if round % 4 == 0 {
                check::<2>(&text, own);
                check::<4>(&text, text.len());
            }
        }
    }
}

#[cfg(test)]
mod real_files {
    use super::*;

    fn compare<const N: usize>(path: &std::path::Path, block_size: u64) -> usize {
        let source = Source::open(path, true).unwrap();
        let mut block = Block::new();
        let mut bad = 0;
        let mut start = 0;
        while start < source.size {
            let end = (start + block_size).min(source.size);
            block.load(&source, start, end).unwrap();
            let grab = |slow: bool| {
                let mut out: Vec<Vec<Vec<u8>>> = Vec::new();
                let mut take = |buf: *const u8, fields: &[(u32, u32); N]| {
                    out.push(fields.iter().map(|&(a, b)| unsafe {
                        std::slice::from_raw_parts(buf.add(a as usize), (b - a) as usize).to_vec()
                    }).collect());
                };
                if slow { block.lines_slow::<N>(&mut take) } else { block.lines::<N>(&mut take) }
                out
            };
            let (fast, slow) = (grab(false), grab(true));
            if fast != slow {
                bad += 1;
                let only_fast: Vec<_> = fast.iter().filter(|l| !slow.contains(l)).take(3).collect();
                let only_slow: Vec<_> = slow.iter().filter(|l| !fast.contains(l)).take(3).collect();
                eprintln!("{} @{}: fast {} slow {}", path.display(), start, fast.len(), slow.len());
                for l in only_fast { eprintln!("  fast only: {:?}", l.iter().map(|f| String::from_utf8_lossy(f).to_string()).collect::<Vec<_>>()); }
                for l in only_slow { eprintln!("  slow only: {:?}", l.iter().map(|f| String::from_utf8_lossy(f).to_string()).collect::<Vec<_>>()); }
            }
            start = end;
        }
        bad
    }

    #[test]
    fn real_data() {
        let Ok(dir) = std::env::var("SCAN_DIR") else { return };
        let mut bad = 0;
        for (folder, n) in [("events", 7), ("clicks", 4)] {
            let mut paths: Vec<_> = std::fs::read_dir(format!("{dir}/{folder}")).unwrap().map(|e| e.unwrap().path()).collect();
            paths.sort();
            for path in paths {
                bad += if n == 7 { compare::<7>(&path, 1 << 19) } else { compare::<4>(&path, 1 << 19) };
            }
        }
        assert_eq!(bad, 0);
    }
}
