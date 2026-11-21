//! Reads one block of a CSV file and hands out its lines as fields, with their
//! wrapping quotes already removed. One pass: for each line, the commas,
//! newlines and quotes of the next 64 bytes are found at once, and the fields
//! are read off those bit masks.
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
}

/// An input file, mapped when it can be.
pub struct Source {
    file: File,
    pub size: u64,
    map: *const u8,
}

unsafe impl Send for Source {}
unsafe impl Sync for Source {}

impl Source {
    pub fn open(path: &std::path::Path, map: bool) -> io::Result<Self> {
        use std::os::fd::AsRawFd;
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        let mut mapped = std::ptr::null();
        if map && size > 0 {
            let address = unsafe { mmap(std::ptr::null_mut(), size as usize, 1, 2, file.as_raw_fd(), 0) };
            if address as usize != usize::MAX {
                mapped = address as *const u8;
            }
        }
        Ok(Self {
            file,
            size,
            map: mapped,
        })
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

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // Block::load guarantees readable padding beyond the last newline:
    // either mapped slack or zeroed buffer bytes. This is the same
    // 64-byte read range as the AVX2 and NEON implementations.
    unsafe {
        let input = _mm512_loadu_si512(p.cast());
        let commas = _mm512_cmpeq_epi8_mask(input, _mm512_set1_epi8(b',' as i8));
        let newlines = _mm512_cmpeq_epi8_mask(input, _mm512_set1_epi8(b'\n' as i8));
        let quotes = _mm512_cmpeq_epi8_mask(input, _mm512_set1_epi8(b'"' as i8));
        (commas | newlines, newlines, quotes)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64, u64) {
    unsafe {
        use std::arch::x86_64::*;
        let low = _mm256_loadu_si256(p as *const __m256i);
        let high = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let (comma, newline, quote) = (
            _mm256_set1_epi8(b',' as i8),
            _mm256_set1_epi8(b'\n' as i8),
            _mm256_set1_epi8(b'"' as i8),
        );
        let bits = |v: __m256i| _mm256_movemask_epi8(v) as u32 as u64;
        let both = |low: __m256i, high: __m256i| bits(low) | (bits(high) << 32);
        let (line_low, line_high) = (_mm256_cmpeq_epi8(low, newline), _mm256_cmpeq_epi8(high, newline));
        let lines = both(line_low, line_high);
        (
            both(_mm256_cmpeq_epi8(low, comma), _mm256_cmpeq_epi8(high, comma)) | lines,
            lines,
            both(_mm256_cmpeq_epi8(low, quote), _mm256_cmpeq_epi8(high, quote)),
        )
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn window(p: *const u8) -> (u64, u64, u64) {
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
                vceqq_u8(chunks[0], newline),
                vceqq_u8(chunks[1], newline),
                vceqq_u8(chunks[2], newline),
                vceqq_u8(chunks[3], newline),
            ]),
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
unsafe fn window(p: *const u8) -> (u64, u64, u64) {
    unsafe {
        let (mut ends, mut lines, mut quotes) = (0u64, 0u64, 0u64);
        for at in 0..64 {
            let byte = *p.add(at);
            ends |= u64::from(byte == b',' || byte == b'\n') << at;
            lines |= u64::from(byte == b'\n') << at;
            quotes |= u64::from(byte == b'"') << at;
        }
        (ends, lines, quotes)
    }
}

#[cold]
#[inline(never)]
unsafe fn long_field(buf: *const u8, start: usize) -> (usize, bool, usize) {
    unsafe {
        let mut end = start + 64;
        loop {
            let (ends, _, _) = window(buf.add(end));
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

    #[inline(always)]
    pub fn lines<const N: usize>(&self, mut visit: impl FnMut(*const u8, &[(u32, u32); N])) {
        let buf = self.base;
        let (own_end, end) = (self.own_end, self.end);
        unsafe {
            let head = std::slice::from_raw_parts(buf, own_end.min(end));
            let Some(mut newline) = head.iter().position(|&byte| byte == b'\n') else {
                return;
            };
            let mut cursor = Cursor::new(buf, (end + 63) & !63, newline + 1);
            while newline < own_end && newline + 1 < end {
                fetch(buf.wrapping_add(newline + READ_AHEAD));
                fetch(buf.wrapping_add(newline + READ_AHEAD + 64));
                let line = newline + 1;
                let mut fields = [(0u32, 0u32); N];
                let found = cursor.split(line, &mut fields);
                let ok;
                if found != ODD {
                    newline = found;
                    ok = true;
                } else {
                    (ok, newline) = odd_line(buf, line, &mut fields);
                    cursor = Cursor::new(buf, cursor.limit, newline + 1);
                }
                if ok {
                    visit(buf, &fields);
                }
            }
        }
    }
}

/// What `Cursor::split` returns for a line it leaves to `odd_line`.
const ODD: usize = usize::MAX;

/// Where the scan is: a 64-byte window of the block and what is left of it.
struct Cursor {
    buf: *const u8,
    /// No window starts at or after this.
    limit: usize,
    /// Start of the current window, a multiple of 64.
    at: usize,
    /// The window's commas and newlines not yet used, its newlines from the
    /// current line on, and all its quotes.
    ends: u64,
    newlines: u64,
    quotes: u64,
    /// Quotes in the windows before `at`, and before the current line.
    counted: u32,
    before: u32,
}

impl Cursor {
    #[inline(always)]
    unsafe fn new(buf: *const u8, limit: usize, line: usize) -> Self {
        unsafe {
            let at = line & !63;
            let (ends, newlines, quotes) = window(buf.add(at));
            let keep = !0u64 << (line & 63);
            Self {
                buf,
                limit,
                at,
                ends: ends & keep,
                newlines: newlines & keep,
                quotes,
                counted: 0,
                before: (quotes & !keep).count_ones(),
            }
        }
    }

    /// Splits the line starting at `line` if it is the usual kind: `N` fields,
    /// each bare or wrapped in one pair of quotes. Returns where its newline
    /// is, or `ODD` for any other line, after which the cursor must be rebuilt.
    /// Kept out of line so its loops have the registers to themselves.
    #[inline(never)]
    unsafe fn split<const N: usize>(&mut self, line: usize, fields: &mut [(u32, u32); N]) -> usize {
        unsafe {
            let buf = self.buf;
            let (mut at, mut ends) = (self.at, self.ends);
            let mut stops = [0u32; N];
            let mut early = 0u64;
            for stop in stops.iter_mut() {
                while ends == 0 {
                    if at + 64 >= self.limit {
                        return ODD;
                    }
                    // A newline left behind here ends the line before its last field.
                    early |= self.newlines;
                    self.counted += self.quotes.count_ones();
                    at += 64;
                    (ends, self.newlines, self.quotes) = window(buf.add(at));
                }
                *stop = (at + ends.trailing_zeros() as usize) as u32;
                ends &= ends - 1;
            }
            self.at = at;
            self.ends = ends;
            let last = stops[N - 1] as usize;
            let within = (last - at) as u32;
            // The line's first newline must be its last separator.
            let through = 2u64.wrapping_shl(within).wrapping_sub(1);
            let mut odd = early | ((self.newlines & through) ^ (1u64 << within));
            self.newlines &= !through;
            let mut opened = 0u32;
            let mut start = line as u32;
            for (slot, &stop) in fields.iter_mut().zip(&stops) {
                let opens = u32::from(*buf.add(start as usize) == b'"');
                let closes = u32::from(*buf.add(stop as usize - 1) == b'"');
                odd |= u64::from(opens ^ closes);
                opened += opens;
                *slot = (start + opens, stop - opens);
                start = stop + 1;
            }
            let here = self.counted + (self.quotes & ((1u64 << within) - 1)).count_ones();
            odd |= u64::from(here.wrapping_sub(self.before) ^ (2 * opened));
            self.before = here;
            if odd == 0 { last } else { ODD }
        }
    }
}

/// One line the fast path would not take: its fields, whether it is a good
/// line of `N` fields, and where its newline is.
#[cold]
#[inline(never)]
unsafe fn odd_line<const N: usize>(buf: *const u8, first: usize, fields: &mut [(u32, u32); N]) -> (bool, usize) {
    unsafe {
        let mut count = 0usize;
        let mut ok = true;
        let mut start = first;
        loop {
            let (ends, _, quotes) = window(buf.add(start));
            let (stop, fine, opens) = if ends != 0 {
                let length = ends.trailing_zeros();
                let inside = quotes & ((1u64 << length) - 1);
                let opens = inside & 1;
                let wanted = opens * (1 | 1u64.wrapping_shl(length.wrapping_sub(1)));
                (
                    start + length as usize,
                    inside == wanted && (opens == 0 || length >= 2),
                    opens as usize,
                )
            } else {
                long_field(buf, start)
            };
            ok &= fine;
            if count < N {
                fields[count] = ((start + opens) as u32, (stop - opens) as u32);
            }
            count += 1;
            if *buf.add(stop) == b'\n' {
                return (ok && count == N, stop);
            }
            start = stop + 1;
        }
    }
}
