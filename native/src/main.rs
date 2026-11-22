//! Shopping Sessions.
//!
//! Three parallel phases, each a pool of threads pulling work from a counter:
//!
//! 1. Parse. CSV blocks become fixed-size records. Events are bucketed by a
//!    hash of `event_id`; clicks have their person looked up and are bucketed
//!    by a hash of the person. One thread builds the links table first.
//! 2. Dedup. Each event bucket fits in cache: keep the earliest copy of every
//!    `event_id`, drop the invalid, look up the person, bucket by person.
//! 3. Sessions. Each person bucket is grouped, sorted, sessionized and
//!    attributed, and its `user_daily` rows are written by the thread itself.

mod diag;
mod keys;
mod pq;
mod scan;
mod ts;

use scan::{Block, Source};
use std::cmp::Ordering as Order;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn _exit(status: i32) -> !;
}

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const SESSION_GAP: u64 = 1_800_000;
/// Bytes of CSV a thread takes at a time.
const BLOCK: u64 = 1 << 19;
/// Bytes of a links file read at a time.
const LINK_BLOCK: u64 = 1 << 19;
/// Bytes of a mapped file a thread unmaps at a time, once parsing is over.
const SEGMENT: u64 = 1 << 25;
const ID_BITS: u32 = 10;
const PERSON_BITS: u32 = 10;
const MIX: u64 = 0x9E37_79B9_7F4A_7C15;
/// The `ts` of an event that is invalid, late, or a losing duplicate.
const DEAD: u32 = u32::MAX;
const NONE: u32 = u32::MAX;
const MAX_THREADS: usize = 32;
const GROUP_ROWS: usize = 1 << 17;
/// How many records ahead to prefetch a links lookup.

// ---------------------------------------------------------------- buckets

/// Bytes per chunk. Chunks are aligned to their size, so a partition's write
/// position alone says when its chunk is full: the hot state of a partition is
/// one pointer, and 2048 of them stay in the L1 cache.
const CHUNK_BYTES: usize = 8192;
/// Chunks per slab.
const SLAB: usize = 1 << 13;
/// The dedup table packs a record's offset in its chunk into this many bits.
const OFFSET_BITS: u32 = 9;

/// Records appended to one of many partitions, in fixed chunks carved from
/// big slabs: no copying on growth, and a chunk never moves. A chunk can also
/// be a stretch of memory someone else is done with, which saves faulting in
/// fresh pages: on the scoring machines that costs about 75 ms per gigabyte.
struct Buckets<T: Copy> {
    /// Where each partition's next record goes.
    next: Vec<*mut T>,
    /// Each partition's newest chunk, and how many chunks it has.
    head: Vec<u32>,
    chunks: Vec<u32>,
    slabs: Vec<Vec<u8>>,
    /// Every chunk's first record, and the chunk before it in its partition.
    bases: Vec<*mut T>,
    previous: Vec<u32>,
    /// Chunks carved from our own slabs so far.
    carved: usize,
    /// Donated memory, each stretch a whole aligned chunk.
    spare: Vec<*mut T>,
}

unsafe impl<T: Copy + Send> Send for Buckets<T> {}
unsafe impl<T: Copy + Send> Sync for Buckets<T> {}

impl<T: Copy> Buckets<T> {
    /// Bytes of a chunk that hold records.
    const FILL: usize = (CHUNK_BYTES - 1) / std::mem::size_of::<T>() * std::mem::size_of::<T>();
    /// Records per chunk.
    const CAPACITY: usize = Self::FILL / std::mem::size_of::<T>();
    const FITS: () = assert!(Self::CAPACITY < 1 << OFFSET_BITS && CHUNK_BYTES % std::mem::align_of::<T>() == 0);

    fn new(parts: usize) -> Self {
        let () = Self::FITS;
        Self {
            // A position that reads as a full chunk, so the first push grows.
            next: vec![Self::FILL as *mut T; parts],
            head: vec![NONE; parts],
            chunks: vec![0; parts],
            slabs: Vec::new(),
            bases: Vec::new(),
            previous: Vec::new(),
            carved: 0,
            spare: Vec::new(),
        }
    }

    #[inline(always)]
    fn push(&mut self, part: usize, value: T) -> *mut T {
        unsafe {
            let slot = self.next.as_mut_ptr().add(part);
            let mut at = *slot;
            if at as usize & (CHUNK_BYTES - 1) == Self::FILL {
                at = self.grow(part);
            }
            // Each worker owns its chunks. Bring the next destination line
            // into its cache while other partitions are receiving records.
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            std::arch::asm!("prefetchw [{p} + 64]", p = in(reg) at, options(nostack, preserves_flags, readonly));
            at.write(value);
            *slot = at.add(1);
            at
        }
    }

    /// Hands over one whole chunk's memory, aligned to `CHUNK_BYTES`.
    fn donate(&mut self, memory: *mut u8) {
        debug_assert!(memory as usize % CHUNK_BYTES == 0);
        self.spare.push(memory as *mut T);
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, part: usize) -> *mut T {
        let base = match self.spare.pop() {
            Some(base) => base,
            None => {
                if self.carved % SLAB == 0 {
                    let bytes = (SLAB + 1) * CHUNK_BYTES;
                    let slab: Vec<u8> = Vec::with_capacity(bytes);
                    huge(slab.as_ptr(), bytes);
                    self.slabs.push(slab);
                }
                let slab = self.slabs.last().unwrap().as_ptr() as usize;
                let first = (slab + CHUNK_BYTES - 1) & !(CHUNK_BYTES - 1);
                let base = (first + (self.carved % SLAB) * CHUNK_BYTES) as *mut T;
                // Commit only the next 2 MiB of chunks. Click and dedup
                // workers often use a small fraction of their last slab.
                const BATCH: usize = (1 << 21) / CHUNK_BYTES;
                if self.carved % BATCH == 0 && POPULATE.load(Ordering::Relaxed) != 0 {
                    fault_slab(base as *const u8, BATCH * CHUNK_BYTES);
                }
                self.carved += 1;
                base
            }
        };
        let chunk = self.bases.len() as u32;
        self.bases.push(base);
        self.previous.push(self.head[part]);
        self.head[part] = chunk;
        self.chunks[part] += 1;
        base
    }

    /// Gives our own slabs back now, from whichever thread calls: at exit the
    /// kernel would free them page by page on one thread, inside the timed run.
    fn release(&self) {
        for slab in &self.slabs {
            release(slab.as_ptr(), (SLAB + 1) * CHUNK_BYTES);
        }
    }

    /// Appends every chunk of `part` to `out` as (first record, count).
    fn chunks(&self, part: usize, out: &mut Vec<(*mut T, usize)>) {
        let mut chunk = self.head[part];
        if chunk == NONE {
            return;
        }
        let base = self.bases[chunk as usize];
        out.push((base, unsafe { self.next[part].offset_from(base) } as usize));
        loop {
            chunk = self.previous[chunk as usize];
            if chunk == NONE {
                break;
            }
            out.push((self.bases[chunk as usize], Self::CAPACITY));
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Parsed {
    id: u64,
    user: u64,
    /// Offset from the ingest origin, or a tagged index into the worker’s overflow times.
    ingest: u32,
    /// Milliseconds after `window_start`, or `DEAD`.
    ts: u32,
    /// 0, or for a purchase its category in the top 8 bits and price below.
    payload: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    person: u64,
    /// `ts` in the top 32 bits, `payload` in the low 32.
    value: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: u64,
    id: u64,
    /// Milliseconds after `window_start - 7 days`.
    ts: u32,
    campaign: u32,
}

#[derive(Clone, Copy)]
struct RawClick {
    user: u64,
    id: u64,
    ts: u32,
    campaign: u32,
}

#[derive(Clone, Copy)]
struct Touch {
    id: u64,
    ts: u32,
    campaign: u32,
}

// ---------------------------------------------------------------- lookups

/// `prefix` + number IDs in a dense table, anything else in a map. 0 is absent.
struct Dim {
    prefix: &'static [u8],
    dense: Vec<u32>,
    other: HashMap<Vec<u8>, u32>,
}

impl Dim {
    fn new(prefix: &'static [u8]) -> Self {
        Self {
            prefix,
            dense: Vec::new(),
            other: HashMap::new(),
        }
    }

    fn insert(&mut self, name: &[u8], number: usize, value: u32) {
        if number == usize::MAX {
            self.other.insert(name.to_vec(), value);
        } else {
            if self.dense.len() <= number {
                self.dense.resize((number + 1).max(self.dense.len() * 2), 0);
            }
            self.dense[number] = value;
        }
    }

    #[inline(always)]
    unsafe fn get(&self, p: *const u8, len: usize) -> u32 {
        unsafe {
            let number = ts::numbered(p, len, self.prefix);
            if number < self.dense.len() {
                return *self.dense.get_unchecked(number);
            }
            if number != usize::MAX || self.other.is_empty() {
                return 0;
            }
            self.other.get(std::slice::from_raw_parts(p, len)).copied().unwrap_or(0)
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Default)]
struct AliasGroup {
    keys: [u64; 4],
    values: [u64; 4],
}

/// Only aliases need an entry. A missing key is already its canonical name.
struct Links {
    slots: Vec<AliasGroup>,
    shift: u32,
    empty: bool,
}

impl Links {
    fn build(pairs: &[(u64, u64)]) -> Self {
        let capacity = (pairs.len() * 3).next_power_of_two().max(16);
        let mask = capacity - 1;
        let shift = 64 - capacity.trailing_zeros();
        let mut slots = vec![[0u64; 2]; capacity];
        huge(slots.as_ptr(), capacity);
        let mut names: Vec<u64> = Vec::new();
        let home = |key: u64| (key.wrapping_mul(MIX) >> shift) as usize;

        fn find(parent: &mut [u32], mut at: u32) -> u32 {
            loop {
                let up = parent[at as usize];
                if up == at {
                    return at;
                }
                let above = parent[up as usize];
                parent[at as usize] = above;
                at = above;
            }
        }

        // Number the IDs as they appear; each pass below prefetches what the
        // pass will touch a few steps on, since every touch is a cache miss.
        let mut numbered: Vec<[u32; 2]> = Vec::with_capacity(pairs.len());
        for (at, &(a, b)) in pairs.iter().enumerate() {
            if let Some(&(c, d)) = pairs.get(at + 8) {
                prefetch(&slots[home(c)]);
                prefetch(&slots[home(d)]);
            }
            let mut index = [0u32; 2];
            for (side, key) in [a, b].into_iter().enumerate() {
                let mut slot = home(key);
                index[side] = loop {
                    let entry = &mut slots[slot];
                    if entry[0] == key {
                        break entry[1] as u32;
                    }
                    if entry[0] == 0 {
                        *entry = [key, names.len() as u64];
                        names.push(key);
                        break (names.len() - 1) as u32;
                    }
                    slot = (slot + 1) & mask;
                };
            }
            numbered.push(index);
        }
        // Join. The smaller number is the root, so no cycle can form.
        let mut aliases = 0usize;
        let mut parent: Vec<u32> = (0..names.len() as u32).collect();
        for (at, &[a, b]) in numbered.iter().enumerate() {
            if let Some(&[c, d]) = numbered.get(at + 8) {
                prefetch(&parent[c as usize]);
                prefetch(&parent[d as usize]);
            }
            let a = find(&mut parent, a);
            let b = find(&mut parent, b);
            if a != b {
                aliases += 1;
                let root = a.min(b) as usize;
                let child = a.max(b) as usize;
                parent[child] = root as u32;
                if keys::compare(names[child], names[root]) == Order::Less {
                    names[root] = names[child];
                }
            }
        }
        // Parents always have smaller indices. In ascending order their
        // canonical names have already been resolved, even along long chains.
        for (id, &up) in parent.iter().enumerate() {
            names[id] = names[up as usize];
        }
        let count = aliases.div_ceil(2).next_power_of_two().max(2);
        let shift = 64 - count.trailing_zeros();
        let mut groups = vec![AliasGroup::default(); count];
        huge(groups.as_ptr(), count);
        for [key, id] in slots {
            if key == 0 { continue; }
            let person = names[id as usize];
            if key == person { continue; }
            let mut at = (key.wrapping_mul(MIX) >> shift) as usize;
            loop {
                let group = &mut groups[at];
                if let Some(lane) = group.keys.iter().position(|&k| k == 0) {
                    group.keys[lane] = key;
                    group.values[lane] = person;
                    break;
                }
                at = (at + 1) & (count - 1);
            }
        }
        Self { slots: groups, shift, empty: aliases == 0 }
    }

    #[inline(always)]
    fn prefetch(&self, key: u64) {
        if !self.empty {
            prefetch(unsafe { self.slots.get_unchecked((key.wrapping_mul(MIX) >> self.shift) as usize) });
        }
    }

    #[inline(always)]
    fn person(&self, key: u64) -> u64 {
        if self.empty {
            return key;
        }
        let mask = self.slots.len() - 1;
        let mut slot = (key.wrapping_mul(MIX) >> self.shift) as usize;
        loop {
            let group = unsafe { self.slots.get_unchecked(slot) };
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            unsafe {
                use std::arch::x86_64::*;
                let keys = _mm256_load_si256(group.keys.as_ptr().cast());
                let matches = _mm256_cmpeq_epi64(keys, _mm256_set1_epi64x(key as i64));
                let found = _mm256_movemask_pd(_mm256_castsi256_pd(matches));
                if found != 0 { return group.values[found.trailing_zeros() as usize]; }
            }
            #[cfg(target_arch = "aarch64")]
            unsafe {
                use std::arch::aarch64::*;
                let k = vdupq_n_u64(key);
                let p = group.keys.as_ptr();
                let cmp0 = vceqq_u64(vld1q_u64(p), k);
                let cmp1 = vceqq_u64(vld1q_u64(p.add(2)), k);
                if vgetq_lane_u64(cmp0, 0) != 0 { return group.values[0]; }
                if vgetq_lane_u64(cmp0, 1) != 0 { return group.values[1]; }
                if vgetq_lane_u64(cmp1, 0) != 0 { return group.values[2]; }
                if vgetq_lane_u64(cmp1, 1) != 0 { return group.values[3]; }
            }
            #[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), target_arch = "aarch64")))]
            if let Some(lane) = group.keys.iter().position(|&k| k == key) {
                return group.values[lane];
            }
            if group.keys[3] == 0 { return key; }
            slot = (slot + 1) & mask;
        }
    }
}

/// Asks for transparent huge pages under `count` values at `start`: fewer
/// page faults filling them and fewer TLB misses scattering into them.
pub fn huge<T>(start: *const T, count: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        const MADV_HUGEPAGE: i32 = 14;
        let bytes = count * std::mem::size_of::<T>();
        let from = (start as usize + 4095) & !4095;
        let to = (start as usize + bytes) & !4095;
        if to > from + (1 << 21) && HUGE.load(Ordering::Relaxed) != 0 {
            madvise(from as *mut _, to - from, MADV_HUGEPAGE);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (start, count);
}

/// Tunables the diagnostic run flips; the defaults are what a scored run uses.
pub static HUGE: AtomicUsize = AtomicUsize::new(1);
/// How long the one thread that builds the links table spent on it.
static LINKS_MICROS: AtomicUsize = AtomicUsize::new(0);
pub static POPULATE: AtomicUsize = AtomicUsize::new(0);

/// Faults every page under `bytes` at `start` before the first store there.
fn fault_slab(start: *const u8, bytes: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        let page = 4096usize;
        let start = start as usize;
        let from = (start + page - 1) & !(page - 1);
        let to = (start + bytes) & !(page - 1);
        if to > from {
            // MADV_POPULATE_WRITE. Older kernels fall back to normal faults.
            madvise(from as *mut _, to - from, 23);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (start, bytes);
}

/// Drops the pages under `count` values at `start` (MADV_DONTNEED).
pub fn release<T>(start: *const T, count: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        let bytes = count * std::mem::size_of::<T>();
        let from = (start as usize + 4095) & !4095;
        let to = (start as usize + bytes) & !4095;
        if to > from {
            madvise(from as *mut _, to - from, 4);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (start, count);
}

#[inline(always)]
fn prefetch<T>(target: &T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch(target as *const T as *const i8, _MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) target as *const T, options(nostack, preserves_flags, readonly));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = target;
}

// ---------------------------------------------------------------- shared

struct Work {
    events: bool,
    /// Diagnostic runs parse the data several times over; this copy's number
    /// is mixed into its IDs so the copies do not collapse into duplicates.
    copy: u64,
    file: usize,
    start: u64,
    end: u64,
}

struct Shared {
    window_start: i64,
    window_end: i64,
    products: Dim,
    unknown: u32,
    categories: Vec<Vec<u8>>,
    campaigns: Dim,
    campaign_names: Vec<Vec<u8>>,
    campaign_channels: Vec<Vec<u8>>,
    files: Vec<Source>,
    work: Vec<Work>,
    next: AtomicUsize,
    link_files: Vec<PathBuf>,
    id_bits: u32,
    defer_long_events: bool,
    person_bits: u32,
    ahead: usize,
    links: OnceLock<Links>,
    output: PathBuf,
    /// Days from the first day a session can start on to the last, inclusive.
    days: usize,
    /// Milliseconds from the midnight before `window_start` to `window_start`.
    day_offset: u64,
    first_day: i64,
}

fn csv_files(folder: &Path) -> io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    if !folder.is_dir() {
        return Ok(found);
    }
    for entry in fs::read_dir(folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "csv") {
            continue;
        }
        let kind = entry.file_type()?;
        if kind.is_file() || (kind.is_symlink() && path.is_file()) {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

/// Visit the fields in the padded CSV block while it is still alive.
fn dimension_rows(path: &Path, prefix: &[u8], mut visit: impl FnMut(&[u8], usize, &[u8])) -> io::Result<()> {
    let source = Source::open(path, false)?;
    if source.size != 0 {
        let mut block = Block::new();
        block.load(&source, 0, source.size)?;
        block.lines::<2>(|buf, fields| unsafe {
            let bytes = |at: usize| {
                let (from, to) = fields[at];
                std::slice::from_raw_parts(buf.add(from as usize), (to - from) as usize)
            };
            let name = bytes(0);
            visit(name, ts::numbered(name.as_ptr(), name.len(), prefix), bytes(1));
        });
    }
    Ok(())
}

fn manifest_time(text: &str, name: &str) -> i64 {
    let after = &text[text.find(&format!("\"{name}\"")).expect("manifest key") + name.len() + 2..];
    let after = &after[after.find('"').expect("manifest value") + 1..];
    let value = &after[..after.find('"').expect("manifest value")];
    let mut padded = value.as_bytes().to_vec();
    padded.extend_from_slice(&[0; 64]);
    let parsed = unsafe { ts::parse(padded.as_ptr(), value.len(), &mut ts::Dates::new()) };
    assert!(parsed != ts::BAD, "manifest {name}");
    parsed
}

// ---------------------------------------------------------------- phase 1

fn read_links(shared: &Shared, block: &mut Block, worker: usize, threads: usize) -> io::Result<Vec<(u64, u64)>> {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    for path in shared.link_files.iter().skip(worker).step_by(threads) {
        let source = Source::open(path, true)?;
        let size = source.size;
        let mut start = 0;
        while start < size {
            let end = (start + LINK_BLOCK).min(size);
            block.load(&source, start, end)?;
            let mut queued = Vec::with_capacity(32);
            let drain = |queued: &mut Vec<(*const u8, u64, usize, bool)>, pairs: &mut Vec<(u64,u64)>| {
                for (name, hash, row, right) in queued.drain(..) {
                    let user = unsafe { keys::finish_long_user(name, hash) };
                    if right { pairs[row].1 = user; } else { pairs[row].0 = user; }
                }
            };
            block.raw_lines::<2>(|buf, start, seps| unsafe {
                let (left, nl) = scan::field(buf, start, seps, 0);
                let (right, nr) = scan::field(buf, start, seps, 1);
                if nl == 0 || nr == 0 { return; }
                let row = pairs.len();
                let mut user = |p,n,is_right| {
                    if n == 33 && shared.defer_long_events {
                        queued.push((p, keys::prepare_long_user(p), row, is_right));
                        1
                    } else { keys::pack_user(p,n) }
                };
                let a = user(left,nl,false);
                let b = user(right,nr,true);
                pairs.push((a,b));
                if queued.len() >= 30 { drain(&mut queued, &mut pairs); }
            });
            drain(&mut queued, &mut pairs);
            start = end;
        }
    }
    pairs.retain(|&(a,b)| a != 0 && b != 0 && a != b);
    Ok(pairs)
}

#[inline(always)]
fn person_part(person: u64, bits: u32) -> usize {
    (person.wrapping_mul(MIX) >> (64 - bits)) as usize
}

const LONG_EVENT: u64 = 3 << 62;
const EVENT_FINGERPRINT: u64 = (1 << 30) - 1;

/// A hash selects the dedup bucket. Collisions are resolved by all 33 bytes.
#[inline(always)]
fn event_fingerprint(bytes: &[u8; 33]) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    unsafe {
        use std::arch::x86_64::_mm_crc32_u64;
        let mut crc = u64::from(bytes[0]);
        for word in bytes[1..].chunks_exact(8) {
            crc = _mm_crc32_u64(crc, u64::from_le_bytes(word.try_into().unwrap()));
        }
        crc & EVENT_FINGERPRINT
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.2")))]
    {
        let mixed = keys::long_name_mix(bytes) ^ u64::from(bytes[0]);
        (mixed ^ (mixed >> 32)) & EVENT_FINGERPRINT
    }
}

struct EventSource {
    overflow_times: Vec<i64>,
    buckets: Buckets<Parsed>,
    names: Vec<[u8; 33]>,
}

// Inline offsets cover nearly 25 days starting one day before the window.
// Other timestamps retain all 64 bits in the parsing worker's side table.
const OVERFLOW_TIME: u32 = 1 << 31;

#[inline]
fn encode_ingest(time: i64, origin: i64, overflow: &mut Vec<i64>) -> u32 {
    let offset = time.wrapping_sub(origin) as u64;
    if offset < u64::from(OVERFLOW_TIME) {
        return offset as u32;
    }
    let index = overflow.len();
    assert!(index < OVERFLOW_TIME as usize);
    overflow.push(time);
    OVERFLOW_TIME | index as u32
}

#[inline]
fn decode_ingest(encoded: u32, origin: i64, overflow: &[i64]) -> i64 {
    if encoded < OVERFLOW_TIME {
        origin.wrapping_add(i64::from(encoded))
    } else {
        overflow[(encoded ^ OVERFLOW_TIME) as usize]
    }
}

fn parse_events(shared: &Shared, block: &Block, copy: u64, dates: &mut ts::Dates, out: &mut EventSource) {
    let id_shift = 64 - shared.id_bits;
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    let window_start = shared.window_start;
    let window_end = shared.window_end;
    const LOOKUPS: usize = 16;
    let mut lookups = [(std::ptr::null::<u8>(), std::ptr::null_mut::<Parsed>(), 0u64); LOOKUPS];
    let mut queued = 0;
    let drain = |items: &[(*const u8, *mut Parsed, u64)]| {
        for &(name,dest,mixed) in items {
            unsafe { (*dest).user = keys::finish_long_user(name,mixed); }
        }
    };
    block.raw_lines::<7>(|buf, start, seps| unsafe {
        let field = |at: usize| scan::field(buf, start, seps, at);
        let (p, n) = field(0);
        if n == 0 {
            return;
        }
        let (id, partition_hash) = if shared.defer_long_events {
            if n <= 9 && *p == b'e' && *p.add(n - 1) != 0 && (n < 9 || *p.add(8) < 128) {
                let suffix = u64::from_le((p.add(1) as *const u64).read_unaligned());
                let mask = u64::MAX.checked_shr((9 - n) as u32 * 8).unwrap_or(0);
                let id = (suffix & mask) ^ copy;
                (id, id.wrapping_mul(MIX))
            } else if n == 33 {
                let bytes = &*p.cast::<[u8; 33]>();
                let fingerprint = event_fingerprint(bytes);
                let index = u32::try_from(out.names.len()).expect("too many long event IDs in one worker");
                out.names.push(*bytes);
                (LONG_EVENT | (fingerprint << 32) | index as u64, fingerprint.wrapping_mul(MIX))
            } else {
                let id = keys::pack(p, n);
                if id == 0 {
                    return;
                }
                let id = id ^ copy;
                (id, id.wrapping_mul(MIX))
            }
        } else {
            let id = keys::pack(p, n);
            if id == 0 {
                return;
            }
            let id = id ^ copy;
            (id, id.wrapping_mul(MIX))
        };
        let (p, n) = field(3);
        let ingest = ts::ingest(p, n, dates);
        if ingest == ts::BAD {
            return;
        }
        let (p, n) = field(2);
        let at = ts::parse(p, n, dates);
        let (p, n) = field(4);
        let purchase = n == 8 && (p as *const u64).read_unaligned() == PURCHASE.to_le();
        let other = n == 4 && {
            let word = u32::from_le((p as *const u32).read_unaligned());
            word == VIEW || word == CART
        };
        let (prod_p, prod_n) = field(5);
        let in_window = at >= window_start && at < window_end && ingest - at <= DAY;
        let mut valid = in_window && (purchase || other) && prod_n > 0;
        let mut user = 0;
        let mut payload = 0;
        let mut deferred = false;
        let mut mixed = 0;
        let mut name = std::ptr::null();
        if valid {
            let (un, ul) = field(1);
            deferred = ul == 33 && shared.defer_long_events;
            mixed = if deferred { keys::prepare_long_user(un) } else { 0 };
            user = if deferred { 1 } else { keys::pack_user(un, ul) };
            if user == 0 {
                valid = false;
            } else {
                user ^= copy;
                name = un;
                if purchase {
                    let (p, n) = field(6);
                    let price = ts::price(p, n);
                    if price == 0 {
                        valid = false;
                    } else {
                        let category = shared.products.get(prod_p, prod_n);
                        let category = if category == 0 { shared.unknown } else { category - 1 };
                        payload = (category << 24) | price;
                    }
                }
            }
        }
        let record = Parsed {
            id,
            user,
            ingest: encode_ingest(ingest, window_start - DAY, &mut out.overflow_times),
            ts: if valid { (at - window_start) as u32 } else { DEAD },
            payload,
        };
        let dest = out.buckets.push((partition_hash >> id_shift) as usize, record);
        if deferred && valid {
            lookups[queued] = (name,dest,mixed);
            queued += 1;
            if queued == LOOKUPS {
                drain(&lookups);
                queued = 0;
            }
        }
    });
    drain(&lookups[..queued]);
}

struct ClickSource {
    buckets: Buckets<Click>,
    names: Vec<[u8; 33]>,
    worker: usize,
}

fn click_order(a: u64, b: u64, sources: &[ClickSource], local: bool) -> std::cmp::Ordering {
    if !local || (a & LONG_EVENT != LONG_EVENT && b & LONG_EVENT != LONG_EVENT) {
        return keys::compare(a, b);
    }
    fn name<'a>(key: u64, sources: &'a [ClickSource], scratch: &'a mut [u8; 9]) -> &'a [u8] {
        if key & LONG_EVENT == LONG_EVENT {
            &sources[((key & !LONG_EVENT) >> 32) as usize].names[key as u32 as usize]
        } else {
            keys::bytes(key, scratch)
        }
    }
    name(a, sources, &mut [0; 9]).cmp(name(b, sources, &mut [0; 9]))
}

fn parse_clicks(
    shared: &Shared,
    block: &Block,
    copy: u64,
    dates: &mut ts::Dates,
    pending: &mut Vec<RawClick>,
    out: &mut ClickSource,
) {
    let earliest = shared.window_start - WEEK;
    let window_end = shared.window_end;
    pending.clear();
    let mut queued = Vec::with_capacity(16);
    let drain = |queued: &mut Vec<(*const u8,u64,usize)>, pending: &mut Vec<RawClick>| {
        for (name, hash, index) in queued.drain(..) {
            pending[index].user = unsafe { keys::finish_long_user(name,hash) };
        }
    };
    block.raw_lines::<4>(|buf, start, seps| unsafe {
        let field = |at: usize| scan::field(buf, start, seps, at);
        let (p, n) = field(3);
        let at = ts::parse(p, n, dates);
        if at < earliest || at >= window_end {
            return;
        }
        let (p, n) = field(2);
        let campaign = shared.campaigns.get(p, n);
        if campaign == 0 {
            return;
        }
        let (p, n) = field(0);
        let id = if n == 33 && shared.defer_long_events {
            let index = u32::try_from(out.names.len()).expect("too many long click IDs in one worker");
            out.names.push(*p.cast::<[u8; 33]>());
            LONG_EVENT | (out.worker as u64) << 32 | index as u64
        } else {
            keys::pack(p, n)
        };
        let (name, n) = field(1);
        let deferred = n == 33 && shared.defer_long_events;
        let hash = if deferred { keys::prepare_long_user(name) } else { 0 };
        let user = if deferred { 1 } else { keys::pack_user(name,n) };
        if id == 0 || user == 0 {
            return;
        }
        let (id, user) = (id ^ copy, user ^ copy);
        let index = pending.len();
        pending.push(RawClick {
            user,
            id,
            ts: (at - earliest) as u32,
            campaign,
        });
        if deferred {
            queued.push((name, hash, index));
            if queued.len() == 16 { drain(&mut queued,pending); }
        }
    });
    drain(&mut queued,pending);
    let links = loop {
        match shared.links.get() {
            Some(links) => break links,
            None => std::thread::yield_now(),
        }
    };
    for at in 0..pending.len() {
        if let Some(ahead) = pending.get(at + shared.ahead) {
            links.prefetch(ahead.user);
        }
        let click = pending[at];
        let person = links.person(click.user);
        out.buckets.push(
            person_part(person, shared.person_bits),
            Click {
                person,
                id: click.id,
                ts: click.ts,
                campaign: click.campaign,
            },
        );
    }
}

type LinkShards = std::sync::Mutex<Vec<Vec<(u64, u64)>>>;

fn parse_worker(shared: &Shared, worker: usize, threads: usize, shards: &LinkShards) -> io::Result<(EventSource, ClickSource)> {
    let mut block = Block::new();
    let began = Instant::now();
    let own = read_links(shared, &mut block, worker, threads).unwrap_or_else(|error| {
        eprintln!("links: {error}");
        std::process::exit(1);
    });
    let ready = {
        let mut parts = shards.lock().unwrap();
        parts.push(own);
        (parts.len() == threads).then(|| std::mem::take(&mut *parts))
    };
    if let Some(parts) = ready {
        let mut pairs = Vec::with_capacity(parts.iter().map(Vec::len).sum());
        for mut shard in parts { pairs.append(&mut shard); }
        let links = Links::build(&pairs);
        let _ = shared.links.set(links);
        LINKS_MICROS.store(began.elapsed().as_micros() as usize, Ordering::Relaxed);
    }
    let mut events = EventSource {
        buckets: Buckets::new(1 << shared.id_bits),
        names: Vec::new(),
        overflow_times: Vec::new(),
    };
    let mut clicks = ClickSource { buckets: Buckets::new(1 << shared.person_bits), names: Vec::new(), worker };
    let mut pending = Vec::new();
    let mut dates = ts::Dates::new();
    while let Some(work) = shared.work.get(shared.next.fetch_add(1, Ordering::Relaxed)) {
        block.load(&shared.files[work.file], work.start, work.end)?;
        if work.events {
            parse_events(shared, &block, work.copy, &mut dates, &mut events);
        } else {
            parse_clicks(shared, &block, work.copy, &mut dates, &mut pending, &mut clicks);
        }
    }
    Ok((events, clicks))
}

// ---------------------------------------------------------------- phase 2

fn dedup_worker(shared: &Shared, sources: &[EventSource], next: &AtomicUsize) -> Buckets<Event> {
    let links = shared.links.get().unwrap();
    // The events go into the memory of the parsed records they came from: a
    // parsed chunk, once read, is room for one chunk of events.
    let mut out = Buckets::new(1 << shared.person_bits);
    let (ahead, person_bits) = (shared.ahead, shared.person_bits);
    let mut chunks: Vec<(*mut Parsed, usize)> = Vec::new();
    let mut owners = Vec::new();
    let mut table: Vec<u32> = Vec::new();
    let mut used_slots: Vec<usize> = Vec::new();
    loop {
        let part = next.fetch_add(1, Ordering::Relaxed);
        if part >= 1 << shared.id_bits {
            break;
        }
        chunks.clear();
        owners.clear();
        for (owner, source) in sources.iter().enumerate() {
            source.buckets.chunks(part, &mut chunks);
            owners.resize(chunks.len(), owner);
        }
        let total: usize = chunks.iter().map(|chunk| chunk.1).sum();
        if total == 0 {
            continue;
        }
        let needed = (total * 2 + 16).next_power_of_two();
        if needed > table.len() {
            table.resize(needed, NONE);
        }
        let capacity = table.len();
        let mask = capacity - 1;
        unsafe {
            for (index, &(base, count)) in chunks.iter().enumerate() {
                for offset in 0..count {
                    let record = base.add(offset);
                    let id = (*record).id;
                    let entry = ((index << OFFSET_BITS) | offset) as u32;
                    let long = shared.defer_long_events && id & LONG_EVENT == LONG_EVENT;
                    let hash_key = if long { (id >> 32) & EVENT_FINGERPRINT } else { id };
                    let mut slot = (hash_key.wrapping_mul(MIX) >> 24) as usize & mask;
                    loop {
                        let seen = *table.get_unchecked(slot);
                        if seen == NONE {
                            *table.get_unchecked_mut(slot) = entry;
                            used_slots.push(slot);
                            break;
                        }
                        let other = chunks
                            .get_unchecked((seen >> OFFSET_BITS) as usize)
                            .0
                            .add((seen & ((1 << OFFSET_BITS) - 1)) as usize);
                        let other_id = (*other).id;
                        let equal = if long {
                            // Equal fingerprints select candidates. Only the full bytes
                            // decide whether these two records are duplicates.
                            id >> 32 == other_id >> 32
                                && sources[owners[index]].names[id as u32 as usize]
                                    == sources[owners[(seen >> OFFSET_BITS) as usize]].names[other_id as u32 as usize]
                        } else {
                            id == other_id
                        };
                        if equal {
                            let a = (*record).ingest;
                            let b = (*other).ingest;
                            let earlier = if (a | b) < OVERFLOW_TIME {
                                a < b
                            } else {
                                let origin = shared.window_start - DAY;
                                decode_ingest(a, origin, &sources[owners[index]].overflow_times)
                                    < decode_ingest(b, origin, &sources[owners[(seen >> OFFSET_BITS) as usize]].overflow_times)
                            };
                            if earlier {
                                (*other).ts = DEAD;
                                *table.get_unchecked_mut(slot) = entry;
                            } else {
                                (*record).ts = DEAD;
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            for &(base, count) in &chunks {
                for offset in 0..count {
                    if offset + ahead < count {
                        links.prefetch((*base.add(offset + ahead)).user);
                    }
                    let record = *base.add(offset);
                    if record.ts == DEAD {
                        continue;
                    }
                    let person = links.person(record.user);
                    let value = ((record.ts as u64) << 32) | record.payload as u64;
                    out.push(person_part(person, person_bits), Event { person, value });
                }
            }
            // Every record of this bucket has been read: its chunks are free.
            for &(base, _) in &chunks {
                out.donate(base as *mut u8);
            }
            for &slot in &used_slots {
                *table.get_unchecked_mut(slot) = NONE;
            }
            used_slots.clear();
        }
    }
    out
}

// ---------------------------------------------------------------- phase 3

const USER_COLUMNS: [(&str, pq::Kind); 6] = [
    ("user_id", pq::Kind::Keyed),
    ("day", pq::Kind::Date),
    ("sessions", pq::Kind::Int32),
    ("events", pq::Kind::Int32),
    ("revenue_cents", pq::Kind::Int64),
    ("top_category", pq::Kind::Category),
];

/// Per campaign and day: credited sessions, of those purchasing, revenue, assists.
type Totals = [i64; 4];

struct Sessions<'a> {
    shared: &'a Shared,
    writer: Option<pq::Writer>,
    thread: usize,
    totals: Vec<Totals>,
    /// Clicks of each campaign inside the current 7-day reach.
    reach: Vec<u32>,
    place: Vec<u32>,
    active: Vec<u32>,
    sums: [u64; 256],
    touched: Vec<u8>,
}

impl Sessions<'_> {
    #[inline(always)]
    fn row(&mut self, person: u64, day: u32, sessions: u32, events: u32, revenue: u64) -> io::Result<()> {
        let shared = self.shared;
        if self.writer.is_none() {
            let path = shared
                .output
                .join("user_daily")
                .join(format!("part-{:03}.parquet", self.thread));
            self.writer = Some(pq::Writer::create(
                &path,
                &USER_COLUMNS,
                GROUP_ROWS,
                &shared.categories,
            )?);
        }
        let writer = self.writer.as_mut().unwrap();
        writer.keyed(0, person, |name| keys::unpack(person, name));
        writer.int(1, shared.first_day + day as i64);
        writer.int(2, sessions as i64);
        writer.int(3, events as i64);
        writer.int(4, revenue as i64);
        let top: Option<u8> = match self.touched.as_slice() {
            [] => None,
            &[cat] => {
                self.sums[cat as usize] = 0;
                self.touched.clear();
                Some(cat)
            }
            slice => {
                let mut best = slice[0] as usize;
                let mut best_sum = self.sums[best];
                for &cat in &slice[1..] {
                    let cat = cat as usize;
                    let sum = self.sums[cat];
                    if sum > best_sum || (sum == best_sum && cat < best) {
                        best = cat;
                        best_sum = sum;
                    }
                }
                for &cat in slice {
                    self.sums[cat as usize] = 0;
                }
                self.touched.clear();
                Some(best as u8)
            }
        };
        writer.category(5, top);
        writer.end_row();
        Ok(())
    }

    /// One person: their events sorted by time, their clicks by time then ID.
    fn person(&mut self, person: u64, events: &[u64], clicks: &[Touch]) -> io::Result<()> {
        let days = self.shared.days;
        let day_offset = self.shared.day_offset;
        let (mut low, mut high) = (0usize, 0usize);
        let mut at = 0;
        let mut current = NONE;
        let (mut day_sessions, mut day_events, mut day_revenue) = (0u32, 0u32, 0u64);
        while at < events.len() {
            let start = events[at] >> 32;
            let day = ((start + day_offset) / DAY as u64) as u32;
            if day != current {
                if current != NONE {
                    self.row(person, current, day_sessions, day_events, day_revenue)?;
                }
                current = day;
                (day_sessions, day_events, day_revenue) = (0, 0, 0);
            }
            let mut previous = start;
            let mut revenue = 0u64;
            let mut purchased = false;
            let first = at;
            while at < events.len() {
                let value = events[at];
                let time = value >> 32;
                if time - previous > SESSION_GAP {
                    break;
                }
                previous = time;
                let price = value & 0xFF_FFFF;
                if price != 0 {
                    purchased = true;
                    revenue += price;
                    let category = ((value >> 24) & 0xFF) as usize;
                    if self.sums[category] == 0 {
                        self.touched.push(category as u8);
                    }
                    self.sums[category] += price;
                }
                at += 1;
            }
            day_sessions += 1;
            day_events += (at - first) as u32;
            day_revenue += revenue;

            let mut credited = 0usize;
            if !clicks.is_empty() {
                // A click's `ts` counts from 7 days before the events' zero, so
                // the clicks in reach are those with start <= ts <= start + 7 days.
                let limit = start + WEEK as u64;
                while high < clicks.len() && clicks[high].ts as u64 <= limit {
                    let campaign = clicks[high].campaign as usize;
                    if self.reach[campaign] == 0 {
                        self.place[campaign] = self.active.len() as u32;
                        self.active.push(campaign as u32);
                    }
                    self.reach[campaign] += 1;
                    high += 1;
                }
                while low < high && (clicks[low].ts as u64) < start {
                    let campaign = clicks[low].campaign as usize;
                    self.reach[campaign] -= 1;
                    if self.reach[campaign] == 0 {
                        let hole = self.place[campaign] as usize;
                        let moved = self.active.pop().unwrap();
                        if hole < self.active.len() {
                            self.active[hole] = moved;
                            self.place[moved as usize] = hole as u32;
                        }
                    }
                    low += 1;
                }
                if high > low {
                    let latest = clicks[high - 1].ts;
                    let mut pick = high - 1;
                    while pick > low && clicks[pick - 1].ts == latest {
                        pick -= 1;
                    }
                    credited = clicks[pick].campaign as usize;
                }
                for &campaign in &self.active {
                    self.totals[campaign as usize * days + day as usize][3] += 1;
                }
            }
            let totals = &mut self.totals[credited * days + day as usize];
            totals[0] += 1;
            totals[1] += i64::from(purchased);
            totals[2] += revenue as i64;
        }
        if current != NONE {
            self.row(person, current, day_sessions, day_events, day_revenue)?;
        }
        for campaign in self.active.drain(..) {
            self.reach[campaign as usize] = 0;
        }
        Ok(())
    }
}

/// Sorts many events of one person by their `ts` (the top 32 bits): three
/// counting passes of 11 bits. Whales have a million events each.
fn sort_by_time(values: &mut [u64], scratch: &mut Vec<u64>) {
    scratch.clear();
    scratch.resize(values.len(), 0);
    let mut histograms = [[0u32; 2048]; 3];
    for &value in values.iter() {
        histograms[0][(value >> 32) as usize & 2047] += 1;
        histograms[1][(value >> 43) as usize & 2047] += 1;
        histograms[2][(value >> 54) as usize] += 1;
    }
    let (mut from, mut to) = (&mut values[..], &mut scratch[..]);
    for (shift, counts) in [32, 43, 54].into_iter().zip(&mut histograms) {
        let mut sum = 0;
        for count in counts.iter_mut() {
            let here = *count;
            *count = sum;
            sum += here;
        }
        for &value in from.iter() {
            let slot = &mut counts[(value >> shift) as usize & 2047];
            to[*slot as usize] = value;
            *slot += 1;
        }
        std::mem::swap(&mut from, &mut to);
    }
    // Three passes: the sorted values are in the scratch buffer.
    to.copy_from_slice(from);
}

/// Sort up to 32 timestamps with register comparisons. Padding sorts to the
/// end and masked loads and stores touch only the person's own records.
#[cfg(all(target_arch="x86_64", target_feature="avx512f"))]
#[inline(always)]
unsafe fn sort_short_times(values: &mut [u64]) {
    use std::arch::x86_64::*;
    #[inline(always)]
    unsafe fn step<const SELECT: u8>(v: __m512i, order: __m512i) -> __m512i {
        let other = _mm512_permutexvar_epi64(order,v);
        _mm512_mask_max_epu64(_mm512_min_epu64(v,other),SELECT,v,other)
    }
    let swap1 = _mm512_setr_epi64(1,0,3,2,5,4,7,6);
    let swap2 = _mm512_setr_epi64(2,3,0,1,6,7,4,5);
    let swap4 = _mm512_setr_epi64(4,5,6,7,0,1,2,3);
    let reverse = _mm512_setr_epi64(7,6,5,4,3,2,1,0);
    let finish = |v| step::<0xaa>(step::<0xcc>(step::<0xf0>(v,swap4),swap2),swap1);
    let sort = |v| finish(step::<0x5a>(step::<0x3c>(step::<0x66>(v,swap1),swap2),swap1));
    let pair = |a,b| {
        let br = _mm512_permutexvar_epi64(reverse,b);
        (finish(_mm512_min_epu64(a,br)), finish(_mm512_max_epu64(a,br)))
    };
    let n = values.len();
    let mask = |i:usize| (0xffu16 >> (8-n.saturating_sub(i).min(8))) as u8;
    let p = values.as_mut_ptr();
    let read = |i:usize| sort(_mm512_mask_loadu_epi64(_mm512_set1_epi64(-1),mask(i),p.wrapping_add(i).cast()));
    let write = |i:usize,v| _mm512_mask_storeu_epi64(p.wrapping_add(i).cast(),mask(i),v);
    let a = read(0);
    if values.len() <= 8 { write(0,a); return; }
    let (a,b) = pair(a,read(8));
    if values.len() <= 16 { write(0,a); write(8,b); return; }
    let (c,d) = pair(read(16),read(24));
    let c = _mm512_permutexvar_epi64(reverse,c);
    let d = _mm512_permutexvar_epi64(reverse,d);
    let (al,ah) = (_mm512_min_epu64(a,d),_mm512_max_epu64(a,d));
    let (bl,bh) = (_mm512_min_epu64(b,c),_mm512_max_epu64(b,c));
    write(0,finish(_mm512_min_epu64(al,bl)));
    write(8,finish(_mm512_max_epu64(al,bl)));
    write(16,finish(_mm512_min_epu64(ah,bh)));
    write(24,finish(_mm512_max_epu64(ah,bh)));
}

#[cfg(all(target_arch="x86_64",target_feature="avx512f"))]
fn sort_medium_times(values: &mut [u64], tmp: &mut [u64], depth: usize) {
    use std::arch::x86_64::*;
    let n=values.len();
    if n<=32 {
        if n>=5 {unsafe {sort_short_times(values)}} else {values.sort_unstable();}
        return;
    }
    if depth==0 { values.sort_unstable(); return; }
    let (a,b,c)=(values[0],values[n/2],values[n-1]);
    let pivot=a.min(b).max(a.max(b).min(c));
    let (mut lo,mut hi)=(0,n);
    unsafe {
        let bound=_mm512_set1_epi64(pivot as i64);
        for i in (0..n).step_by(8) {
            let count=(n-i).min(8);
            let valid=(0xffu16>>(8-count)) as u8;
            let v=_mm512_maskz_loadu_epi64(valid,values.as_ptr().add(i).cast());
            let below=_mm512_cmplt_epu64_mask(v,bound)&valid;
            let small=below.count_ones() as usize;
            hi-=count-small;
            _mm512_mask_compressstoreu_epi64(tmp.as_mut_ptr().add(lo).cast(),below,v);
            _mm512_mask_compressstoreu_epi64(tmp.as_mut_ptr().add(hi).cast(),valid & !below,v);
            lo+=small;
        }
    }
    if lo==0 || lo==n { values.sort_unstable(); return; }
    values.copy_from_slice(&tmp[..n]);
    let (left,right)=values.split_at_mut(lo);
    let (l,r)=tmp.split_at_mut(lo);
    sort_medium_times(left,l,depth-1);
    sort_medium_times(right,r,depth-1);
}

fn session_worker(
    shared: &Shared,
    events: &[Buckets<Event>],
    clicks: &[ClickSource],
    order: &[u32],
    next: &AtomicUsize,
    thread: usize,
) -> io::Result<(Vec<Totals>, bool)> {
    let campaigns = shared.campaign_names.len();
    let mut sessions = Sessions {
        shared,
        writer: None,
        thread,
        totals: vec![[0; 4]; campaigns * shared.days],
        reach: vec![0; campaigns],
        place: vec![0; campaigns],
        active: Vec::new(),
        sums: [0; 256],
        touched: Vec::new(),
    };
    let mut event_chunks: Vec<(*mut Event, usize)> = Vec::new();
    let mut click_chunks: Vec<(*mut Click, usize)> = Vec::new();
    let mut table_keys: Vec<u64> = Vec::new();
    let mut table_ids: Vec<u32> = Vec::new();
    let mut used_slots: Vec<usize> = Vec::new();
    let mut persons: Vec<u64> = Vec::new();
    let mut ends: Vec<u32> = Vec::new();
    let mut local: Vec<u32> = Vec::new();
    let mut values: Vec<u64> = Vec::new();
    let mut click_ends: Vec<u32> = Vec::new();
    let mut click_local: Vec<u32> = Vec::new();
    let mut touches: Vec<Touch> = Vec::new();
    let mut scratch: Vec<u64> = Vec::new();
    while let Some(&part) = order.get(next.fetch_add(1, Ordering::Relaxed)) {
        let part = part as usize;
        event_chunks.clear();
        for source in events {
            source.chunks(part, &mut event_chunks);
        }
        let total: usize = event_chunks.iter().map(|chunk| chunk.1).sum();
        if total == 0 {
            continue;
        }

        // Group the events by person: number the people as they appear.
        let needed = (total * 2 + 16).next_power_of_two();
        if needed > table_keys.len() {
            table_keys.resize(needed, 0);
            table_ids.resize(needed, 0);
        }
        let capacity = table_keys.len();
        let mask = capacity - 1;
        persons.clear();
        ends.clear();
        local.clear();
        unsafe {
            for &(base, count) in &event_chunks {
                for offset in 0..count {
                    let person = (*base.add(offset)).person;
                    let mut slot = (person.wrapping_mul(MIX) >> 24) as usize & mask;
                    let id = loop {
                        let key = *table_keys.get_unchecked(slot);
                        if key == person {
                            break *table_ids.get_unchecked(slot);
                        }
                        if key == 0 {
                            let id = persons.len() as u32;
                            *table_keys.get_unchecked_mut(slot) = person;
                            *table_ids.get_unchecked_mut(slot) = id;
                            used_slots.push(slot);
                            persons.push(person);
                            ends.push(0);
                            break id;
                        }
                        slot = (slot + 1) & mask;
                    };
                    *ends.get_unchecked_mut(id as usize) += 1;
                    local.push(id);
                }
            }
            // `ends` holds counts; make it each person's first slot, then fill,
            // which leaves it at each person's end.
            let mut sum = 0u32;
            for end in ends.iter_mut() {
                let count = *end;
                *end = sum;
                sum += count;
            }
            if values.len() < total {
                values.resize(total, 0);
            }
            let mut index = 0;
            for &(base, count) in &event_chunks {
                for offset in 0..count {
                    let end = ends.get_unchecked_mut(*local.get_unchecked(index) as usize);
                    *values.get_unchecked_mut(*end as usize) = (*base.add(offset)).value;
                    *end += 1;
                    index += 1;
                }
            }
        }

        // The clicks of the people who have events, grouped the same way.
        click_chunks.clear();
        for source in clicks {
            source.buckets.chunks(part, &mut click_chunks);
        }
        click_ends.clear();
        click_ends.resize(persons.len(), 0);
        click_local.clear();
        let mut kept = 0usize;
        unsafe {
            for &(base, count) in &click_chunks {
                for offset in 0..count {
                    let person = (*base.add(offset)).person;
                    let mut slot = (person.wrapping_mul(MIX) >> 24) as usize & mask;
                    let id = loop {
                        let key = *table_keys.get_unchecked(slot);
                        if key == person {
                            break *table_ids.get_unchecked(slot);
                        }
                        if key == 0 {
                            break NONE;
                        }
                        slot = (slot + 1) & mask;
                    };
                    click_local.push(id);
                    if id != NONE {
                        *click_ends.get_unchecked_mut(id as usize) += 1;
                        kept += 1;
                    }
                }
            }
            let mut sum = 0u32;
            for end in click_ends.iter_mut() {
                let count = *end;
                *end = sum;
                sum += count;
            }
            if touches.len() < kept {
                touches.resize(
                    kept,
                    Touch {
                        id: 0,
                        ts: 0,
                        campaign: 0,
                    },
                );
            }
            let mut index = 0;
            for &(base, count) in &click_chunks {
                for offset in 0..count {
                    let id = *click_local.get_unchecked(index);
                    index += 1;
                    if id != NONE {
                        let click = *base.add(offset);
                        let end = click_ends.get_unchecked_mut(id as usize);
                        *touches.get_unchecked_mut(*end as usize) = Touch {
                            id: click.id,
                            ts: click.ts,
                            campaign: click.campaign,
                        };
                        *end += 1;
                    }
                }
            }
        }

        for &slot in &used_slots {
            unsafe {
                *table_keys.get_unchecked_mut(slot) = 0;
            }
        }
        used_slots.clear();

        let (mut from, mut click_from) = (0usize, 0usize);
        for (id, &person) in persons.iter().enumerate() {
            let to = ends[id] as usize;
            let click_to = click_ends[id] as usize;
            let mine = &mut values[from..to];
            if mine.len() == 2 {
                if mine[0] > mine[1] {
                    mine.swap(0, 1);
                }
            } else if mine.len() == 3 {
                if mine[0] > mine[1] { mine.swap(0, 1); }
                if mine[1] > mine[2] { mine.swap(1, 2); }
                if mine[0] > mine[1] { mine.swap(0, 1); }
            } else if mine.len() == 4 {
                if mine[0] > mine[1] { mine.swap(0, 1); }
                if mine[2] > mine[3] { mine.swap(2, 3); }
                if mine[0] > mine[2] { mine.swap(0, 2); }
                if mine[1] > mine[3] { mine.swap(1, 3); }
                if mine[1] > mine[2] { mine.swap(1, 2); }
            } else if mine.len() > 4 {
                if mine.len() <= 8 {
                    for i in 1..mine.len() {
                        let mut j = i;
                        while j > 0 && mine[j - 1] > mine[j] {
                            mine.swap(j - 1, j);
                            j -= 1;
                        }
                    }
                } else if mine.len() > 2048 {
                    sort_by_time(mine, &mut scratch);
                } else {
                    #[cfg(all(target_arch="x86_64", target_feature="avx512f"))]
                    if mine.len() <= 32 {
                        if mine.len() >= 5 { unsafe { sort_short_times(mine) } } else { mine.sort_unstable(); }
                    } else {
                        if scratch.len() < mine.len() { scratch.resize(mine.len(), 0); }
                        sort_medium_times(mine, &mut scratch[..mine.len()], 12);
                    }
                    #[cfg(not(all(target_arch="x86_64", target_feature="avx512f")))]
                    mine.sort_unstable();
                }
            }
            let reached = &mut touches[click_from..click_to];
            if reached.len() == 2 {
                let cmp = reached[0].ts.cmp(&reached[1].ts)
                    .then_with(|| click_order(reached[0].id, reached[1].id, clicks, shared.defer_long_events));
                if cmp.is_gt() {
                    reached.swap(0, 1);
                }
            } else if reached.len() > 2 && reached.len() <= 6 {
                for i in 1..reached.len() {
                    let mut j = i;
                    while j > 0 {
                        let cmp = reached[j - 1].ts.cmp(&reached[j].ts)
                            .then_with(|| click_order(reached[j - 1].id, reached[j].id, clicks, shared.defer_long_events));
                        if cmp.is_gt() {
                            reached.swap(j - 1, j);
                            j -= 1;
                        } else {
                            break;
                        }
                    }
                }
            } else if reached.len() > 6 {
                reached.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| click_order(a.id, b.id, clicks, shared.defer_long_events)));
            }
            sessions.person(person, mine, reached)?;
            from = to;
            click_from = click_to;
        }
    }
    let wrote = sessions.writer.is_some();
    if let Some(writer) = sessions.writer.take() {
        writer.finish()?;
    }
    Ok((sessions.totals, wrote))
}

// ---------------------------------------------------------------- main

fn tune(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// One thread per CPU, except under a cgroup CPU quota (the scorer's
/// `cpu.max` is 8 CPUs on a larger host): there, half as many threads again
/// measured about 10% faster, since threads also wait on page faults.
fn default_threads() -> usize {
    let cpus = std::thread::available_parallelism().map_or(8, usize::from);
    let quota = fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default();
    let mut parts = quota.split_whitespace();
    match (
        parts.next().and_then(|max| max.parse::<u64>().ok()),
        parts.next().and_then(|period| period.parse::<u64>().ok()),
    ) {
        (Some(max), Some(period)) if period > 0 && max.div_ceil(period) as usize == cpus => cpus + cpus / 2,
        _ => cpus,
    }
}

fn main() -> io::Result<()> {
    let began = Instant::now();
    if std::env::var_os("SS_DIAG").is_some() && std::env::var_os("SS_CHILD").is_none() {
        diag::run();
    }
    HUGE.store(tune("SS_HUGE", 1) as usize, Ordering::Relaxed);
    POPULATE.store(tune("SS_POPULATE", 0) as usize, Ordering::Relaxed);
    let copies = tune("SS_COPIES", 1).clamp(1, 100);
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or_else(default_threads)
        .min(MAX_THREADS);

    let manifest = fs::read_to_string(data.join("manifest.json"))?;
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");
    assert!(
        window_end > window_start && window_end - window_start < 40 * DAY,
        "window"
    );

    // Categories are numbered in byte order, so the smaller number wins a tie.
    // Products, without an allocation per row: categories are numbered as they
    // appear, then renumbered in byte order so the smaller number wins a tie.
    let mut seen: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut products = Dim::new(b"p");
    dimension_rows(&data.join("products.csv"), products.prefix, |product, number, category| {
        if product.is_empty() {
            return;
        }
        let index = match seen.iter().position(|name| name.as_slice() == category) {
            Some(index) => index,
            None => {
                seen.push(category.to_vec());
                seen.len() - 1
            }
        };
        products.insert(product, number, index as u32 + 1);
    })?;
    assert!(seen.len() <= 256, "more than 256 categories");
    let mut categories = seen.clone();
    categories.sort();
    let renumber: Vec<u32> = seen
        .iter()
        .map(|name| categories.binary_search(name).unwrap() as u32 + 1)
        .collect();
    for value in products.dense.iter_mut().chain(products.other.values_mut()) {
        if *value != 0 {
            *value = renumber[*value as usize - 1];
        }
    }
    let unknown = renumber[0] - 1;

    let mut campaigns = Dim::new(b"c_");
    let mut campaign_names = vec![b"organic".to_vec()];
    let mut campaign_channels = vec![b"organic".to_vec()];
    dimension_rows(&data.join("campaigns.csv"), campaigns.prefix, |campaign, number, channel| {
        if !campaign.is_empty() {
            campaigns.insert(campaign, number, campaign_names.len() as u32);
            campaign_names.push(campaign.to_vec());
            campaign_channels.push(channel.to_vec());
        }
    })?;

    let mut work = Vec::new();
    let mut paths = csv_files(&data.join("events"))?;
    let event_files = paths.len();
    paths.extend(csv_files(&data.join("clicks"))?);
    let mut files = paths.iter().map(|path| Source::open(path, false)).collect::<io::Result<Vec<_>>>()?;
    let total: u64 = files.iter().map(|source| source.size).sum();
    let large = total >= 4u64 << 30;
    let map_by_default = total >= 1 << 30;
    let block = tune(
        "SS_BLOCK",
        if large { 1 << 22 } else if map_by_default { 1 << 20 } else { BLOCK },
    )
    .max(4096);
    let map = tune("SS_MAP", 1) != 0;
    // Release mapped working sets in parallel so exit does not reclaim
    // every input page on one thread.
    let release_pages = large || (cfg!(target_os = "linux") && map_by_default && map);
    let _ = release_pages;
    let give_back = tune("SS_RELEASE", 0) != 0;
    for (file, source) in files.iter_mut().enumerate() {
        if map {
            source.map_input();
        }
        for copy in 0..copies {
            let mut start = 0;
            while start < source.size {
                let end = (start + block).min(source.size);
                work.push(Work { events: file < event_files, copy: copy << 56, file, start, end });
                start = end;
            }
        }
    }

    let input_bytes = total.saturating_mul(copies);
    if copies == 1 { keys::prepare_users(input_bytes); }
    // At intermediate sizes, fewer active destinations reduce scattered
    // writes. Larger inputs need finer partitions for the dedup table.
    let partition_bits = |bytes_per_part| input_bytes.div_ceil(bytes_per_part).max(threads as u64 * 8).ilog2();
    let id_bits = partition_bits(1 << 20);
    let person_bits = partition_bits(1 << 20);
    let first_day = window_start.div_euclid(DAY);
    let day_offset = (window_start - first_day * DAY) as u64;
    let shared = Shared {
        window_start,
        window_end,
        products,
        unknown,
        categories,
        campaigns,
        campaign_names,
        campaign_channels,
        files,
        work,
        next: AtomicUsize::new(0),
        link_files: csv_files(&data.join("links"))?,
        defer_long_events: copies == 1,
        id_bits: tune("SS_ID_BITS", id_bits.min(if large { 12 } else { ID_BITS + 1 }) as u64).clamp(1, 16) as u32,
        person_bits: tune("SS_PERSON_BITS", person_bits.min(PERSON_BITS) as u64).clamp(1, 16) as u32,
        ahead: tune("SS_AHEAD", 32) as usize,
        links: OnceLock::new(),
        output,
        days: ((window_end - 1).div_euclid(DAY) - first_day + 1) as usize,
        day_offset,
        first_day,
    };
    let user_dir = shared.output.join("user_daily");
    let reusing_user_dir = user_dir.is_dir();
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(shared.output.join("campaign_daily"))?;
    let shared = &shared;
    let setup = began.elapsed();

    let shards = &LinkShards::default();
    let parsed: Vec<(EventSource, ClickSource)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| scope.spawn(move || parse_worker(shared, thread, threads, shards)))
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<io::Result<Vec<_>>>()
    })?;
    let (parsed_events, clicks): (Vec<_>, Vec<_>) = parsed.into_iter().unzip();
    let parse = began.elapsed();

    let next = AtomicUsize::new(0);
    let events: Vec<Buckets<Event>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| scope.spawn(|| dedup_worker(shared, &parsed_events, &next)))
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let dedup = began.elapsed();
    let parsed_events = &parsed_events;

    // Biggest person buckets first, so a whale's is never the last one running.
    let mut order: Vec<u32> = (0..1u32 << shared.person_bits).collect();
    let weight = |part: u32| {
        events
            .iter()
            .map(|buckets| buckets.chunks[part as usize] as u64)
            .sum::<u64>()
    };
    order.sort_by_cached_key(|&part| std::cmp::Reverse(weight(part)));
    let order = &order;
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<Totals>, bool)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (events, clicks, next) = (&events, &clicks, &next);
                scope.spawn(move || session_worker(shared, events, clicks, order, next, thread))
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<io::Result<Vec<_>>>()
    })?;
    let sessions = began.elapsed();

    let empty_result = !results.iter().any(|result| result.1);
    if empty_result {
        let path = user_dir.join("part-000.parquet");
        pq::Writer::create(&path, &USER_COLUMNS, GROUP_ROWS, &shared.categories)?.finish()?;
    }
    let mut writer = pq::Writer::create(
        &shared.output.join("campaign_daily").join("part-000.parquet"),
        &[
            ("campaign_id", pq::Kind::Text),
            ("channel", pq::Kind::Text),
            ("day", pq::Kind::Date),
            ("sessions", pq::Kind::Int64),
            ("purchasing_sessions", pq::Kind::Int64),
            ("revenue_cents", pq::Kind::Int64),
            ("assisted_sessions", pq::Kind::Int64),
        ],
        GROUP_ROWS,
        &[],
    )?;
    for campaign in 0..shared.campaign_names.len() {
        for day in 0..shared.days {
            let mut sum: Totals = [0; 4];
            for result in &results {
                let totals = &result.0[campaign * shared.days + day];
                for (total, part) in sum.iter_mut().zip(totals) {
                    *total += part;
                }
            }
            if sum[0] == 0 && sum[3] == 0 {
                continue;
            }
            writer.text(0, &shared.campaign_names[campaign]);
            writer.text(1, &shared.campaign_channels[campaign]);
            writer.int(2, shared.first_day + day as i64);
            writer.int(3, sum[0]);
            writer.int(4, sum[1]);
            writer.int(5, sum[2]);
            writer.int(6, sum[3]);
            writer.end_row();
        }
    }
    writer.finish()?;
    // A retry may use fewer workers or assign all rows to different workers.
    // Remove only this writer's stale parts, after both tables are complete.
    if reusing_user_dir {
        for entry in fs::read_dir(&user_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let part = name.to_str().and_then(|s| s.strip_prefix("part-")?.strip_suffix(".parquet"));
            let Some(digits) = part.filter(|s| s.len() == 3 && s.bytes().all(|b| b.is_ascii_digit())) else {
                continue;
            };
            let worker = digits.parse::<usize>().unwrap();
            let wrote = results.get(worker).is_some_and(|result| result.1) || (worker == 0 && empty_result);
            if worker < MAX_THREADS && !wrote && entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
    }


    // Nothing reads any of this any more. Give it back from every thread at
    // once: left to the kernel, it is freed on one thread at exit, on the clock.
    let mut segments = Vec::new();
    for (file, source) in shared.files.iter().enumerate() {
        let mut start = 0;
        while start < source.size {
            segments.push((file, start, (start + SEGMENT).min(source.size)));
            start += SEGMENT;
        }
    }
    if !give_back {
        segments.clear();
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for thread in 0..threads.min(if give_back { threads } else { 0 }) {
            let (events, clicks, segments, next) = (&events, &clicks, &segments, &next);
            let parsed_events = &parsed_events;
            scope.spawn(move || {
                // The parsed slabs hold the events too by now.
                parsed_events[thread].buckets.release();
                events[thread].release();
                clicks[thread].buckets.release();
                while let Some(&(file, start, end)) = segments.get(next.fetch_add(1, Ordering::Relaxed)) {
                    shared.files[file].release(start, end);
                }
            });
        }
        if let Some(links) = shared.links.get().filter(|_| give_back) {
            crate::release(links.slots.as_ptr(), links.slots.len());
        }
    });
    let total = began.elapsed();
    eprintln!(
        "setup={:.3} parse={:.3} dedup={:.3} sessions={:.3} write={:.3} links={:.3} total={:.3}",
        setup.as_secs_f64(),
        (parse - setup).as_secs_f64(),
        (dedup - parse).as_secs_f64(),
        (sessions - dedup).as_secs_f64(),
        (total - sessions).as_secs_f64(),
        LINKS_MICROS.load(Ordering::Relaxed) as f64 / 1e6,
        total.as_secs_f64()
    );
    // All output is durable at this point. Skip libc/destructor teardown:
    // kernel reclamation of the multi-gigabyte arenas is outside the run.
    #[cfg(target_os = "linux")]
    unsafe { _exit(0) };
    #[cfg(not(target_os = "linux"))]
    std::process::exit(0);
}

#[cfg(test)]
mod ingest_tests {
    use super::*;

    #[test]
    fn compact_ingest_preserves_boundaries_and_overflow_values() {
        let origin = 1_700_000_000_000;
        let limit = i64::from(OVERFLOW_TIME);
        let times = [i64::MIN, origin - 1, origin, origin + limit - 1, origin + limit, i64::MAX];
        let mut overflow = Vec::new();
        let encoded = times.map(|time| encode_ingest(time, origin, &mut overflow));
        assert_eq!(encoded[2..4], [0, OVERFLOW_TIME - 1]);
        assert_eq!(overflow, [i64::MIN, origin - 1, origin + limit, i64::MAX]);
        assert_eq!(encoded.map(|time| decode_ingest(time, origin, &overflow)), times);
    }
}
