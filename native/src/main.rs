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

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const SESSION_GAP: u64 = 1_800_000;
/// Bytes of CSV a thread takes at a time.
const BLOCK: u64 = 1 << 19;
/// Bytes of a links file read at a time.
const LINK_BLOCK: u64 = 1 << 19;
/// Bytes of a mapped file a thread unmaps at a time, once parsing is over.
const SEGMENT: u64 = 1 << 25;
const ID_BITS: u32 = 11;
const PERSON_BITS: u32 = 11;
const MIX: u64 = 0x9E37_79B9_7F4A_7C15;
/// The `ts` of an event that is invalid, late, or a losing duplicate.
const DEAD: u32 = u32::MAX;
const NONE: u32 = u32::MAX;
const GROUP_ROWS: usize = 1 << 17;
/// How many records ahead to prefetch a links lookup.
const AHEAD: usize = 16;

// ---------------------------------------------------------------- buckets

/// Records per chunk. The dedup table packs a record's offset in its chunk into 8 bits.
const CHUNK: usize = 256;
const _: () = assert!(CHUNK == 256);
const SLAB: usize = 1 << 13;

struct Part<T> {
    next: *mut T,
    end: *mut T,
    head: u32,
    chunks: u32,
}

/// Records appended to one of many partitions, in fixed chunks carved from
/// big slabs: no copying on growth, and a chunk never moves. A chunk can also
/// be a stretch of memory someone else is done with, which saves faulting in
/// fresh pages: on the scoring machines that costs about 75 ms per gigabyte.
struct Buckets<T: Copy> {
    parts: Vec<Part<T>>,
    slabs: Vec<Vec<T>>,
    /// Every chunk's first record, and the chunk before it in its partition.
    bases: Vec<*mut T>,
    previous: Vec<u32>,
    /// Records per chunk.
    capacity: usize,
    /// Chunks carved from our own slabs so far.
    carved: usize,
    /// Donated memory, each stretch big enough for one chunk.
    spare: Vec<*mut T>,
}

unsafe impl<T: Copy + Send> Send for Buckets<T> {}
unsafe impl<T: Copy + Send> Sync for Buckets<T> {}

impl<T: Copy> Buckets<T> {
    fn new(parts: usize, capacity: usize) -> Self {
        Self {
            parts: (0..parts)
                .map(|_| Part {
                    next: std::ptr::null_mut(),
                    end: std::ptr::null_mut(),
                    head: NONE,
                    chunks: 0,
                })
                .collect(),
            slabs: Vec::new(),
            bases: Vec::new(),
            previous: Vec::new(),
            capacity,
            carved: 0,
            spare: Vec::new(),
        }
    }

    #[inline(always)]
    fn push(&mut self, part: usize, value: T) {
        unsafe {
            let mut slot = self.parts.as_mut_ptr().add(part);
            if (*slot).next == (*slot).end {
                self.grow(part);
                slot = self.parts.as_mut_ptr().add(part);
            }
            (*slot).next.write(value);
            (*slot).next = (*slot).next.add(1);
        }
    }

    /// Hands over memory with room for one chunk, aligned for `T`.
    fn donate(&mut self, memory: *mut u8) {
        if memory as usize % std::mem::align_of::<T>() == 0 {
            self.spare.push(memory as *mut T);
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, part: usize) {
        let base = match self.spare.pop() {
            Some(base) => base,
            None => {
                if self.carved % SLAB == 0 {
                    let slab = Vec::with_capacity(SLAB * self.capacity);
                    huge(slab.as_ptr(), SLAB * self.capacity);
                    self.slabs.push(slab);
                }
                let slab = self.slabs.last().unwrap().as_ptr() as *mut T;
                let base = unsafe { slab.add((self.carved % SLAB) * self.capacity) };
                self.carved += 1;
                populate(base, self.capacity);
                base
            }
        };
        let chunk = self.bases.len() as u32;
        self.bases.push(base);
        let slot = &mut self.parts[part];
        slot.chunks += 1;
        self.previous.push(slot.head);
        slot.head = chunk;
        slot.next = base;
        slot.end = unsafe { base.add(self.capacity) };
    }

    /// Gives our own slabs back now, from whichever thread calls: at exit the
    /// kernel would free them page by page on one thread, inside the timed run.
    fn release(&self) {
        for slab in &self.slabs {
            release(slab.as_ptr(), SLAB * self.capacity);
        }
    }

    /// Appends every chunk of `part` to `out` as (first record, count).
    fn chunks(&self, part: usize, out: &mut Vec<(*mut T, usize)>) {
        let slot = &self.parts[part];
        let mut chunk = slot.head;
        if chunk == NONE {
            return;
        }
        let base = self.bases[chunk as usize];
        out.push((base, unsafe { slot.next.offset_from(base) } as usize));
        loop {
            chunk = self.previous[chunk as usize];
            if chunk == NONE {
                break;
            }
            out.push((self.bases[chunk as usize], self.capacity));
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Parsed {
    id: u64,
    user: u64,
    /// Milliseconds after `window_start - 10 days`, clamped.
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

    fn insert(&mut self, name: &[u8], value: u32) {
        // `numbered` reads only the name's own bytes.
        let number = unsafe { ts::numbered(name.as_ptr(), name.len(), self.prefix) };
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

/// user ID -> person, for every ID that a link names.
struct Links {
    slots: Vec<[u64; 2]>,
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
        let mut parent: Vec<u32> = (0..names.len() as u32).collect();
        for (at, &[a, b]) in numbered.iter().enumerate() {
            if let Some(&[c, d]) = numbered.get(at + 8) {
                prefetch(&parent[c as usize]);
                prefetch(&parent[d as usize]);
            }
            let a = find(&mut parent, a);
            let b = find(&mut parent, b);
            if a < b {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
        // Each person's name: the byte-wise smallest ID under their root.
        let mut smallest = names.clone();
        for (id, &name) in names.iter().enumerate() {
            let root = find(&mut parent, id as u32) as usize;
            if keys::compare(name, smallest[root]) == Order::Less {
                smallest[root] = name;
            }
        }
        for at in 0..slots.len() {
            if at + 16 < slots.len() && slots[at + 16][0] != 0 {
                prefetch(&parent[slots[at + 16][1] as usize]);
            }
            let entry = &mut slots[at];
            if entry[0] != 0 {
                // After the pass above every ID is at most one step from its root.
                entry[1] = smallest[find(&mut parent, entry[1] as u32) as usize];
            }
        }
        Self {
            slots,
            shift,
            empty: pairs.is_empty(),
        }
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
            let entry = unsafe { self.slots.get_unchecked(slot) };
            if entry[0] == key {
                return entry[1];
            }
            if entry[0] == 0 {
                return key;
            }
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

/// With `POPULATE` on: when `count` values at `start` begin a new 2 MB stretch,
/// fault the whole stretch in with one call (MADV_POPULATE_WRITE).
#[inline(always)]
fn populate<T>(start: *const T, count: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn madvise(address: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        const STRETCH: usize = 1 << 21;
        let first = start as usize;
        let last = first + count * std::mem::size_of::<T>() - 1;
        if POPULATE.load(Ordering::Relaxed) != 0 && (first / STRETCH != last / STRETCH || first % STRETCH < 8192) {
            let from = last & !(STRETCH - 1);
            madvise(from as *mut _, STRETCH, 23);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (start, count);
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
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "csv") && path.is_file() {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

/// Every well-formed line of a small two-column file.
fn read_pairs(path: &Path) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let source = Source::open(path, false)?;
    let mut rows = Vec::new();
    if source.size == 0 {
        return Ok(rows);
    }
    let mut block = Block::new();
    block.load(&source, 0, source.size)?;
    block.lines::<2>(|buf, fields| unsafe {
        let text = |at: usize| {
            std::slice::from_raw_parts(buf.add(fields[at].0 as usize), (fields[at].1 - fields[at].0) as usize).to_vec()
        };
        rows.push((text(0), text(1)));
    });
    Ok(rows)
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

fn read_links(shared: &Shared, block: &mut Block) -> io::Result<Links> {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    for path in &shared.link_files {
        let source = Source::open(path, true)?;
        let size = source.size;
        let mut start = 0;
        while start < size {
            let end = (start + LINK_BLOCK).min(size);
            block.load(&source, start, end)?;
            block.lines::<2>(|buf, fields| unsafe {
                let a = keys::pack(buf.add(fields[0].0 as usize), (fields[0].1 - fields[0].0) as usize);
                let b = keys::pack(buf.add(fields[1].0 as usize), (fields[1].1 - fields[1].0) as usize);
                if a != 0 && b != 0 && a != b {
                    pairs.push((a, b));
                }
            });
            start = end;
        }
    }
    Ok(Links::build(&pairs))
}

#[inline(always)]
fn person_part(person: u64, bits: u32) -> usize {
    (person.wrapping_mul(MIX) >> (64 - bits)) as usize
}

fn parse_events(shared: &Shared, block: &Block, copy: u64, dates: &mut ts::Dates, out: &mut Buckets<Parsed>) {
    let id_shift = 64 - shared.id_bits;
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    let window_start = shared.window_start;
    let window_end = shared.window_end;
    let ingest_base = window_start - 10 * DAY;
    block.lines::<7>(|buf, fields| unsafe {
        let field = |at: usize| (buf.add(fields[at].0 as usize), (fields[at].1 - fields[at].0) as usize);
        if fields[0].0 == fields[0].1 { return; }
        let (p, n) = field(3);
        let ingest = ts::parse(p, n, dates);
        if ingest == ts::BAD { return; }

        let (p, n) = field(4);
        let purchase = n == 8 && (p as *const u64).read_unaligned() == PURCHASE.to_le();
        let other = n == 4 && {
            let word = u32::from_le((p as *const u32).read_unaligned());
            word == VIEW || word == CART
        };
        let mut at = ts::BAD;
        let mut user = 0;
        let mut payload = 0;
        if (purchase || other) && fields[1].1 > fields[1].0 && fields[5].1 > fields[5].0 {
            let (p, n) = field(2);
            at = ts::parse(p, n, dates);
            if at >= window_start && at < window_end && ingest - at <= DAY {
                let price = if purchase {
                    let (p, n) = field(6);
                    ts::price(p, n)
                } else { 0 };
                if !purchase || price != 0 {
                    let (p, n) = field(1);
                    let raw = keys::pack(p, n);
                    if raw != 0 {
                        user = raw ^ copy;
                        if purchase {
                            let (p, n) = field(5);
                            let category = shared.products.get(p, n);
                            let category = if category == 0 { shared.unknown } else { category - 1 };
                            payload = (category << 24) | price;
                        }
                    }
                }
            }
        }
        // Even invalid rows compete for earliest ingest by event ID.
        let (p, n) = field(0);
        let id = keys::pack(p, n);
        if id == 0 { return; }
        let id = id ^ copy;
        let valid = user != 0;
        let record = Parsed {
            id,
            user,
            ingest: (ingest - ingest_base).clamp(0, u32::MAX as i64) as u32,
            ts: if valid { (at - window_start) as u32 } else { DEAD },
            payload,
        };
        out.push((id.wrapping_mul(MIX) >> id_shift) as usize, record);
    });
}

fn parse_clicks(
    shared: &Shared,
    block: &Block,
    copy: u64,
    dates: &mut ts::Dates,
    pending: &mut Vec<RawClick>,
    out: &mut Buckets<Click>,
) {
    let earliest = shared.window_start - WEEK;
    let window_end = shared.window_end;
    pending.clear();
    block.lines::<4>(|buf, fields| unsafe {
        let field = |at: usize| (buf.add(fields[at].0 as usize), (fields[at].1 - fields[at].0) as usize);
        if fields[0].0 == fields[0].1 || fields[1].0 == fields[1].1 { return; }
        let (p, n) = field(3);
        let at = ts::parse(p, n, dates);
        if at < earliest || at >= window_end { return; }
        let (p, n) = field(2);
        let campaign = shared.campaigns.get(p, n);
        if campaign == 0 { return; }
        let (p, n) = field(0);
        let id = keys::pack(p, n);
        let (p, n) = field(1);
        let user = keys::pack(p, n);
        if id == 0 || user == 0 { return; }
        let (id, user) = (id ^ copy, user ^ copy);
        pending.push(RawClick {
            user,
            id,
            ts: (at - earliest) as u32,
            campaign,
        });
    });
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
        out.push(
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

fn parse_worker(shared: &Shared, first: bool) -> io::Result<(Buckets<Parsed>, Buckets<Click>)> {
    let mut block = Block::new();
    if first {
        // The other threads wait on this table, so a failure must not leave them waiting.
        let began = Instant::now();
        let links = read_links(shared, &mut block).unwrap_or_else(|error| {
            eprintln!("links: {error}");
            std::process::exit(1);
        });
        LINKS_MICROS.store(began.elapsed().as_micros() as usize, Ordering::Relaxed);
        let _ = shared.links.set(links);
    }
    let mut events = Buckets::new(1 << shared.id_bits, CHUNK);
    let mut clicks = Buckets::new(1 << shared.person_bits, CHUNK);
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

fn dedup_worker(shared: &Shared, sources: &[Buckets<Parsed>], next: &AtomicUsize) -> Buckets<Event> {
    let links = shared.links.get().unwrap();
    // The events go into the memory of the parsed records they came from: a
    // parsed chunk, once read, is room for one chunk of events.
    let room = CHUNK * std::mem::size_of::<Parsed>() / std::mem::size_of::<Event>();
    let mut out = Buckets::new(1 << shared.person_bits, room);
    let (ahead, person_bits) = (shared.ahead, shared.person_bits);
    let mut chunks: Vec<(*mut Parsed, usize)> = Vec::new();
    let mut table: Vec<u32> = Vec::new();
    loop {
        let part = next.fetch_add(1, Ordering::Relaxed);
        if part >= 1 << shared.id_bits {
            break;
        }
        chunks.clear();
        for source in sources {
            source.chunks(part, &mut chunks);
        }
        let total: usize = chunks.iter().map(|chunk| chunk.1).sum();
        if total == 0 {
            continue;
        }
        let capacity = (total * 2).next_power_of_two();
        let mask = capacity - 1;
        table.clear();
        table.resize(capacity, NONE);
        unsafe {
            for (index, &(base, count)) in chunks.iter().enumerate() {
                for offset in 0..count {
                    let record = base.add(offset);
                    let id = (*record).id;
                    let entry = ((index << 8) | offset) as u32;
                    let mut slot = (id.wrapping_mul(MIX) >> 24) as usize & mask;
                    loop {
                        let seen = *table.get_unchecked(slot);
                        if seen == NONE {
                            *table.get_unchecked_mut(slot) = entry;
                            break;
                        }
                        let other = chunks.get_unchecked((seen >> 8) as usize).0.add((seen & 255) as usize);
                        if (*other).id == id {
                            if (*record).ingest < (*other).ingest {
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
        let mut top: Option<usize> = None;
        for &category in &self.touched {
            let category = category as usize;
            top = match top {
                Some(best) if self.sums[best] > self.sums[category] => Some(best),
                Some(best) if self.sums[best] == self.sums[category] && best < category => Some(best),
                _ => Some(category),
            };
        }
        for &category in &self.touched {
            self.sums[category as usize] = 0;
        }
        self.touched.clear();
        writer.category(5, top.map(|category| category as u8));
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
    let mut counts = [0u32; 2048];
    let (mut from, mut to) = (&mut values[..], &mut scratch[..]);
    for shift in [32, 43, 54] {
        counts.fill(0);
        for &value in from.iter() {
            counts[(value >> shift) as usize & 2047] += 1;
        }
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

fn session_worker(
    shared: &Shared,
    events: &[Buckets<Event>],
    clicks: &[Buckets<Click>],
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
        active: Vec::with_capacity(campaigns),
        sums: [0; 256],
        touched: Vec::with_capacity(shared.categories.len()),
    };
    let mut event_chunks: Vec<(*mut Event, usize)> = Vec::new();
    let mut click_chunks: Vec<(*mut Click, usize)> = Vec::new();
    let mut table_keys: Vec<u64> = Vec::new();
    let mut table_ids: Vec<u32> = Vec::new();
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
        let mut capacity = (total / 4 + 16).next_power_of_two();
        let mut mask = capacity - 1;
        table_keys.clear();
        table_keys.resize(capacity, 0);
        table_ids.clear();
        table_ids.resize(capacity, 0);
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
                            persons.push(person);
                            ends.push(0);
                            break id;
                        }
                        slot = (slot + 1) & mask;
                    };
                    *ends.get_unchecked_mut(id as usize) += 1;
                    local.push(id);
                    if persons.len() * 2 > capacity {
                        capacity *= 2;
                        mask = capacity - 1;
                        table_keys.clear();
                        table_keys.resize(capacity, 0);
                        table_ids.clear();
                        table_ids.resize(capacity, 0);
                        for (id, &person) in persons.iter().enumerate() {
                            let mut slot = (person.wrapping_mul(MIX) >> 24) as usize & mask;
                            while table_keys[slot] != 0 {
                                slot = (slot + 1) & mask;
                            }
                            table_keys[slot] = person;
                            table_ids[slot] = id as u32;
                        }
                    }
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
            source.chunks(part, &mut click_chunks);
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

        let (mut from, mut click_from) = (0usize, 0usize);
        for (id, &person) in persons.iter().enumerate() {
            let to = ends[id] as usize;
            let click_to = click_ends[id] as usize;
            let mine = &mut values[from..to];
            if mine.len() > 2048 {
                sort_by_time(mine, &mut scratch);
            } else {
                mine.sort_unstable();
            }
            let reached = &mut touches[click_from..click_to];
            if reached.len() > 1 {
                reached.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| keys::compare(a.id, b.id)));
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

fn main() -> io::Result<()> {
    let began = Instant::now();
    if std::env::var_os("SS_DIAG").is_some() && std::env::var_os("SS_CHILD").is_none() {
        diag::run();
    }
    HUGE.store(tune("SS_HUGE", 1) as usize, Ordering::Relaxed);
    POPULATE.store(tune("SS_POPULATE", 0) as usize, Ordering::Relaxed);
    let block = tune("SS_BLOCK", BLOCK).max(4096);
    let map = tune("SS_MAP", 1) != 0;
    let give_back = tune("SS_RELEASE", 1) != 0;
    let copies = tune("SS_COPIES", 1).clamp(1, 100);
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .min(32);

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
    {
        let source = Source::open(&data.join("products.csv"), false)?;
        if source.size > 0 {
            let mut block = Block::new();
            block.load(&source, 0, source.size)?;
            block.lines::<2>(|buf, fields| unsafe {
                let text = |at: usize| {
                    std::slice::from_raw_parts(buf.add(fields[at].0 as usize), (fields[at].1 - fields[at].0) as usize)
                };
                let (product, category) = (text(0), text(1));
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
                products.insert(product, index as u32 + 1);
            });
        }
    }
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
    for (campaign, channel) in read_pairs(&data.join("campaigns.csv"))? {
        if !campaign.is_empty() {
            campaigns.insert(&campaign, campaign_names.len() as u32);
            campaign_names.push(campaign);
            campaign_channels.push(channel);
        }
    }

    let mut files = Vec::new();
    let mut work = Vec::new();
    for (folder, events) in [("events", true), ("clicks", false)] {
        for path in csv_files(&data.join(folder))? {
            let source = Source::open(&path, map)?;
            let size = source.size;
            // An ID's first byte sits in the key's top 7 bits; a copy flips some of them.
            for copy in 0..copies {
                let mut start = 0;
                while start < size {
                    let end = (start + block).min(size);
                    work.push(Work {
                        events,
                        copy: copy << 56,
                        file: files.len(),
                        start,
                        end,
                    });
                    start = end;
                }
            }
            files.push(source);
        }
    }

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
        id_bits: tune("SS_ID_BITS", ID_BITS as u64).clamp(1, 16) as u32,
        person_bits: tune("SS_PERSON_BITS", PERSON_BITS as u64).clamp(1, 16) as u32,
        ahead: tune("SS_AHEAD", AHEAD as u64) as usize,
        links: OnceLock::new(),
        output,
        days: ((window_end - 1).div_euclid(DAY) - first_day + 1) as usize,
        day_offset,
        first_day,
    };
    fs::create_dir_all(shared.output.join("user_daily"))?;
    fs::create_dir_all(shared.output.join("campaign_daily"))?;
    let shared = &shared;
    let setup = began.elapsed();

    let parsed: Vec<(Buckets<Parsed>, Buckets<Click>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| scope.spawn(move || parse_worker(shared, thread == 0)))
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
            .map(|buckets| buckets.parts[part as usize].chunks as u64)
            .sum::<u64>()
    };
    order.sort_by_key(|&part| std::cmp::Reverse(weight(part)));
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

    if !results.iter().any(|result| result.1) {
        let path = shared.output.join("user_daily").join("part-000.parquet");
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
                parsed_events[thread].release();
                events[thread].release();
                clicks[thread].release();
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
    // Everything left is memory the kernel frees faster than we can.
    std::process::exit(0);
}
