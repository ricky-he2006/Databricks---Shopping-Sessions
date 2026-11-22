//! IDs as 64-bit keys. An ID of 1 to 9 bytes, each 1..=127, packs into 63 bits
//! with its first byte most significant, so packed keys order numerically the
//! way their strings order byte-wise. Anything else (longer, non-ASCII, a NUL)
//! is interned in a sharded table and gets bit 63. Key 0 is the empty string.

use std::cmp::Ordering;
use std::hash::{BuildHasher, RandomState};
use std::sync::{LazyLock, Mutex};

pub const INTERNED: u64 = 1 << 63;
const SHARDS: usize = 256;

#[derive(Default)]
struct Names {
    slots: Vec<u32>, // One-based indices into entries.
    entries: Vec<(u64, &'static [u8])>,
    spare: &'static mut [u8],
}

impl Names {
    fn insert(&mut self, hash: u64, value: &[u8]) -> usize {
        if self.entries.len() * 4 >= self.slots.len() * 3 {
            let mut slots = vec![0; (self.slots.len() * 2).max(32)];
            let mask = slots.len() - 1;
            for (i, &(h, _)) in self.entries.iter().enumerate() {
                let mut at = h as usize & mask;
                while slots[at] != 0 {
                    at = (at + 1) & mask;
                }
                slots[at] = i as u32 + 1;
            }
            self.slots = slots;
        }
        let mask = self.slots.len() - 1;
        let mut at = hash as usize & mask;
        while self.slots[at] != 0 {
            let i = self.slots[at] as usize - 1;
            if self.entries[i] == (hash, value) {
                return i;
            }
            at = (at + 1) & mask;
        }
        if self.spare.len() < value.len() {
            // Keys live until process exit. Keep slices stable across table growth.
            self.spare = Box::leak(vec![0; (32 * 1024).max(value.len())].into_boxed_slice());
        }
        let (name, rest) = std::mem::take(&mut self.spare).split_at_mut(value.len());
        name.copy_from_slice(value);
        self.spare = rest;
        let i = self.entries.len();
        self.slots[at] = u32::try_from(i + 1).expect("too many unique IDs in one shard");
        self.entries.push((hash, name));
        i
    }
}

static HASH: LazyLock<RandomState> = LazyLock::new(RandomState::new);
static NAMES: LazyLock<[Mutex<Names>; SHARDS]> =
    LazyLock::new(|| std::array::from_fn(|_| Mutex::new(Names::default())));

#[inline]
pub fn hash_bytes(value: &[u8]) -> u64 {
    HASH.hash_one(value)
}

#[inline]
fn intern(value: &[u8]) -> u64 {
    intern_name(value).0
}

fn intern_name(value: &[u8]) -> (u64, &'static [u8]) {
    let hash = hash_bytes(value);
    let shard = (hash >> 56) as usize;
    let mut table = NAMES[shard].lock().unwrap();
    let i = table.insert(hash, value);
    (INTERNED | ((i as u64) << 8) | shard as u64, table.entries[i].1)
}

// The tag and complete name share a cache line. A slot transitions once
// from empty to writing to published, so readers can reuse it without a lock.
const FIXED_USER: u64 = 1 << 61;
#[repr(C, align(64))]
struct UserSlot {
    tag: std::sync::atomic::AtomicU64,
    name: std::cell::UnsafeCell<[u8; 33]>,
}
// Only the thread that claims an empty slot writes its name. All reads use
// acquire publication or a key obtained from an already published slot.
unsafe impl Sync for UserSlot {}
static USER_SLOTS: std::sync::OnceLock<&'static [UserSlot]> = std::sync::OnceLock::new();

pub fn prepare_users(bytes: u64) {
    USER_SLOTS.get_or_init(|| {
        let size = bytes.div_ceil(4096).clamp(256, 1 << 22).next_power_of_two() as usize;
        // A byte allocation can use calloc even though the slots need cache-line
        // alignment. Untouched pages stay shared until a worker claims a slot.
        let bytes = size * size_of::<UserSlot>() + align_of::<UserSlot>() - 1;
        let storage = Box::leak(vec![0u8; bytes].into_boxed_slice());
        // Both fields admit all-zero bits. The allocation belongs to this
        // process-wide table and is accessed only through the aligned slots.
        let (_, slots, _) = unsafe { storage.align_to_mut::<UserSlot>() };
        &slots[..size]
    });
}

#[inline(always)]
pub fn long_name_mix(bytes: &[u8; 33]) -> u64 {
    let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    (word(1) ^ word(9).rotate_left(17) ^ word(17).rotate_left(33) ^ word(25).rotate_left(49))
        .wrapping_mul(0x9e3779b97f4a7c15)
}

#[inline(always)]
pub unsafe fn prepare_long_user(p: *const u8) -> u64 {
    let mixed = long_name_mix(unsafe { &*p.cast::<[u8; 33]>() });
    let slots = USER_SLOTS.get().unwrap();
    crate::prefetch(&slots[(mixed >> 24) as usize & (slots.len()-1)]);
    mixed
}
#[inline(always)]
pub unsafe fn finish_long_user(p: *const u8, mixed: u64) -> u64 {
    unsafe { fixed_user_mixed(p,USER_SLOTS.get().unwrap(),mixed) }
}

#[inline(always)]
unsafe fn fixed_user(p: *const u8, slots: &[UserSlot]) -> u64 {
    unsafe { fixed_user_mixed(p, slots, long_name_mix(&*p.cast::<[u8; 33]>())) }
}
#[inline(always)]
unsafe fn fixed_user_mixed(p: *const u8, slots: &[UserSlot], mixed: u64) -> u64 {
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let bytes = unsafe { &*p.cast::<[u8;33]>() };
    let hash = (mixed ^ (mixed >> 31)) | 2;
    let mask = slots.len()-1;
    let mut at = (mixed >> 24) as usize & mask;
    for _ in 0..12 {
        let slot = &slots[at];
        let mut tag = slot.tag.load(Acquire);
        if tag == 0 {
            match slot.tag.compare_exchange(0, 1, Relaxed, Acquire) {
                Ok(_) => {
                    unsafe { slot.name.get().write(*bytes); }
                    slot.tag.store(hash, Release);
                    return INTERNED | FIXED_USER | at as u64;
                }
                Err(other) => tag = other,
            }
        }
        while tag == 1 {
            std::hint::spin_loop();
            tag = slot.tag.load(Acquire);
        }
        if tag == hash && unsafe { *slot.name.get() == *bytes } {
            return INTERNED | FIXED_USER | at as u64;
        }
        at = (at + 1) & mask;
    }
    // The probe window never empties. Future occurrences follow this same
    // fallback and share the ordinary interner's canonical key.
    intern(bytes)
}

#[inline(always)]
pub unsafe fn pack_user(p: *const u8, n: usize) -> u64 {
    if n == 33 {
        if let Some(slots) = USER_SLOTS.get() { return unsafe { fixed_user(p, slots) }; }
    }
    unsafe { pack(p, n) }
}

fn name(key: u64) -> &'static [u8] {
    if key & FIXED_USER != 0 {
        let slot = &USER_SLOTS.get().unwrap()[(key & !(INTERNED | FIXED_USER)) as usize];
        return unsafe { &*slot.name.get() };
    }
    let shard = (key & 255) as usize;
    NAMES[shard].lock().unwrap().entries[((key & !INTERNED) >> 8) as usize].1
}

/// The key of the `len` bytes at `p`. At least 9 bytes must be readable at `p`.
#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
pub unsafe fn pack(p: *const u8, len: usize) -> u64 {
    unsafe {
        use std::arch::x86_64::{_bzhi_u64, _pext_u64};
        const LOW: u64 = 0x7F7F_7F7F_7F7F_7F7F;
        const HIGH: u64 = 0x8080_8080_8080_8080;
        if len == 0 {
            return 0;
        }
        if len <= 9 {
            // The first eight bytes, with those past the end cleared.
            let x = _bzhi_u64(u64::from_le((p as *const u64).read_unaligned()), len as u32 * 8);
            // 0x80 in every byte that is 1..=127: wanted in each byte of the ID.
            let plain = (x & LOW).wrapping_add(LOW) & !x & HIGH;
            let ninth = if len == 9 { *p.add(8) } else { 1 };
            if plain == _bzhi_u64(HIGH, len as u32 * 8) && ninth.wrapping_sub(1) < 127 {
                let ninth = if len == 9 { ninth as u64 } else { 0 };
                return (_pext_u64(x.swap_bytes(), LOW) << 7) | ninth;
            }
        }
        intern(std::slice::from_raw_parts(p, len))
    }
}

/// The key of the `len` bytes at `p`. At least 9 bytes must be readable at `p`.
#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
pub unsafe fn pack(p: *const u8, len: usize) -> u64 {
    unsafe { pack_plain(p, len) }
}

/// `pack` without any special instructions.
#[inline(always)]
pub unsafe fn pack_plain(p: *const u8, len: usize) -> u64 {
    unsafe {
        if len == 0 {
            return 0;
        }
        if len <= 9 {
            let raw = u64::from_le((p as *const u64).read_unaligned());
            let mask = if len >= 8 { !0u64 } else { (1u64 << (len * 8)) - 1 };
            let x = raw & mask;
            let filled = x | !mask;
            let zero = filled.wrapping_sub(0x0101_0101_0101_0101) & !filled & 0x8080_8080_8080_8080;
            let ninth = if len == 9 { *p.add(8) } else { 0 };
            let ninth_ok = len < 9 || (ninth != 0 && ninth < 128);
            if (x & 0x8080_8080_8080_8080) | zero == 0 && ninth_ok {
                let mut y = x.swap_bytes();
                y = ((y & 0x7F00_7F00_7F00_7F00) >> 1) | (y & 0x007F_007F_007F_007F);
                y = ((y & 0x3FFF_0000_3FFF_0000) >> 2) | (y & 0x0000_3FFF_0000_3FFF);
                y = ((y & 0x0FFF_FFFF_0000_0000) >> 4) | (y & 0x0000_0000_0FFF_FFFF);
                return (y << 7) | ninth as u64;
            }
        }
        intern(std::slice::from_raw_parts(p, len))
    }
}

/// Appends the string of `key` to `out`.
#[inline(always)]
pub fn unpack(key: u64, out: &mut Vec<u8>) {
    if key & INTERNED == 0 {
        let mut buf = [0u8; 9];
        let mut len = 0;
        let mut shift = 56i32;
        while shift >= 0 {
            let byte = ((key >> shift) & 0x7F) as u8;
            if byte == 0 {
                break;
            }
            buf[len] = byte;
            len += 1;
            shift -= 7;
        }
        out.extend_from_slice(&buf[..len]);
    } else {
        out.extend_from_slice(name(key));
    }
}

pub fn bytes(key: u64, packed: &mut [u8; 9]) -> &[u8] {
    if key & INTERNED != 0 {
        return name(key);
    }
    for (i, byte) in packed.iter_mut().enumerate() {
        *byte = ((key >> (56 - i * 7)) & 127) as u8;
    }
    &packed[..packed.iter().position(|&b| b == 0).unwrap_or(9)]
}

#[cold]
#[inline(never)]
fn compare_slow(a: u64, b: u64) -> Ordering {
    bytes(a, &mut [0; 9]).cmp(bytes(b, &mut [0; 9]))
}

/// Byte-wise order of the two keys' strings.
#[inline(always)]
pub fn compare(a: u64, b: u64) -> Ordering {
    if (a | b) & INTERNED == 0 {
        a.cmp(&b)
    } else {
        compare_slow(a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_matches_plain() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..2_000_000 {
            let mut bytes = [0u8; 24];
            for byte in bytes.iter_mut() {
                let random = next();
                // Mostly ID characters, sometimes a NUL or a byte above 127.
                *byte = match random % 50 {
                    0 => 0,
                    1 => 128 + (random >> 8) as u8 % 128,
                    2 => 1 + (random >> 8) as u8 % 127,
                    _ => b"abcdefghijklmnopqrstuvwxyz0123456789_"[(random >> 8) as usize % 37],
                };
            }
            let len = round % 12;
            let (fast, plain) = unsafe { (pack(bytes.as_ptr(), len), pack_plain(bytes.as_ptr(), len)) };
            if fast & INTERNED == 0 || plain & INTERNED == 0 {
                assert_eq!(fast, plain, "{:?} {len}", &bytes[..len]);
            }
        }
    }
}
