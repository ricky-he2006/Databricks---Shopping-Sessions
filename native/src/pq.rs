//! A small Parquet writer, uncompressed but with compact encodings, because
//! bytes written to the output disk are the expensive part of a scored run:
//! integers are DELTA_BINARY_PACKED, a key that repeats on consecutive rows is
//! a dictionary with run-length indices, and an optional category is a
//! dictionary with bit-packed indices. The whole file is written in one call.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    /// A required string, PLAIN.
    Text,
    /// A required string that repeats on consecutive rows; see `Writer::keyed`.
    Keyed,
    /// An optional string out of the dictionary given to `Writer::create`.
    Category,
    Date,
    Int32,
    Int64,
}

struct Column {
    name: &'static str,
    kind: Kind,
    /// `Text`: values, each its length as 4 little-endian bytes then its bytes.
    /// `Keyed`: the distinct values of this row group, in the same form.
    /// `Category`: one dictionary index per present value.
    bytes: Vec<u8>,
    /// `Keyed`: how many rows each distinct value covers.
    runs: Vec<u32>,
    last_key: u64,
    /// `Category`: one bit per row, 1 where the value is present.
    present: Vec<u8>,
    ints: Vec<i64>,
}

struct ChunkMeta {
    offset: u64,
    data_offset: u64,
    size: u64,
}

struct GroupMeta {
    rows: u64,
    chunks: Vec<ChunkMeta>,
}

pub struct Writer {
    path: PathBuf,
    columns: Vec<Column>,
    dictionary: Vec<u8>,
    dictionary_len: usize,
    category_ranges: Vec<std::ops::Range<usize>>,
    stream: Option<BufWriter<File>>,
    written: u64,
    write_error: Option<io::Error>,
    rows: usize,
    group_rows: usize,
    groups: Vec<GroupMeta>,
    out: Vec<u8>,
}

/// Thrift compact protocol, as far as Parquet's headers and footer need it.
struct Thrift {
    out: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn zigzag(out: &mut Vec<u8>, value: i64) {
    varint(out, ((value << 1) ^ (value >> 63)) as u64);
}

impl Thrift {
    fn new() -> Self {
        Self {
            out: Vec::new(),
            last: 0,
            stack: Vec::new(),
        }
    }

    fn field(&mut self, id: i16, kind: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | kind);
        } else {
            self.out.push(kind);
            zigzag(&mut self.out, id as i64);
        }
        self.last = id;
    }

    fn i32(&mut self, id: i16, value: i32) {
        self.field(id, 5);
        zigzag(&mut self.out, value as i64);
    }

    fn i64(&mut self, id: i16, value: i64) {
        self.field(id, 6);
        zigzag(&mut self.out, value);
    }

    fn text(&mut self, id: i16, value: &str) {
        self.field(id, 8);
        varint(&mut self.out, value.len() as u64);
        self.out.extend_from_slice(value.as_bytes());
    }

    fn list(&mut self, id: i16, element: u8, count: usize) {
        self.field(id, 9);
        if count < 15 {
            self.out.push(((count as u8) << 4) | element);
        } else {
            self.out.push(0xF0 | element);
            varint(&mut self.out, count as u64);
        }
    }

    /// A struct as a field (`id` > 0) or as a list element (`id` == 0).
    fn begin(&mut self, id: i16) {
        if id > 0 {
            self.field(id, 12);
        }
        self.stack.push(self.last);
        self.last = 0;
    }

    fn end(&mut self) {
        self.out.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

/// Bits needed to hold `value`.
fn width(value: u64) -> u32 {
    64 - value.leading_zeros()
}

/// Appends `values`, `bits` bits each, least significant bit first.
fn pack(out: &mut Vec<u8>, values: impl Iterator<Item = u64>, bits: u32) {
    if bits == 0 {
        return;
    }
    let mut buffer: u128 = 0;
    let mut held = 0u32;
    for value in values {
        buffer |= (value as u128) << held;
        held += bits;
        while held >= 8 {
            out.push(buffer as u8);
            buffer >>= 8;
            held -= 8;
        }
    }
    if held > 0 {
        out.push(buffer as u8);
    }
}

/// DELTA_BINARY_PACKED: blocks of 128 deltas in 4 miniblocks of 32, each
/// miniblock packed at the width its largest delta above the block's minimum needs.
fn delta_binary_packed(out: &mut Vec<u8>, values: &[i64]) {
    const BLOCK: usize = 128;
    const MINI: usize = 32;
    varint(out, BLOCK as u64);
    varint(out, (BLOCK / MINI) as u64);
    varint(out, values.len() as u64);
    zigzag(out, values.first().copied().unwrap_or(0));
    let mut deltas = [0i64; BLOCK];
    let mut at = 1;
    while at < values.len() {
        let count = (values.len() - at).min(BLOCK);
        let mut least = i64::MAX;
        for slot in 0..count {
            deltas[slot] = values[at + slot].wrapping_sub(values[at + slot - 1]);
            least = least.min(deltas[slot]);
        }
        zigzag(out, least);
        let minis = count.div_ceil(MINI);
        let mut widths = [0u8; BLOCK / MINI];
        for mini in 0..minis {
            let end = ((mini + 1) * MINI).min(count);
            let most = deltas[mini * MINI..end]
                .iter()
                .fold(0u64, |most, &delta| most.max(delta.wrapping_sub(least) as u64));
            widths[mini] = width(most) as u8;
        }
        out.extend_from_slice(&widths);
        for mini in 0..minis {
            // A short last miniblock is padded with zeros to its full 32 values.
            let from = mini * MINI;
            let packed = (from..from + MINI).map(|slot| {
                if slot < count {
                    deltas[slot].wrapping_sub(least) as u64
                } else {
                    0
                }
            });
            pack(out, packed, widths[mini] as u32);
        }
        at += count;
    }
}

impl Writer {
    /// `dictionary` is every value a `Category` column can take, in index order.
    pub fn create(
        path: &Path,
        columns: &[(&'static str, Kind)],
        group_rows: usize,
        dictionary: &[Vec<u8>],
    ) -> io::Result<Self> {
        let stream = if std::env::var_os("SS_PLAIN").is_none_or(|value| value != "0") {
            Some(BufWriter::with_capacity(1 << 20, File::create(path)?))
        } else {
            None
        };
        let mut ranges = Vec::with_capacity(dictionary.len());
        let mut plain = Vec::new();
        for value in dictionary {
            let start = plain.len();
            plain.extend_from_slice(&(value.len() as u32).to_le_bytes());
            plain.extend_from_slice(value);
            ranges.push(start..plain.len());
        }
        Ok(Self {
            path: path.to_path_buf(),
            columns: columns
                .iter()
                .map(|&(name, kind)| Column {
                    name,
                    kind,
                    bytes: Vec::new(),
                    runs: Vec::new(),
                    last_key: 0,
                    present: Vec::new(),
                    ints: Vec::new(),
                })
                .collect(),
            dictionary: plain,
            dictionary_len: dictionary.len(),
            category_ranges: ranges,
            stream,
            written: 0,
            write_error: None,
            rows: 0,
            group_rows,
            groups: Vec::new(),
            out: b"PAR1".to_vec(),
        })
    }

    #[inline(always)]
    pub fn text(&mut self, column: usize, value: &[u8]) {
        let out = &mut self.columns[column].bytes;
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }

    /// A `Keyed` value. `key` identifies it; when the row before had the same
    /// key the value is not written again. Otherwise `write` appends its bytes.
    #[inline(always)]
    pub fn keyed(&mut self, column: usize, key: u64, write: impl FnOnce(&mut Vec<u8>)) {
        let column = &mut self.columns[column];
        if self.stream.is_none() && column.last_key == key {
            if let Some(run) = column.runs.last_mut() {
                *run += 1;
                return;
            }
        }
        column.last_key = key;
        if self.stream.is_none() { column.runs.push(1); }
        let at = column.bytes.len();
        column.bytes.extend_from_slice(&[0; 4]);
        write(&mut column.bytes);
        let length = (column.bytes.len() - at - 4) as u32;
        column.bytes[at..at + 4].copy_from_slice(&length.to_le_bytes());
    }

    #[inline(always)]
    pub fn category(&mut self, column: usize, value: Option<u8>) {
        let row = self.rows;
        let column = &mut self.columns[column];
        if row & 7 == 0 {
            column.present.push(0);
        }
        if let Some(value) = value {
            *column.present.last_mut().unwrap() |= 1 << (row & 7);
            if self.stream.is_some() {
                column.bytes.extend_from_slice(&self.dictionary[self.category_ranges[value as usize].clone()]);
            } else {
                column.bytes.push(value);
            }
        }
    }

    #[inline(always)]
    pub fn int(&mut self, column: usize, value: i64) {
        let column = &mut self.columns[column];
        if self.stream.is_some() {
            match column.kind {
                Kind::Date | Kind::Int32 => column.bytes.extend_from_slice(&(value as i32).to_le_bytes()),
                Kind::Int64 => column.bytes.extend_from_slice(&value.to_le_bytes()),
                _ => unreachable!(),
            }
        } else {
            column.ints.push(value);
        }
    }

    #[inline(always)]
    pub fn end_row(&mut self) {
        self.rows += 1;
        if self.rows >= self.group_rows {
            self.flush();
        }
    }

    /// A page: its header, then `body`. `dictionary` pages hold `values` PLAIN values.
    fn page(out: &mut Vec<u8>, body: &[u8], values: usize, dictionary: bool, encoding: i32) {
        let mut header = Thrift::new();
        header.i32(1, if dictionary { 2 } else { 0 });
        header.i32(2, body.len() as i32);
        header.i32(3, body.len() as i32);
        if dictionary {
            header.begin(7);
            header.i32(1, values as i32);
            header.i32(2, 0); // PLAIN
            header.end();
        } else {
            header.begin(5);
            header.i32(1, values as i32);
            header.i32(2, encoding);
            header.i32(3, 3); // definition levels: RLE
            header.i32(4, 3); // repetition levels: RLE
            header.end();
        }
        header.out.push(0);
        out.extend_from_slice(&header.out);
        out.extend_from_slice(body);
    }

    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }
        let rows = self.rows;
        let mut chunks = Vec::with_capacity(self.columns.len());
        let mut body = Vec::new();
        for column in &mut self.columns {
            let offset = self.written + self.out.len() as u64;
            let mut data_offset = offset;
            body.clear();
            if self.stream.is_some() {
                if column.kind == Kind::Category {
                    let groups = rows.div_ceil(8);
                    let mut levels = Vec::new();
                    varint(&mut levels, ((groups as u64) << 1) | 1);
                    body.extend_from_slice(&((levels.len() + groups) as u32).to_le_bytes());
                    body.extend_from_slice(&levels);
                    body.extend_from_slice(&column.present[..groups]);
                    body.extend_from_slice(&column.bytes);
                    Self::page(&mut self.out, &body, rows, false, 0);
                } else {
                    Self::page(&mut self.out, &column.bytes, rows, false, 0);
                }
            } else {
            match column.kind {
                Kind::Text => {
                    Self::page(&mut self.out, &column.bytes, rows, false, 0);
                }
                Kind::Keyed => {
                    Self::page(&mut self.out, &column.bytes, column.runs.len(), true, 0);
                    data_offset = self.out.len() as u64;
                    // RLE_DICTIONARY: the index width, then one run per distinct value.
                    let bits = width(column.runs.len() as u64 - 1).max(1);
                    let wide = bits.div_ceil(8) as usize;
                    body.push(bits as u8);
                    for (index, &run) in column.runs.iter().enumerate() {
                        varint(&mut body, (run as u64) << 1);
                        body.extend_from_slice(&(index as u32).to_le_bytes()[..wide]);
                    }
                    Self::page(&mut self.out, &body, rows, false, 8);
                }
                Kind::Category => {
                    Self::page(&mut self.out, &self.dictionary, self.dictionary_len, true, 0);
                    data_offset = self.out.len() as u64;
                    // Definition levels: one bit-packed run of width 1.
                    let groups = rows.div_ceil(8);
                    let mut levels = Vec::new();
                    varint(&mut levels, ((groups as u64) << 1) | 1);
                    body.extend_from_slice(&((levels.len() + groups) as u32).to_le_bytes());
                    body.extend_from_slice(&levels);
                    body.extend_from_slice(&column.present[..groups]);
                    // RLE_DICTIONARY: the index width, then one bit-packed run.
                    let bits = width(self.dictionary_len.max(1) as u64 - 1).max(1);
                    body.push(bits as u8);
                    let groups = column.bytes.len().div_ceil(8);
                    if groups > 0 {
                        varint(&mut body, ((groups as u64) << 1) | 1);
                        let padded = column
                            .bytes
                            .iter()
                            .map(|&index| index as u64)
                            .chain(std::iter::repeat(0))
                            .take(groups * 8);
                        pack(&mut body, padded, bits);
                    }
                    Self::page(&mut self.out, &body, rows, false, 8);
                }
                Kind::Date | Kind::Int32 | Kind::Int64 => {
                    delta_binary_packed(&mut body, &column.ints);
                    Self::page(&mut self.out, &body, rows, false, 5);
                }
            }
            }
            chunks.push(ChunkMeta {
                offset,
                data_offset,
                size: self.written + self.out.len() as u64 - offset,
            });
            column.bytes.clear();
            column.runs.clear();
            column.present.clear();
            column.ints.clear();
        }
        self.groups.push(GroupMeta {
            rows: rows as u64,
            chunks,
        });
        self.rows = 0;
        if let Some(stream) = self.stream.as_mut() {
            if self.write_error.is_none() {
                self.write_error = stream.write_all(&self.out).err();
            }
            self.written += self.out.len() as u64;
            self.out.clear();
        }
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.flush();
        let mut meta = Thrift::new();
        meta.i32(1, 1);
        meta.list(2, 12, self.columns.len() + 1);
        meta.begin(0);
        meta.text(4, "schema");
        meta.i32(5, self.columns.len() as i32);
        meta.end();
        for column in &self.columns {
            let (physical, repetition, converted, logical) = match column.kind {
                Kind::Text | Kind::Keyed => (6, 0, Some(0), Some(1)),
                Kind::Category => (6, 1, Some(0), Some(1)),
                Kind::Date => (1, 0, Some(6), Some(6)),
                Kind::Int32 => (1, 0, None, None),
                Kind::Int64 => (2, 0, None, None),
            };
            meta.begin(0);
            meta.i32(1, physical);
            meta.i32(3, repetition);
            meta.text(4, column.name);
            if let Some(converted) = converted {
                meta.i32(6, converted);
            }
            if let Some(logical) = logical {
                meta.begin(10);
                meta.begin(logical);
                meta.end();
                meta.end();
            }
            meta.end();
        }
        meta.i64(3, self.groups.iter().map(|group| group.rows as i64).sum());
        meta.list(4, 12, self.groups.len());
        for group in &self.groups {
            meta.begin(0);
            meta.list(1, 12, group.chunks.len());
            for (chunk, column) in group.chunks.iter().zip(&self.columns) {
                let (physical, mut encodings): (i32, &[i64]) = match column.kind {
                    Kind::Text => (6, &[0]),
                    Kind::Keyed => (6, &[0, 8]),
                    Kind::Category => (6, &[0, 8, 3]),
                    Kind::Date | Kind::Int32 => (1, &[5]),
                    Kind::Int64 => (2, &[5]),
                };
                if self.stream.is_some() {
                    encodings = if column.kind == Kind::Category { &[0, 3] } else { &[0] };
                }
                meta.begin(0);
                meta.i64(2, chunk.offset as i64);
                meta.begin(3);
                meta.i32(1, physical);
                meta.list(2, 5, encodings.len());
                for &encoding in encodings {
                    zigzag(&mut meta.out, encoding);
                }
                meta.list(3, 8, 1);
                varint(&mut meta.out, column.name.len() as u64);
                meta.out.extend_from_slice(column.name.as_bytes());
                meta.i32(4, 0);
                meta.i64(5, group.rows as i64);
                meta.i64(6, chunk.size as i64);
                meta.i64(7, chunk.size as i64);
                meta.i64(9, chunk.data_offset as i64);
                if chunk.data_offset != chunk.offset {
                    meta.i64(11, chunk.offset as i64);
                }
                meta.end();
                meta.end();
            }
            meta.i64(2, group.chunks.iter().map(|chunk| chunk.size as i64).sum());
            meta.i64(3, group.rows as i64);
            meta.end();
        }
        meta.text(6, "shopping-sessions");
        meta.out.push(0);
        let length = meta.out.len() as u32;
        self.out.extend_from_slice(&meta.out);
        self.out.extend_from_slice(&length.to_le_bytes());
        self.out.extend_from_slice(b"PAR1");
        if let Some(mut stream) = self.stream {
            if let Some(error) = self.write_error { return Err(error); }
            stream.write_all(&self.out)?;
            stream.flush()
        } else {
            File::create(&self.path)?.write_all(&self.out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unpacks what `delta_binary_packed` wrote, the way a reader would.
    fn decode(bytes: &[u8]) -> Vec<i64> {
        let mut at = 0;
        let read_varint = |at: &mut usize| {
            let (mut value, mut shift) = (0u64, 0);
            loop {
                let byte = bytes[*at];
                *at += 1;
                value |= ((byte & 0x7F) as u64) << shift;
                shift += 7;
                if byte < 0x80 {
                    return value;
                }
            }
        };
        let unzig = |value: u64| ((value >> 1) as i64) ^ -((value & 1) as i64);
        let block = read_varint(&mut at) as usize;
        let minis = read_varint(&mut at) as usize;
        let total = read_varint(&mut at) as usize;
        let mut out = vec![unzig(read_varint(&mut at))];
        let per = block / minis;
        while out.len() < total {
            let least = unzig(read_varint(&mut at));
            let widths = bytes[at..at + minis].to_vec();
            at += minis;
            for &bits in &widths {
                if out.len() >= total {
                    break;
                }
                let bits = bits as usize;
                for slot in 0..per {
                    let mut value = 0u64;
                    for bit in 0..bits {
                        let position = slot * bits + bit;
                        value |= (((bytes[at + position / 8] >> (position % 8)) & 1) as u64) << bit;
                    }
                    if out.len() < total {
                        out.push(out[out.len() - 1].wrapping_add(least).wrapping_add(value as i64));
                    }
                }
                at += (per * bits).div_ceil(8);
            }
        }
        assert_eq!(at, bytes.len());
        out
    }

    #[test]
    fn delta_round_trips() {
        let mut seed = 12345u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for count in [1usize, 2, 31, 32, 33, 127, 128, 129, 160, 161, 257, 1000, 4097] {
            for range in [1u64, 2, 14, 1 << 20, 1 << 40, u64::MAX] {
                let values: Vec<i64> = (0..count)
                    .map(|_| (next() % range) as i64 - (range / 2) as i64)
                    .collect();
                let mut bytes = Vec::new();
                delta_binary_packed(&mut bytes, &values);
                assert_eq!(decode(&bytes), values, "count {count} range {range}");
            }
        }
    }
}
