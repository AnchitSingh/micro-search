//! Delta encoding/decoding for efficient log transmission.

use crate::types::{Tok, DocId};
use std::io;

pub const TAG_FULL: u8 = 0;
pub const TAG_DIFF: u8 = 1;

#[derive(Debug, PartialEq)]
pub enum Frame {
    Full { doc_id: DocId, tokens: Vec<Tok> },
    Diff { doc_id: DocId, remove: Vec<Tok>, add: Vec<Tok> },
}

/// Encode a full token set.
pub fn encode_full(doc: DocId, tokens: &[Tok]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(tokens.len() * 9 + 10);
    buf.push(TAG_FULL);
    write_uvar(doc, &mut buf);
    write_uvar(tokens.len() as u64, &mut buf);
    for &t in tokens {
        write_uvar(t, &mut buf);
    }
    buf
}

/// Encode a differential update.
pub fn encode_diff(doc: DocId, remove: &[Tok], add: &[Tok]) -> Vec<u8> {
    let mut buf = Vec::with_capacity((remove.len() + add.len()) * 9 + 10);
    buf.push(TAG_DIFF);
    write_uvar(doc, &mut buf);
    write_uvar(remove.len() as u64, &mut buf);
    for &t in remove {
        write_uvar(t, &mut buf);
    }
    write_uvar(add.len() as u64, &mut buf);
    for &t in add {
        write_uvar(t, &mut buf);
    }
    buf
}

/// Decode bytes into a frame.
pub fn decode(mut bytes: &[u8]) -> io::Result<Frame> {
    if bytes.is_empty() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    
    let tag = bytes[0];
    bytes = &bytes[1..];
    let doc_id = read_uvar(&mut bytes)?;

    match tag {
        TAG_FULL => {
            let len = read_uvar(&mut bytes)? as usize;
            let mut tokens = Vec::with_capacity(len);
            for _ in 0..len {
                tokens.push(read_uvar(&mut bytes)?);
            }
            Ok(Frame::Full { doc_id, tokens })
        }
        TAG_DIFF => {
            let rlen = read_uvar(&mut bytes)? as usize;
            let mut remove = Vec::with_capacity(rlen);
            for _ in 0..rlen {
                remove.push(read_uvar(&mut bytes)?);
            }
            let alen = read_uvar(&mut bytes)? as usize;
            let mut add = Vec::with_capacity(alen);
            for _ in 0..alen {
                add.push(read_uvar(&mut bytes)?);
            }
            Ok(Frame::Diff { doc_id, remove, add })
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "bad tag")),
    }
}

/// Write variable-length integer.
#[inline]
fn write_uvar(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

/// Read variable-length integer.
#[inline]
fn read_uvar(src: &mut &[u8]) -> io::Result<u64> {
    let mut shift = 0;
    let mut acc = 0u64;
    for _ in 0..10 {
        if src.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let b = src[0];
        *src = &src[1..];
        acc |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(acc);
        }
        shift += 7;
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"))
}