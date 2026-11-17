// AMQP 0-9-1 wire types, big-endian: a Reader whose first failure sticks
// (every later read returns zero, so a decoder reads a whole structure
// and checks once), and writers onto a builder.Bytes.

import "builder";
import "strings";
import "unicode";

pub gc struct Reader {
    b: bytes,
    pos: int,
    end: int,
    failed: str,
}

// A reader over b[from..to].
pub fn reader(b: bytes, from: int, to: int) -> Reader {
    return Reader { b: b, pos: from, end: to, failed: "" };
}

fn need(r: Reader, n: int) -> bool {
    if r.failed != "" {
        return false;
    }
    if n < 0 || r.pos + n > r.end {
        r.failed = "amqp: truncated: " + to_str(n) + " bytes needed at offset " + to_str(r.pos) + " of " +
                   to_str(r.end);
        return false;
    }
    return true;
}

fn fail_read(r: Reader, msg: str) {
    if r.failed == "" {
        r.failed = "amqp: " + msg;
    }
}

fn rd_u8(r: Reader) -> int {
    if !need(r, 1) {
        return 0;
    }
    let v = r.b[r.pos];
    r.pos += 1;
    return v;
}

fn rd_u16(r: Reader) -> int {
    if !need(r, 2) {
        return 0;
    }
    let v = (r.b[r.pos] << 8) | r.b[r.pos + 1];
    r.pos += 2;
    return v;
}

fn rd_u32(r: Reader) -> int {
    if !need(r, 4) {
        return 0;
    }
    let v = 0;
    for k in 0..4 {
        v = (v << 8) | r.b[r.pos + k];
    }
    r.pos += 4;
    return v;
}

// Eight bytes; the top bit set is a negative int (two's complement), as
// an i64 field means it.
fn rd_u64(r: Reader) -> int {
    if !need(r, 8) {
        return 0;
    }
    let v = 0;
    for k in 0..8 {
        v = (v << 8) | r.b[r.pos + k];
    }
    r.pos += 8;
    return v;
}

fn rd_i8(r: Reader) -> int {
    let v = rd_u8(r);
    if v >= 128 {
        return v - 256;
    }
    return v;
}

fn rd_i16(r: Reader) -> int {
    let v = rd_u16(r);
    if v >= 32768 {
        return v - 65536;
    }
    return v;
}

fn rd_i32(r: Reader) -> int {
    let v = rd_u32(r);
    if v >= 2147483648 {
        return v - 4294967296;
    }
    return v;
}

fn rd_raw(r: Reader, n: int) -> bytes {
    if !need(r, n) {
        return b"";
    }
    let out = r.b[r.pos..r.pos + n];
    r.pos += n;
    return out;
}

// A short string: an octet length, then UTF-8 without NUL.
fn rd_shortstr(r: Reader) -> str {
    let n = rd_u8(r);
    return rd_text(r, n);
}

fn rd_text(r: Reader, n: int) -> str {
    if !need(r, n) {
        return "";
    }
    for k in r.pos..(r.pos + n) {
        if r.b[k] == 0 {
            fail_read(r, "a NUL byte in a string");
            return "";
        }
    }
    let raw = r.b[r.pos..r.pos + n];
    if !unicode.valid_bytes(raw) {
        fail_read(r, "a string that is not UTF-8");
        return "";
    }
    let s = strings.from_bytes(r.b, r.pos, r.pos + n);
    r.pos += n;
    return s;
}

// A long string: a 32-bit length, then bytes (binary is allowed).
fn rd_longstr(r: Reader) -> bytes {
    let n = rd_u32(r);
    return rd_raw(r, n);
}

fn wr_u8(w: builder.Bytes, v: int) {
    w.write_byte(v & 255);
}

fn wr_u16(w: builder.Bytes, v: int) {
    w.write_byte((v >> 8) & 255);
    w.write_byte(v & 255);
}

fn wr_u32(w: builder.Bytes, v: int) {
    w.write_byte((v >> 24) & 255);
    w.write_byte((v >> 16) & 255);
    w.write_byte((v >> 8) & 255);
    w.write_byte(v & 255);
}

fn wr_u64(w: builder.Bytes, v: int) {
    let shift = 56;
    while shift >= 0 {
        w.write_byte((v >> shift) & 255);
        shift -= 8;
    }
}

fn wr_shortstr(w: builder.Bytes, s: str) {
    let b = to_bytes(s);
    wr_u8(w, len(b));
    w.write(b);
}

fn wr_longstr(w: builder.Bytes, b: bytes) {
    wr_u32(w, len(b));
    w.write(b);
}

// A short string's length check, for encoders.
fn short_ok(s: str) -> bool {
    return len(s) <= 255;
}
