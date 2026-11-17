// Field tables -- message headers and the arguments of declare, bind and
// consume -- and the values in them, with the types RabbitMQ reads and
// writes (the 0-9-1 errata set):
//
//     t bool        b i8     B u8     s i16    u u16    I i32    i u32
//     l i64         f f32    d f64    D decimal         S string (bytes)
//     x byte array  T timestamp (seconds)  A array  F table  V void
//
//     let args = amqp.table();
//     amqp.table_set(args, "x-max-priority", amqp.i32_value(10));
//     amqp.table_set(args, "x-queue-type", amqp.str_value("quorum"));
//     let v = amqp.table_get(headers, "attempt");      // opt[Value]
//     amqp.as_int(v ?? amqp.void_value());              // opt[int]
//
// A value keeps its type code, so a table read from the wire is written
// back byte for byte.

import "builder";
import "math";
import "strings";

// Nesting of tables and arrays inside one another.
let MAX_TABLE_DEPTH = 32;

pub gc struct Value {
    // the type code, one of the bytes above
    code: int,
    // integers, booleans (0 or 1), timestamps, a decimal's digits
    i: int,
    // f and d
    f: float,
    // D: digits after the point
    scale: int,
    // S and x
    b: bytes,
    t: opt[Table],
    a: opt[[Value]],
}

// Keys in the order they were set or read.
pub gc struct Table {
    keys: [str],
    vals: [Value],
}

pub fn table() -> Table {
    let ks: [str] = [];
    let vs: [Value] = [];
    return Table { keys: ks, vals: vs };
}

// Sets key k, replacing a value already there.
pub fn table_set(t: Table, k: str, v: Value) {
    for i in 0..len(t.keys) {
        if t.keys[i] == k {
            t.vals[i] = v;
            return;
        }
    }
    push(t.keys, k);
    push(t.vals, v);
}

pub fn table_get(t: Table, k: str) -> opt[Value] {
    for i in 0..len(t.keys) {
        if t.keys[i] == k {
            return some(t.vals[i]);
        }
    }
    return none;
}

fn value(code: int) -> Value {
    return Value { code: code, i: 0, f: 0.0, scale: 0, b: b"", t: none, a: none };
}

pub fn void_value() -> Value {
    return value(86);
}

pub fn bool_value(x: bool) -> Value {
    let v = value(116);
    if x {
        v.i = 1;
    }
    return v;
}

// A 64-bit signed integer (l).
pub fn int_value(n: int) -> Value {
    let v = value(108);
    v.i = n;
    return v;
}

// A 32-bit signed integer (I), as RabbitMQ's x- arguments are commonly
// written.
pub fn i32_value(n: int) -> Value {
    let v = value(73);
    v.i = n;
    return v;
}

pub fn float_value(x: float) -> Value {
    let v = value(100);
    v.f = x;
    return v;
}

pub fn str_value(s: str) -> Value {
    let v = value(83);
    v.b = to_bytes(s);
    return v;
}

pub fn bytes_value(b: bytes) -> Value {
    let v = value(120);
    v.b = b;
    return v;
}

// Seconds since the Unix epoch.
pub fn timestamp_value(secs: int) -> Value {
    let v = value(84);
    v.i = secs;
    return v;
}

// digits * 10^-scale; scale 0-255, digits 0 to 2^32-1.
pub fn decimal_value(scale: int, digits: int) -> Value {
    let v = value(68);
    v.scale = scale;
    v.i = digits;
    return v;
}

pub fn table_value(t: Table) -> Value {
    let v = value(70);
    v.t = some(t);
    return v;
}

pub fn array_value(items: [Value]) -> Value {
    let v = value(65);
    v.a = some(items);
    return v;
}

// A string's (S) text, if it is UTF-8; a byte array is not a string.
pub fn as_str(v: Value) -> opt[str] {
    if v.code != 83 {
        return none;
    }
    let r = reader(v.b, 0, len(v.b));
    let s = rd_text(r, len(v.b));
    if r.failed != "" {
        return none;
    }
    return some(s);
}

pub fn as_bytes(v: Value) -> opt[bytes] {
    if v.code == 83 || v.code == 120 {
        return some(v.b);
    }
    return none;
}

// Any integer type, or a timestamp.
pub fn as_int(v: Value) -> opt[int] {
    let c = v.code;
    if c == 98 || c == 66 || c == 115 || c == 117 || c == 73 || c == 105 || c == 108 || c == 84 {
        return some(v.i);
    }
    return none;
}

pub fn as_bool(v: Value) -> opt[bool] {
    if v.code == 116 {
        return some(v.i != 0);
    }
    return none;
}

pub fn as_float(v: Value) -> opt[float] {
    if v.code == 100 || v.code == 102 {
        return some(v.f);
    }
    return none;
}

pub fn as_table(v: Value) -> opt[Table] {
    if v.code == 70 {
        return v.t;
    }
    return none;
}

pub fn as_array(v: Value) -> opt[[Value]] {
    if v.code == 65 {
        return v.a;
    }
    return none;
}

// ---- encoding ---------------------------------------------------------

// The table, its 32-bit length first.
fn encode_table(w: builder.Bytes, t: Table, depth: int) -> result[bool, str] {
    if depth > MAX_TABLE_DEPTH {
        return err("amqp: tables nested more than " + to_str(MAX_TABLE_DEPTH) + " deep");
    }
    let body = builder.new_bytes();
    for k in 0..len(t.keys) {
        let key = t.keys[k];
        if !short_ok(key) || key == "" {
            return err("amqp: a table key must be 1 to 255 bytes: \"" + key + "\"");
        }
        wr_shortstr(body, key);
        let vr = encode_value(body, t.vals[k], depth);
        guard let _v = vr else let e = err_of(vr) { return err(e); }
    }
    let bb = body.finish();
    wr_u32(w, len(bb));
    w.write(bb);
    return ok(true);
}

// One value: its type code, then its bytes.
fn encode_value(w: builder.Bytes, v: Value, depth: int) -> result[bool, str] {
    let c = v.code;
    if c == 116 {
        wr_u8(w, c);
        wr_u8(w, v.i & 1);
    } else if c == 98 || c == 66 || c == 115 || c == 117 || c == 73 || c == 105 {
        let lo = 0;
        let hi = 0;
        let width = 1;
        if c == 98 {
            lo = -128;
            hi = 127;
        } else if c == 66 {
            hi = 255;
        } else if c == 115 {
            lo = -32768;
            hi = 32767;
            width = 2;
        } else if c == 117 {
            hi = 65535;
            width = 2;
        } else if c == 73 {
            lo = -2147483648;
            hi = 2147483647;
            width = 4;
        } else {
            hi = 4294967295;
            width = 4;
        }
        if v.i < lo || v.i > hi {
            return err("amqp: " + to_str(v.i) + " does not fit field type '" + code_char(c) + "'");
        }
        wr_u8(w, c);
        if width == 1 {
            wr_u8(w, v.i);
        } else if width == 2 {
            wr_u16(w, v.i);
        } else {
            wr_u32(w, v.i);
        }
    } else if c == 108 || c == 84 {
        wr_u8(w, c);
        wr_u64(w, v.i);
    } else if c == 102 {
        wr_u8(w, c);
        wr_u32(w, math.f32_bits(v.f as f32) as int);
    } else if c == 100 {
        wr_u8(w, c);
        wr_u64(w, math.float_bits(v.f) as int);
    } else if c == 68 {
        if v.scale < 0 || v.scale > 255 || v.i < 0 || v.i > 4294967295 {
            return err("amqp: a decimal's scale is 0-255 and its digits 0 to 2^32-1");
        }
        wr_u8(w, c);
        wr_u8(w, v.scale);
        wr_u32(w, v.i);
    } else if c == 83 || c == 120 {
        wr_u8(w, c);
        wr_longstr(w, v.b);
    } else if c == 86 {
        wr_u8(w, c);
    } else if c == 70 {
        wr_u8(w, c);
        let t = v.t ?? table();
        let tr = encode_table(w, t, depth + 1);
        guard let _t = tr else let e = err_of(tr) { return err(e); }
    } else if c == 65 {
        if depth + 1 > MAX_TABLE_DEPTH {
            return err("amqp: tables nested more than " + to_str(MAX_TABLE_DEPTH) + " deep");
        }
        wr_u8(w, c);
        let body = builder.new_bytes();
        let items: [Value] = v.a ?? [];
        for it in items {
            let ir = encode_value(body, it, depth + 1);
            guard let _i = ir else let e = err_of(ir) { return err(e); }
        }
        let bb = body.finish();
        wr_u32(w, len(bb));
        w.write(bb);
    } else {
        return err("amqp: unknown field type " + to_str(c));
    }
    return ok(true);
}

fn code_char(c: int) -> str {
    let b = strings.bytes_zero(1);
    b[0] = c;
    return to_str(b);
}

// ---- decoding ---------------------------------------------------------

// A table at r.pos (its 32-bit length first); failures go to r.failed.
fn decode_table(r: Reader, depth: int) -> Table {
    let t = table();
    if depth > MAX_TABLE_DEPTH {
        fail_read(r, "tables nested more than " + to_str(MAX_TABLE_DEPTH) + " deep");
        return t;
    }
    let n = rd_u32(r);
    if !need(r, n) {
        return t;
    }
    let end = r.pos + n;
    let outer = r.end;
    r.end = end;
    while r.failed == "" && r.pos < end {
        let k = rd_shortstr(r);
        let v = decode_value(r, depth);
        if r.failed == "" {
            push(t.keys, k);
            push(t.vals, v);
        }
    }
    r.end = outer;
    return t;
}

fn decode_value(r: Reader, depth: int) -> Value {
    let c = rd_u8(r);
    let v = value(c);
    if r.failed != "" {
        return v;
    }
    if c == 116 {
        v.i = rd_u8(r);
        if v.i > 1 {
            v.i = 1;
        }
    } else if c == 98 {
        v.i = rd_i8(r);
    } else if c == 66 {
        v.i = rd_u8(r);
    } else if c == 115 {
        v.i = rd_i16(r);
    } else if c == 117 {
        v.i = rd_u16(r);
    } else if c == 73 {
        v.i = rd_i32(r);
    } else if c == 105 {
        v.i = rd_u32(r);
    } else if c == 108 || c == 84 {
        v.i = rd_u64(r);
    } else if c == 102 {
        let bits = rd_u32(r);
        v.f = math.f32_from_bits(bits as u32) as float;
    } else if c == 100 {
        let bits = rd_u64(r);
        v.f = math.float_from_bits(bits as u64);
    } else if c == 68 {
        v.scale = rd_u8(r);
        v.i = rd_u32(r);
    } else if c == 83 || c == 120 {
        v.b = rd_longstr(r);
    } else if c == 86 {
        // void: no bytes
    } else if c == 70 {
        v.t = some(decode_table(r, depth + 1));
    } else if c == 65 {
        if depth + 1 > MAX_TABLE_DEPTH {
            fail_read(r, "tables nested more than " + to_str(MAX_TABLE_DEPTH) + " deep");
            return v;
        }
        let n = rd_u32(r);
        let items: [Value] = [];
        if need(r, n) {
            let end = r.pos + n;
            let outer = r.end;
            r.end = end;
            while r.failed == "" && r.pos < end {
                let it = decode_value(r, depth + 1);
                if r.failed == "" {
                    push(items, it);
                }
            }
            r.end = outer;
        }
        v.a = some(items);
    } else {
        fail_read(r, "unknown field type " + to_str(c));
    }
    return v;
}
