"""Generates tests/wire/{main.sl,expected.txt}: AMQP 0-9-1 frames that the
package must decode, render as below, and encode back byte for byte; and
malformed ones it must refuse.

Frames are written here with Python's struct, to the 0-9-1 spec and
RabbitMQ's field-table types. pika 1.3.2 checks that writer: every method
and content header whose tables use types pika reads correctly is decoded
by pika and encoded again, and must come out the same bytes. (pika's
decoder reads b/B, l, f and d wrongly, so tables using those are checked
against struct alone, and against RabbitMQ in the live suite.)

    python3 -m venv v && v/bin/pip install pika==1.3.2
    v/bin/python tests/wire/gen.py
"""
import json
import os
import random
import struct

import pika.spec

HERE = os.path.dirname(os.path.abspath(__file__))
rnd = random.Random(91)

SIGS = {
    (10, 10): "ootSS", (10, 11): "tnSn", (10, 20): "S", (10, 21): "S", (10, 30): "sls", (10, 31): "sls",
    (10, 40): "nnb", (10, 41): "n", (10, 50): "snss", (10, 51): "", (10, 60): "n", (10, 61): "",
    (20, 10): "n", (20, 11): "S", (20, 20): "b", (20, 21): "b", (20, 40): "snss", (20, 41): "",
    (40, 10): "snnbbbbbt", (40, 11): "", (40, 20): "snbb", (40, 21): "", (40, 30): "snnnbt", (40, 31): "",
    (40, 40): "snnnbt", (40, 51): "",
    (50, 10): "snbbbbbt", (50, 11): "nll", (50, 20): "snnnbt", (50, 21): "", (50, 30): "snb", (50, 31): "l",
    (50, 40): "snbbb", (50, 41): "l", (50, 50): "snnnt", (50, 51): "",
    (60, 10): "lsb", (60, 11): "", (60, 20): "snnbbbbt", (60, 21): "n", (60, 30): "nb", (60, 31): "n",
    (60, 40): "snnbb", (60, 50): "snnn", (60, 60): "nLbnn", (60, 70): "snb", (60, 71): "Lbnnl", (60, 72): "n",
    (60, 80): "Lb", (60, 90): "Lb", (60, 100): "b", (60, 110): "b", (60, 111): "", (60, 120): "Lbb",
    (85, 10): "b", (85, 11): "",
}

# ---- values: (code, payload) -----------------------------------------

INT_RANGES = {"b": (-128, 127), "B": (0, 255), "s": (-32768, 32767), "u": (0, 65535),
              "I": (-2**31, 2**31 - 1), "i": (0, 2**32 - 1), "l": (-2**63, 2**63 - 1)}
INT_FMT = {"b": ">b", "B": ">B", "s": ">h", "u": ">H", "I": ">i", "i": ">I", "l": ">q"}


def rtext(maxlen=12):
    alph = ["a", "Z", "0", " ", "é", "☃", "😀", '"', "\\", "-", ".", "\t"]
    s = "".join(rnd.choice(alph) for _ in range(rnd.randint(0, maxlen)))
    while len(s.encode()) > 255:
        s = s[:-1]
    return s


def rbytes():
    return bytes(rnd.randrange(256) for _ in range(rnd.randint(0, 10)))


def rvalue(depth, pika_safe):
    codes = ["S", "I", "t", "x", "T", "V", "D", "F", "A", "l"]
    if not pika_safe:
        codes += ["b", "B", "s", "u", "i", "f", "d"]
    if depth >= 3:
        codes = [c for c in codes if c not in "FA"]
    c = rnd.choice(codes)
    if c in INT_RANGES:
        lo, hi = INT_RANGES[c]
        if pika_safe and c == "l":
            v = rnd.choice([2**31, 2**40 + 7, 2**62])  # pika writes l only outside i32, and reads it unsigned
        elif pika_safe and c == "I":
            v = rnd.randint(lo, hi)
        else:
            v = rnd.choice([lo, hi, 0, rnd.randint(lo, hi)])
        return (c, v)
    if c == "t":
        return (c, rnd.randint(0, 1))
    if c == "f":
        return (c, struct.pack(">f", rnd.choice([0.0, -1.5, 3.25, 1e30, float("inf")])))
    if c == "d":
        return (c, struct.pack(">d", rnd.choice([0.0, -0.1, 2.5e-300, 1e300, 123456.789])))
    if c == "D":
        digits = rnd.randint(0, 2**31 - 1)
        if pika_safe and digits % 10 == 0:
            digits += 1  # pika's Decimal drops trailing zeros (93.9181470 -> scale 6)
        return (c, (rnd.randint(0, 9), digits))
    if c == "S":
        return (c, rtext().encode())
    if c == "x":
        return (c, rbytes())
    if c == "T":
        return (c, rnd.randint(0, 4102444800))  # pika reads a timestamp as a datetime: years up to 9999
    if c == "V":
        return (c, None)
    if c == "F":
        return (c, rtable(depth + 1, pika_safe))
    return (c, [rvalue(depth + 1, pika_safe) for _ in range(rnd.randint(0, 3))])


def rtable(depth, pika_safe):
    t = []
    seen = set()
    for _ in range(rnd.randint(0, 4)):
        k = rtext(8) or "k"
        if k in seen:
            continue
        seen.add(k)
        t.append((k, rvalue(depth, pika_safe)))
    return t


def shortstr(s):
    b = s.encode()
    return struct.pack(">B", len(b)) + b


def enc_value(v):
    c, p = v
    out = c.encode()
    if c in INT_FMT:
        return out + struct.pack(INT_FMT[c], p)
    if c == "t":
        return out + struct.pack(">B", p)
    if c in "fd":
        return out + p
    if c == "D":
        return out + struct.pack(">BI", p[0], p[1])
    if c in "Sx":
        return out + struct.pack(">I", len(p)) + p
    if c == "T":
        return out + struct.pack(">Q", p)
    if c == "V":
        return out
    if c == "F":
        return out + enc_table(p)
    body = b"".join(enc_value(x) for x in p)
    return out + struct.pack(">I", len(body)) + body


def enc_table(t):
    body = b"".join(shortstr(k) + enc_value(v) for k, v in t)
    return struct.pack(">I", len(body)) + body


def render_value(v):
    c, p = v
    if c in INT_FMT or c in "tT":
        return c + str(p)
    if c in "fd":
        return c + p.hex()
    if c == "D":
        return "D%d/%d" % p
    if c in "Sx":
        return c + p.hex()
    if c == "V":
        return "V"
    if c == "F":
        return "F" + render_table(p)
    return "A[" + ",".join(render_value(x) for x in p) + "]"


def render_table(t):
    return "{" + ",".join(json.dumps(k, ensure_ascii=False) + "=" + render_value(v) for k, v in t) + "}"


# ---- methods ---------------------------------------------------------

def rarg(letter, pika_safe):
    if letter == "o":
        return rnd.randint(0, 255)
    if letter == "s":
        return rnd.choice([0, 1, 65535, rnd.randint(0, 65535)])
    if letter == "l":
        return rnd.choice([0, 2**32 - 1, rnd.randint(0, 2**32 - 1)])
    if letter == "L":
        return rnd.choice([1, 2**63 - 1, rnd.randint(0, 2**63 - 1)])
    if letter == "b":
        return rnd.randint(0, 1)
    if letter == "n":
        return rnd.choice([rtext(), "x" * 255]) if rnd.random() < 0.1 else rtext()
    if letter == "S":
        return rbytes()
    return rtable(0, pika_safe)


def enc_args(sig, args):
    out = b""
    bits = 0
    nbits = 0
    for letter, a in zip(sig, args):
        if letter == "b":
            if nbits == 8:
                out += struct.pack(">B", bits)
                bits = nbits = 0
            bits |= a << nbits
            nbits += 1
            continue
        if nbits:
            out += struct.pack(">B", bits)
            bits = nbits = 0
        if letter == "o":
            out += struct.pack(">B", a)
        elif letter == "s":
            out += struct.pack(">H", a)
        elif letter == "l":
            out += struct.pack(">I", a)
        elif letter == "L":
            out += struct.pack(">Q", a)
        elif letter == "n":
            out += shortstr(a)
        elif letter == "S":
            out += struct.pack(">I", len(a)) + a
        else:
            out += enc_table(a)
    if nbits:
        out += struct.pack(">B", bits)
    return out


def render_method(cid, mid, sig, args):
    parts = []
    for letter, a in zip(sig, args):
        if letter in "oslLb":
            parts.append(str(a))
        elif letter == "n":
            parts.append(json.dumps(a, ensure_ascii=False))
        elif letter == "S":
            parts.append(a.hex())
        else:
            parts.append(render_table(a))
    return "M %d.%d %s" % (cid, mid, "|".join(parts))


def frame(kind, channel, payload):
    return struct.pack(">BHI", kind, channel, len(payload)) + payload + b"\xce"


def pika_method_roundtrip(cid, mid, args_bytes):
    m = pika.spec.methods[(cid << 16) | mid]()
    m.decode(args_bytes)
    return b"".join(m.encode())


# ---- content headers -------------------------------------------------

PROPS = [("content_type", "n", 0x8000), ("content_encoding", "n", 0x4000), ("headers", "t", 0x2000),
         ("delivery_mode", "o", 0x1000), ("priority", "o", 0x0800), ("correlation_id", "n", 0x0400),
         ("reply_to", "n", 0x0200), ("expiration", "n", 0x0100), ("message_id", "n", 0x0080),
         ("timestamp", "L", 0x0040), ("type", "n", 0x0020), ("user_id", "n", 0x0010),
         ("app_id", "n", 0x0008), ("cluster_id", "n", 0x0004)]


def rprops(pika_safe):
    props = {}
    for name, letter, _bit in PROPS:
        if rnd.random() < 0.4:
            if letter == "n":
                props[name] = rtext() or "v"
            elif letter == "t":
                props[name] = rtable(0, pika_safe)
            elif letter == "o":
                props[name] = rnd.randint(0, 9) if name == "priority" else rnd.choice([1, 2])
            else:
                props[name] = rnd.randint(0, 2**40)
    return props


def enc_props(props):
    flags = 0
    body = b""
    for name, letter, bit in PROPS:
        if name not in props:
            continue
        flags |= bit
        v = props[name]
        if letter == "n":
            body += shortstr(v)
        elif letter == "t":
            body += enc_table(v)
        elif letter == "o":
            body += struct.pack(">B", v)
        else:
            body += struct.pack(">Q", v)
    return struct.pack(">H", flags) + body


def render_header(cid, size, props):
    parts = []
    for name, letter, _bit in PROPS:
        if name not in props:
            continue
        v = props[name]
        if letter == "n":
            v = json.dumps(v, ensure_ascii=False)
        elif letter == "t":
            v = render_table(v)
        parts.append("%s=%s" % (name, v))
    return "H %d %d %s" % (cid, size, " ".join(parts))


def pika_props_roundtrip(prop_bytes):
    p = pika.spec.BasicProperties()
    p.decode(prop_bytes)
    return b"".join(p.encode())


# ---- the vectors -----------------------------------------------------

cases = []   # (frame hex, frame_max, expected line)
checked = 0
for (cid, mid), sig in sorted(SIGS.items()):
    for n in range(6):
        safe = n % 2 == 0
        args = [rarg(letter, safe) for letter in sig]
        body = enc_args(sig, args)
        if safe:
            assert pika_method_roundtrip(cid, mid, body) == body, (cid, mid, args)
            checked += 1
        payload = struct.pack(">HH", cid, mid) + body
        cases.append((frame(1, rnd.randint(0, 2047), payload).hex(), 0, render_method(cid, mid, sig, args)))

for n in range(60):
    safe = n % 2 == 0
    props = rprops(safe)
    pb = enc_props(props)
    if safe:
        assert pika_props_roundtrip(pb) == pb, props
        checked += 1
    size = rnd.choice([0, 1, 131072, 2**40])
    payload = struct.pack(">HHQ", 60, 0, size) + pb
    cases.append((frame(2, rnd.randint(1, 100), payload).hex(), 0, render_header(60, size, props)))

for _ in range(10):
    body = rbytes()
    cases.append((frame(3, 7, body).hex(), 0, "B %d %s" % (len(body), body.hex())))
cases.append((frame(8, 0, b"").hex(), 0, "HB"))

# a frame that is not all there yet
whole = frame(1, 1, struct.pack(">HH", 60, 80) + enc_args("Lb", [5, 1]))
cases.append((whole[:6].hex(), 0, "more"))
cases.append((whole[:-1].hex(), 0, "more"))

BAD = [
    frame(1, 1, struct.pack(">HH", 60, 80) + enc_args("Lb", [5, 1]))[:-1] + b"\x00",    # end octet
    frame(5, 1, b"x"),                                                                   # frame type
    frame(1, 1, struct.pack(">HH", 99, 1)),                                              # unknown method
    frame(1, 1, struct.pack(">HH", 60, 80) + enc_args("Lb", [5, 1]) + b"\x00"),          # trailing byte
    frame(1, 1, struct.pack(">HH", 60, 80) + b"\x00\x00"),                               # truncated
    frame(1, 1, struct.pack(">HH", 50, 10) + struct.pack(">H", 0) + shortstr("q") + b"\x00" +
          struct.pack(">I", 10) + b"\x01k"),                                             # table past its end
    frame(1, 1, struct.pack(">HH", 50, 10) + struct.pack(">H", 0) + shortstr("q") + b"\x00" +
          struct.pack(">I", 3) + b"\x01kZ"),                                             # field type Z
    frame(1, 1, struct.pack(">HH", 60, 21) + b"\x02a\x00"),                              # NUL in a shortstr
    frame(1, 1, struct.pack(">HH", 60, 21) + b"\x02\xc3\x28"),                           # not UTF-8
    frame(2, 1, struct.pack(">HHQH", 60, 0, 1, 0x0001)),                                 # more flags
    frame(2, 1, struct.pack(">HHQH", 60, 0, 1, 0x0000) + b"\x00"),                       # trailing byte
]
nest = b"V"
for _ in range(40):
    inner = struct.pack(">I", len(nest)) + nest
    nest = b"A" + inner
BAD.append(frame(1, 1, struct.pack(">HH", 50, 10) + struct.pack(">H", 0) + shortstr("q") + b"\x00" +
                 enc_table([]) [:0] + struct.pack(">I", 2 + len(nest)) + b"\x01k" + nest))     # 41 levels
for b in BAD:
    cases.append((b.hex(), 0, "err"))
cases.append((frame(3, 1, b"x" * 200).hex(), 128, "err"))                              # over frame-max

# ---- output ----------------------------------------------------------

HELPERS = r'''// Generated by tests/wire/gen.py -- do not edit.

import "amqp";
import "encoding";
import "json";
import "math";
import "strings";

fn hex_be(n: int, width: int) -> str {
    let b = strings.bytes_zero(width);
    for k in 0..width {
        b[width - 1 - k] = (n >> (8 * k)) & 255;
    }
    return encoding.hex_encode(b);
}

fn render_value(v: amqp.Value) -> str {
    let c = v.code;
    let cs = strings.from_bytes(strings.bytes_zero(0), 0, 0);
    let one = strings.bytes_zero(1);
    one[0] = c;
    cs = to_str(one);
    if c == 102 {
        return cs + hex_be(math.f32_bits(v.f as f32) as int, 4);
    }
    if c == 100 {
        return cs + hex_be(math.float_bits(v.f) as int, 8);
    }
    if c == 68 {
        return "D" + to_str(v.scale) + "/" + to_str(v.i);
    }
    if c == 83 || c == 120 {
        return cs + encoding.hex_encode(v.b);
    }
    if c == 86 {
        return "V";
    }
    if c == 70 {
        return "F" + render_table(v.t ?? amqp.table());
    }
    if c == 65 {
        let parts: [str] = [];
        let items: [amqp.Value] = v.a ?? [];
        for it in items {
            push(parts, render_value(it));
        }
        return "A[" + strings.join(parts, ",") + "]";
    }
    return cs + to_str(v.i);
}

fn render_table(t: amqp.Table) -> str {
    let parts: [str] = [];
    for k in 0..len(t.keys) {
        push(parts, json.encode(t.keys[k]) + "=" + render_value(t.vals[k]));
    }
    return "{" + strings.join(parts, ",") + "}";
}

fn render_method(m: amqp.Method) -> str {
    let sig = to_bytes(amqp.signature(m.class_id, m.method_id));
    let parts: [str] = [];
    for k in 0..len(sig) {
        let c = sig[k];
        let a = m.args[k];
        if c == 110 {
            push(parts, json.encode(a.s));
        } else if c == 83 {
            push(parts, encoding.hex_encode(a.b));
        } else if c == 116 {
            push(parts, render_table(a.t ?? amqp.table()));
        } else {
            push(parts, to_str(a.i));
        }
    }
    return "M " + to_str(m.class_id) + "." + to_str(m.method_id) + " " + strings.join(parts, "|");
}

fn prop(parts: [str], name: str, v: str) {
    push(parts, name + "=" + v);
}

fn render_header(h: amqp.ContentHeader) -> str {
    let p = h.props;
    let parts: [str] = [];
    if p.content_type != "" { prop(parts, "content_type", json.encode(p.content_type)); }
    if p.content_encoding != "" { prop(parts, "content_encoding", json.encode(p.content_encoding)); }
    if let t = p.headers { prop(parts, "headers", render_table(t)); }
    if p.delivery_mode != 0 { prop(parts, "delivery_mode", to_str(p.delivery_mode)); }
    if p.priority >= 0 { prop(parts, "priority", to_str(p.priority)); }
    if p.correlation_id != "" { prop(parts, "correlation_id", json.encode(p.correlation_id)); }
    if p.reply_to != "" { prop(parts, "reply_to", json.encode(p.reply_to)); }
    if p.expiration != "" { prop(parts, "expiration", json.encode(p.expiration)); }
    if p.message_id != "" { prop(parts, "message_id", json.encode(p.message_id)); }
    if p.timestamp >= 0 { prop(parts, "timestamp", to_str(p.timestamp)); }
    if p.type_name != "" { prop(parts, "type", json.encode(p.type_name)); }
    if p.user_id != "" { prop(parts, "user_id", json.encode(p.user_id)); }
    if p.app_id != "" { prop(parts, "app_id", json.encode(p.app_id)); }
    if p.cluster_id != "" { prop(parts, "cluster_id", json.encode(p.cluster_id)); }
    return "H " + to_str(h.class_id) + " " + to_str(h.body_size) + " " + strings.join(parts, " ");
}

// Decode the frame, render it, and encode it again: the bytes must match.
fn check(hex: str, frame_max: int) {
    let hr = encoding.hex_decode(hex);
    guard let b = hr else { println("bad hex"); return; }
    let sr = amqp.scan_frame(b, 0, len(b), frame_max);
    guard let s = sr else { println("err"); return; }
    if s.status == 0 {
        println("more");
        return;
    }
    let f = s.frame;
    let line = "";
    let again = b"";
    if f.kind == 1 {
        let mr = amqp.decode_method(f.payload);
        guard let m = mr else { println("err"); return; }
        line = render_method(m);
        let er = amqp.encode_method(m);
        guard let e = er else let x = err_of(er) { println("encode failed: " + x); return; }
        again = e;
    } else if f.kind == 2 {
        let hr2 = amqp.decode_header(f.payload);
        guard let h = hr2 else { println("err"); return; }
        line = render_header(h);
        let er = amqp.encode_header(h.class_id, h.body_size, h.props);
        guard let e = er else let x = err_of(er) { println("encode failed: " + x); return; }
        again = e;
    } else if f.kind == 3 {
        line = "B " + to_str(len(f.payload)) + " " + encoding.hex_encode(f.payload);
        again = f.payload;
    } else {
        line = "HB";
    }
    let whole = amqp.encode_frame(f.kind, f.channel, again);
    if encoding.hex_encode(whole) != hex {
        line = line + " (re-encoded differently: " + encoding.hex_encode(whole) + ")";
    }
    println(line);
}

'''

src = [HELPERS]
for h, fm, _want in cases:
    src.append('check("%s", %d);' % (h, fm))
with open(os.path.join(HERE, "main.sl"), "w") as f:
    f.write("\n".join(src) + "\n")
with open(os.path.join(HERE, "expected.txt"), "w") as f:
    f.write("\n".join(w for _h, _fm, w in cases) + "\n")
print("%d frames (%d checked by pika's round trip)" % (len(cases), checked))
