// AMQP 0-9-1 methods: one codec driven by each method's argument
// signature, taken from the spec (amqp0-9-1.xml), for the connection,
// channel, exchange, queue, basic and confirm classes. Signature letters:
//
//     o octet   s short   l long   L long-long   T timestamp
//     n short string   S long string   t table   b bit
//
// Consecutive bits share an octet, the first in its lowest bit.

import "builder";

// Class ids.
let C_CONNECTION = 10;
let C_CHANNEL = 20;
let C_EXCHANGE = 40;
let C_QUEUE = 50;
let C_BASIC = 60;
let C_CONFIRM = 85;

// One argument; which field holds it follows its signature letter: o s
// l L T b in i, n in s, S in b, t in t.
pub gc struct Arg {
    i: int,
    s: str,
    b: bytes,
    t: opt[Table],
}

pub gc struct Method {
    class_id: int,
    method_id: int,
    args: [Arg],
}

pub fn arg_int(n: int) -> Arg {
    return Arg { i: n, s: "", b: b"", t: none };
}

pub fn arg_bool(x: bool) -> Arg {
    let a = arg_int(0);
    if x {
        a.i = 1;
    }
    return a;
}

pub fn arg_str(s: str) -> Arg {
    return Arg { i: 0, s: s, b: b"", t: none };
}

pub fn arg_bytes(b: bytes) -> Arg {
    return Arg { i: 0, s: "", b: b, t: none };
}

pub fn arg_table(t: Table) -> Arg {
    return Arg { i: 0, s: "", b: b"", t: some(t) };
}

pub fn method(class_id: int, method_id: int, args: [Arg]) -> Method {
    return Method { class_id: class_id, method_id: method_id, args: args };
}

// The signature of class.method, or "?" for one this package does not
// speak.
pub fn signature(class_id: int, method_id: int) -> str {
    let k = class_id * 1000 + method_id;
    // connection
    if k == 10010 {
        return "ootSS";
    }
    if k == 10011 {
        return "tnSn";
    }
    if k == 10020 || k == 10021 {
        return "S";
    }
    if k == 10030 || k == 10031 {
        return "sls";
    }
    if k == 10040 {
        return "nnb";
    }
    if k == 10041 || k == 10060 {
        return "n";
    }
    if k == 10050 {
        return "snss";
    }
    if k == 10051 || k == 10061 {
        return "";
    }
    // channel
    if k == 20010 {
        return "n";
    }
    if k == 20011 {
        return "S";
    }
    if k == 20020 || k == 20021 {
        return "b";
    }
    if k == 20040 {
        return "snss";
    }
    if k == 20041 {
        return "";
    }
    // exchange
    if k == 40010 {
        return "snnbbbbbt";
    }
    if k == 40020 {
        return "snbb";
    }
    if k == 40030 || k == 40040 {
        return "snnnbt";
    }
    if k == 40011 || k == 40021 || k == 40031 || k == 40051 {
        return "";
    }
    // queue
    if k == 50010 {
        return "snbbbbbt";
    }
    if k == 50011 {
        return "nll";
    }
    if k == 50020 {
        return "snnnbt";
    }
    if k == 50030 {
        return "snb";
    }
    if k == 50031 || k == 50041 {
        return "l";
    }
    if k == 50040 {
        return "snbbb";
    }
    if k == 50050 {
        return "snnnt";
    }
    if k == 50021 || k == 50051 {
        return "";
    }
    // basic
    if k == 60010 {
        return "lsb";
    }
    if k == 60020 {
        return "snnbbbbt";
    }
    if k == 60021 {
        return "n";
    }
    if k == 60030 {
        return "nb";
    }
    if k == 60031 {
        return "n";
    }
    if k == 60040 {
        return "snnbb";
    }
    if k == 60050 {
        return "snnn";
    }
    if k == 60060 {
        return "nLbnn";
    }
    if k == 60070 {
        return "snb";
    }
    if k == 60071 {
        return "Lbnnl";
    }
    if k == 60072 {
        return "n";
    }
    if k == 60080 || k == 60090 {
        return "Lb";
    }
    if k == 60100 || k == 60110 {
        return "b";
    }
    if k == 60120 {
        return "Lbb";
    }
    if k == 60011 || k == 60111 {
        return "";
    }
    // confirm
    if k == 85010 {
        return "b";
    }
    if k == 85011 {
        return "";
    }
    return "?";
}

// A method frame's payload: class, method, then the arguments.
pub fn encode_method(m: Method) -> result[bytes, str] {
    let sig = to_bytes(signature(m.class_id, m.method_id));
    if len(sig) == 1 && sig[0] == 63 {
        return err("amqp: unknown method " + to_str(m.class_id) + "." + to_str(m.method_id));
    }
    if len(m.args) != len(sig) {
        return err("amqp: method " + to_str(m.class_id) + "." + to_str(m.method_id) + " takes " + to_str(len(sig)) +
                   " arguments, given " + to_str(len(m.args)));
    }
    let w = builder.new_bytes();
    wr_u16(w, m.class_id);
    wr_u16(w, m.method_id);
    let bits = 0;
    let nbits = 0;
    for k in 0..len(sig) {
        let c = sig[k];
        let a = m.args[k];
        if c == 98 {
            if nbits == 8 {
                wr_u8(w, bits);
                bits = 0;
                nbits = 0;
            }
            if a.i != 0 {
                bits = bits | (1 << nbits);
            }
            nbits += 1;
            continue;
        }
        if nbits > 0 {
            wr_u8(w, bits);
            bits = 0;
            nbits = 0;
        }
        if c == 111 {
            wr_u8(w, a.i);
        } else if c == 115 {
            wr_u16(w, a.i);
        } else if c == 108 {
            wr_u32(w, a.i);
        } else if c == 76 || c == 84 {
            wr_u64(w, a.i);
        } else if c == 110 {
            if !short_ok(a.s) {
                return err("amqp: a short string over 255 bytes in method " + to_str(m.class_id) + "." +
                           to_str(m.method_id));
            }
            wr_shortstr(w, a.s);
        } else if c == 83 {
            wr_longstr(w, a.b);
        } else if c == 116 {
            let tr = encode_table(w, a.t ?? table(), 0);
            guard let _t = tr else let e = err_of(tr) { return err(e); }
        }
    }
    if nbits > 0 {
        wr_u8(w, bits);
    }
    return ok(w.finish());
}

// A method frame's payload, read whole: anything left over is an error.
pub fn decode_method(payload: bytes) -> result[Method, str] {
    let r = reader(payload, 0, len(payload));
    let cid = rd_u16(r);
    let mid = rd_u16(r);
    if r.failed != "" {
        return err(r.failed);
    }
    let sig = to_bytes(signature(cid, mid));
    if len(sig) == 1 && sig[0] == 63 {
        return err("amqp: unknown method " + to_str(cid) + "." + to_str(mid));
    }
    let args: [Arg] = [];
    let bits = 0;
    let nbits = 8;
    for k in 0..len(sig) {
        let c = sig[k];
        if c == 98 {
            if nbits == 8 {
                bits = rd_u8(r);
                nbits = 0;
            }
            push(args, arg_int((bits >> nbits) & 1));
            nbits += 1;
            continue;
        }
        nbits = 8;
        if c == 111 {
            push(args, arg_int(rd_u8(r)));
        } else if c == 115 {
            push(args, arg_int(rd_u16(r)));
        } else if c == 108 {
            push(args, arg_int(rd_u32(r)));
        } else if c == 76 || c == 84 {
            push(args, arg_int(rd_u64(r)));
        } else if c == 110 {
            push(args, arg_str(rd_shortstr(r)));
        } else if c == 83 {
            push(args, arg_bytes(rd_longstr(r)));
        } else if c == 116 {
            push(args, arg_table(decode_table(r, 0)));
        }
    }
    if r.failed != "" {
        return err(r.failed);
    }
    if r.pos != r.end {
        return err("amqp: " + to_str(r.end - r.pos) + " bytes after the arguments of method " + to_str(cid) + "." +
                   to_str(mid));
    }
    return ok(method(cid, mid, args));
}
