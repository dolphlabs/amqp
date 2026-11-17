// Connections: the URL, the handshake, the reader, heartbeats and close.
//
//     let cr = amqp.dial("amqps://app:secret@mq.internal/prod", amqp.options(), deadline);
//     guard let conn = cr else let e = err_of(cr) { panic(e); }   // "amqp: ... 403 ACCESS_REFUSED ..."
//     ...
//     amqp.close(conn, deadline);
//
// URL: amqp://user:pass@host:port/vhost?heartbeat=30&frame_max=131072
// &channel_max=2047&auth_mechanism=external&connection_name=api. amqps://
// is TLS (port 5671), verified against the system trust store or
// Options.ca_path; the host name is checked. The vhost is the
// percent-decoded path, "/" when there is none ("/%2F" is "/" too).
// Credentials default to guest:guest, as RabbitMQ's do; they never appear
// in an error.
//
// One reader task per connection reads every frame: channel 0 is the
// connection's own (close, blocked), the rest go to their channels. A
// heartbeat task sends one when nothing else was written for half the
// negotiated interval, and the reader closes the connection after two
// intervals without a byte from the broker.

import "builder";
import "encoding";
import "net";
import "strings";
import "time";

let SECOND = 1000000000;
let STATE_OPEN = 1;
let STATE_CLOSING = 2;
let STATE_CLOSED = 3;
let READ_CHUNK = 65536;

pub gc struct Options {
    // seconds; 0 turns heartbeats off (when the broker agrees)
    heartbeat: int,
    // the largest frame offered, bytes (the broker may lower it)
    frame_max: int,
    channel_max: int,
    // CA file for amqps:// ("" is the system trust store)
    ca_path: str,
    // a client certificate and key: amqps:// with auth_mechanism=external
    cert_path: str,
    key_path: str,
    // shown in RabbitMQ's management UI
    connection_name: str,
    // the largest message body accepted from the broker, bytes
    max_body: int,
    // how long a write may take before the connection is closed
    write_timeout_ms: int,
}

pub fn options() -> Options {
    return Options { heartbeat: 60, frame_max: 131072, channel_max: 2047, ca_path: "", cert_path: "", key_path: "",
                     connection_name: "", max_body: 134217728, write_timeout_ms: 30000 };
}

gc struct Target {
    host: str,
    port: int,
    tls: bool,
    user: str,
    pass: str,
    vhost: str,
    external: bool,
}

gc struct Waiter {
    deadline: until,
    wake: chan[bool],
    done: bool,
}

pub gc struct Conn {
    opts: Options,
    fd: int,
    ssl: rawptr,
    tls_ctx: rawptr,
    state: int,
    // why the connection closed, once it has
    why: str,
    // negotiated
    frame_max: int,
    channel_max: int,
    heartbeat: int,
    server_properties: Table,
    // one message's frames are written together
    wlock: mutex,
    last_send: int,
    // channels by number, and the next number to try
    clock: mutex,
    chans: map[int]Channel,
    next_channel: int,
    // the broker's answer to our connection.close
    close_ok: chan[bool],
    // the reply code a protocol error found by the reader closes with
    fail_code: int,
    // connection.blocked: publishing is paused by the broker
    blocked: bool,
    blocked_reason: str,
    // deadline waiters, woken by the timer task
    wl: mutex,
    waiters: [Waiter],
    tick: chan[bool],
    reader_done: chan[bool],
}

// ---- URL ------------------------------------------------------------------

fn parse_url(url: str, o: Options) -> result[Target, str] {
    let t = Target { host: "", port: 5672, tls: false, user: "guest", pass: "guest", vhost: "/", external: false };
    let rest = "";
    if strings.has_prefix(url, "amqps://") {
        t.tls = true;
        t.port = 5671;
        rest = strings.slice(url, 8, len(url));
    } else if strings.has_prefix(url, "amqp://") {
        rest = strings.slice(url, 7, len(url));
    } else {
        return err("amqp: the URL must start with amqp:// or amqps://");
    }
    let query = "";
    let q = strings.find(rest, "?");
    if q >= 0 {
        query = strings.slice(rest, q + 1, len(rest));
        rest = strings.slice(rest, 0, q);
    }
    let path = "";
    let sl = strings.find(rest, "/");
    if sl >= 0 {
        path = strings.slice(rest, sl + 1, len(rest));
        rest = strings.slice(rest, 0, sl);
    }
    let at = strings.rfind(rest, "@");
    if at >= 0 {
        let cred = strings.slice(rest, 0, at);
        rest = strings.slice(rest, at + 1, len(rest));
        let colon = strings.find(cred, ":");
        let u = cred;
        let p = "";
        if colon >= 0 {
            u = strings.slice(cred, 0, colon);
            p = strings.slice(cred, colon + 1, len(cred));
        }
        let ur = encoding.url_decode(u);
        guard let ud = ur else { return err("amqp: the URL's user is not percent-encoded properly"); }
        let pr = encoding.url_decode(p);
        guard let pd = pr else { return err("amqp: the URL's password is not percent-encoded properly"); }
        t.user = ud;
        t.pass = pd;
    }
    // host, [v6]:port, host:port
    if strings.has_prefix(rest, "[") {
        let close = strings.find(rest, "]");
        if close < 0 {
            return err("amqp: an IPv6 host without its closing ]");
        }
        t.host = strings.slice(rest, 1, close);
        rest = strings.slice(rest, close + 1, len(rest));
        if strings.has_prefix(rest, ":") {
            let pr = parse_port(strings.slice(rest, 1, len(rest)));
            guard let port = pr else let e = err_of(pr) { return err(e); }
            t.port = port;
        } else if rest != "" {
            return err("amqp: unexpected text after the host");
        }
    } else {
        let colon = strings.rfind(rest, ":");
        if colon >= 0 {
            let pr = parse_port(strings.slice(rest, colon + 1, len(rest)));
            guard let port = pr else let e = err_of(pr) { return err(e); }
            t.port = port;
            rest = strings.slice(rest, 0, colon);
        }
        t.host = rest;
    }
    if t.host == "" {
        t.host = "localhost";
    }
    if path != "" {
        let vr = encoding.url_decode(path);
        guard let v = vr else { return err("amqp: the URL's vhost is not percent-encoded properly"); }
        t.vhost = v;
    }
    if query != "" {
        for kv in strings.split(query, "&") {
            let eq = strings.find(kv, "=");
            if eq < 0 {
                return err("amqp: URL option " + kv + " has no value");
            }
            let k = strings.slice(kv, 0, eq);
            let vr = encoding.url_decode(strings.slice(kv, eq + 1, len(kv)));
            guard let v = vr else { return err("amqp: URL option " + k + " is not percent-encoded properly"); }
            if k == "heartbeat" || k == "frame_max" || k == "channel_max" {
                let nr = to_int(v);
                guard let n = nr else { return err("amqp: URL option " + k + " must be a number"); }
                if n < 0 {
                    return err("amqp: URL option " + k + " must not be negative");
                }
                if k == "heartbeat" {
                    o.heartbeat = n;
                } else if k == "frame_max" {
                    o.frame_max = n;
                } else {
                    o.channel_max = n;
                }
            } else if k == "auth_mechanism" {
                if v == "external" || v == "EXTERNAL" {
                    t.external = true;
                } else if v != "plain" && v != "PLAIN" {
                    return err("amqp: auth_mechanism is plain or external");
                }
            } else if k == "connection_name" {
                o.connection_name = v;
            } else {
                return err("amqp: unknown URL option " + k);
            }
        }
    }
    return ok(t);
}

fn parse_port(s: str) -> result[int, str] {
    let r = to_int(s);
    guard let p = r else { return err("amqp: the URL's port must be a number"); }
    if p < 1 || p > 65535 {
        return err("amqp: the URL's port must be 1 to 65535");
    }
    return ok(p);
}

// ---- dialing and the handshake --------------------------------------------

// Connects, logs in and opens the vhost, all before deadline.
pub fn dial(url: str, o: Options, deadline: until) -> result[Conn, str] {
    let opts = Options { heartbeat: o.heartbeat, frame_max: o.frame_max, channel_max: o.channel_max, ca_path: o.ca_path,
                         cert_path: o.cert_path, key_path: o.key_path, connection_name: o.connection_name,
                         max_body: o.max_body, write_timeout_ms: o.write_timeout_ms };
    let tr = parse_url(url, opts);
    guard let t = tr else let e = err_of(tr) { return err(e); }
    if opts.frame_max != 0 && opts.frame_max < 4096 {
        return err("amqp: frame_max must be at least 4096 (the spec's minimum) or 0");
    }
    if opts.channel_max < 0 || opts.channel_max > 65535 || opts.heartbeat < 0 || opts.heartbeat > 65535 {
        return err("amqp: channel_max and heartbeat must be 0 to 65535");
    }
    if t.external && !t.tls {
        return err("amqp: auth_mechanism=external needs amqps:// and a client certificate");
    }
    let c = Conn { opts: opts, fd: -1, ssl: nullptr, tls_ctx: nullptr, state: 0, why: "", frame_max: 0,
                   channel_max: 0, heartbeat: 0, server_properties: table(), wlock: make_mutex(), last_send: 0,
                   clock: make_mutex(), chans: {}, next_channel: 1, close_ok: make_chan(1), fail_code: 0,
                   blocked: false,
                   blocked_reason: "", wl: make_mutex(), waiters: [], tick: make_chan(1),
                   reader_done: make_chan(1) };
    let dr = net.dial_until(t.host, t.port, deadline);
    guard let fd = dr else let e = err_of(dr) {
        return err("amqp: dial " + t.host + ":" + to_str(t.port) + ": " + e);
    }
    c.fd = fd as int;
    if t.tls {
        let ur = tls_upgrade(c, t, deadline);
        guard let _u = ur else let e = err_of(ur) {
            close_transport(c);
            return err(e);
        }
    }
    let rb = RBuf { data: b"", pos: 0 };
    let hr = handshake(c, t, rb, deadline);
    guard let _h = hr else let e = err_of(hr) {
        close_transport(c);
        return err(e);
    }
    c.state = STATE_OPEN;
    spawn read_loop(c, rb);
    spawn timer(c);
    if c.heartbeat > 0 {
        spawn heartbeater(c);
    }
    return ok(c);
}

fn tls_upgrade(c: Conn, t: Target, deadline: until) -> result[bool, str] {
    let cr = net.tls_client_ctx(c.opts.ca_path);
    guard let ctx = cr else let e = err_of(cr) { return err("amqp: tls: " + e); }
    if c.opts.cert_path != "" {
        let kr = net.tls_ctx_use_cert(ctx, c.opts.cert_path, c.opts.key_path);
        guard let _k = kr else let e = err_of(kr) { return err("amqp: tls: client certificate: " + e); }
    } else if t.external {
        return err("amqp: auth_mechanism=external needs Options.cert_path and key_path");
    }
    c.tls_ctx = ctx;
    let ur = net.tls_upgrade_until(c.fd, t.host, ctx, deadline);
    guard let ssl = ur else let e = err_of(ur) { return err("amqp: tls: " + e); }
    c.ssl = ssl;
    return ok(true);
}

fn client_properties(c: Conn) -> Table {
    let caps = table();
    let names = ["publisher_confirms", "exchange_exchange_bindings", "basic.nack", "consumer_cancel_notify",
                 "connection.blocked", "authentication_failure_close", "per_consumer_qos"];
    for n in names {
        table_set(caps, n, bool_value(true));
    }
    let p = table();
    table_set(p, "product", str_value("slang amqp"));
    table_set(p, "version", str_value("0.1.0"));
    table_set(p, "platform", str_value("slang"));
    table_set(p, "information", str_value("https://github.com/dolphlabs/amqp"));
    table_set(p, "capabilities", table_value(caps));
    if c.opts.connection_name != "" {
        table_set(p, "connection_name", str_value(c.opts.connection_name));
    }
    return p;
}

// 0 is no limit: the other side's value; else the smaller.
fn negotiate(ours: int, theirs: int) -> int {
    if ours == 0 || theirs == 0 {
        if ours > theirs {
            return ours;
        }
        return theirs;
    }
    if ours < theirs {
        return ours;
    }
    return theirs;
}

fn handshake(c: Conn, t: Target, rb: RBuf, deadline: until) -> result[bool, str] {
    let wr = write_raw(c, protocol_header(), deadline);
    guard let _w = wr else let e = err_of(wr) { return err(e); }
    // connection.start (or, from a broker that wants another version, its
    // protocol header)
    let fr = next_frame(c, rb, deadline, 0, true);
    guard let f = fr else let e = err_of(fr) { return err(e); }
    let sr = expect_method(f, 10, 10, "connection.start");
    guard let start = sr else let e = err_of(sr) { return err(e); }
    if start.args[0].i != 0 || start.args[1].i != 9 {
        return err("amqp: the broker speaks AMQP " + to_str(start.args[0].i) + "-" + to_str(start.args[1].i) +
                   ", not 0-9-1");
    }
    c.server_properties = start.args[2].t ?? table();
    let mechs = " " + to_str(start.args[3].b) + " ";
    let mech = "PLAIN";
    let response = b"";
    if t.external {
        mech = "EXTERNAL";
    } else {
        let w = builder.new_bytes();
        w.write_byte(0);
        w.write(to_bytes(t.user));
        w.write_byte(0);
        w.write(to_bytes(t.pass));
        response = w.finish();
    }
    if !strings.contains(mechs, " " + mech + " ") {
        return err("amqp: the broker does not offer " + mech + " authentication (it offers" +
                   strings.trim_end(mechs) + ")");
    }
    let sok = method(10, 11, [arg_table(client_properties(c)), arg_str(mech), arg_bytes(response), arg_str("en_US")]);
    let swr = send_method_raw(c, 0, sok, deadline);
    guard let _s = swr else let e = err_of(swr) { return err(e); }
    // connection.tune; a broker that refuses the login closes here
    let tfr = next_frame(c, rb, deadline, 0, false);
    guard let tf = tfr else let e = err_of(tfr) {
        if strings.contains(e, "closed by the broker") {
            return err("amqp: the broker closed the connection during login (wrong user or password?)");
        }
        return err(e);
    }
    let tur = expect_method(tf, 10, 30, "connection.tune");
    guard let tune = tur else let e = err_of(tur) { return err(e); }
    c.channel_max = negotiate(c.opts.channel_max, tune.args[0].i);
    if c.channel_max == 0 || c.channel_max > 65535 {
        c.channel_max = 65535;
    }
    c.frame_max = negotiate(c.opts.frame_max, tune.args[1].i);
    c.heartbeat = negotiate(c.opts.heartbeat, tune.args[2].i);
    if c.opts.heartbeat == 0 {
        c.heartbeat = 0;
    }
    let tok = method(10, 31, [arg_int(c.channel_max), arg_int(c.frame_max), arg_int(c.heartbeat)]);
    let twr = send_method_raw(c, 0, tok, deadline);
    guard let _t2 = twr else let e = err_of(twr) { return err(e); }
    let open = method(10, 40, [arg_str(t.vhost), arg_str(""), arg_bool(false)]);
    let owr = send_method_raw(c, 0, open, deadline);
    guard let _o = owr else let e = err_of(owr) { return err(e); }
    let ofr = next_frame(c, rb, deadline, c.frame_max, false);
    guard let of = ofr else let e = err_of(ofr) { return err(e); }
    let oor = expect_method(of, 10, 41, "connection.open-ok");
    guard let _ok = oor else let e = err_of(oor) { return err(e); }
    return ok(true);
}

// The method in frame f, if it is class.method on channel 0; a
// connection.close instead is the broker's reason.
fn expect_method(f: Frame, cid: int, mid: int, name: str) -> result[Method, str] {
    if f.kind != 1 || f.channel != 0 {
        return err("amqp: expected " + name + ", got a frame of type " + to_str(f.kind) + " on channel " +
                   to_str(f.channel));
    }
    let mr = decode_method(f.payload);
    guard let m = mr else let e = err_of(mr) { return err(e); }
    if m.class_id == 10 && m.method_id == 50 {
        return err(close_reason(m));
    }
    if m.class_id != cid || m.method_id != mid {
        return err("amqp: expected " + name + ", got method " + to_str(m.class_id) + "." + to_str(m.method_id));
    }
    return ok(m);
}

// connection.close or channel.close as an error: "amqp: 404 NOT_FOUND -
// no queue 'jobs' in vhost '/'".
fn close_reason(m: Method) -> str {
    return "amqp: " + to_str(m.args[0].i) + " " + m.args[1].s;
}

// ---- reading --------------------------------------------------------------

gc struct RBuf {
    data: bytes,
    pos: int,
}

fn rbuf_add(rb: RBuf, b: bytes) {
    if rb.pos == len(rb.data) {
        rb.data = b;
        rb.pos = 0;
        return;
    }
    if rb.pos > 0 {
        rb.data = rb.data[rb.pos..] + b;
        rb.pos = 0;
        return;
    }
    rb.data = rb.data + b;
}

// The next whole frame, reading until deadline. first: the reply to the
// protocol header, where a broker that wants another version answers
// with its own header instead of a frame.
fn next_frame(c: Conn, rb: RBuf, deadline: until, frame_max: int, first: bool) -> result[Frame, str] {
    while true {
        if first && len(rb.data) - rb.pos >= 4 && rb.data[rb.pos] == 65 && rb.data[rb.pos + 1] == 77 {
            return err("amqp: the broker does not speak AMQP 0-9-1 (it answered with its own protocol header)");
        }
        let sr = scan_frame(rb.data, rb.pos, len(rb.data), frame_max);
        guard let s = sr else let e = err_of(sr) { return err(e); }
        if s.status == 1 {
            rb.pos = s.next;
            return ok(s.frame);
        }
        let rr = recv(c, READ_CHUNK, deadline);
        guard let b = rr else let e = err_of(rr) {
            if e == "timeout" {
                return err("amqp: timed out waiting for the broker");
            }
            return err("amqp: read: " + e);
        }
        if len(b) == 0 {
            return err("amqp: the connection was closed by the broker");
        }
        rbuf_add(rb, b);
    }
    return err("amqp: unreachable");
}

fn recv(c: Conn, n: int, deadline: until) -> result[bytes, str] {
    if c.ssl != nullptr {
        return net.tls_recv_until(c.ssl, n, deadline);
    }
    return net.recv_until(c.fd, n, deadline);
}

// Reads frames until the connection closes. Silence for two heartbeat
// intervals is a dead connection.
fn read_loop(c: Conn, rb: RBuf) {
    while c.state != STATE_CLOSED {
        let deadline = until_never();
        if c.heartbeat > 0 {
            deadline = until_of(time.mono() + 2 * c.heartbeat * SECOND);
        }
        let fr = next_frame(c, rb, deadline, c.frame_max, false);
        guard let f = fr else let e = err_of(fr) {
            if c.state == STATE_CLOSED {
                break;
            }
            if strings.contains(e, "timed out") {
                shut_down(c, "amqp: no heartbeat from the broker for " + to_str(2 * c.heartbeat) + " s");
            } else if is_io_error(e) {
                shut_down(c, e);
            } else {
                // a frame the reader cannot take: 501 FRAME_ERROR
                hard_close(c, 501, e);
            }
            break;
        }
        c.fail_code = 505;
        let hr = handle_frame(c, f);
        guard let _h = hr else let e = err_of(hr) {
            // a protocol error: tell the broker why (504 CHANNEL_ERROR, 502
            // SYNTAX_ERROR or 505 UNEXPECTED_FRAME), then drop the connection
            hard_close(c, c.fail_code, e);
            break;
        }
    }
    select {
        case chan_send(c.reader_done, true) { }
        default { }
    }
}

// The reader's failures that are the socket's, not the broker's bytes.
fn is_io_error(e: str) -> bool {
    return strings.has_prefix(e, "amqp: read:") || strings.has_prefix(e, "amqp: the connection was closed");
}

fn handle_frame(c: Conn, f: Frame) -> result[bool, str] {
    if f.kind == 8 {
        if f.channel != 0 {
            return err("amqp: a heartbeat on channel " + to_str(f.channel));
        }
        return ok(true);
    }
    if f.channel == 0 {
        if f.kind != 1 {
            return err("amqp: a content frame on channel 0");
        }
        let mr = decode_method(f.payload);
        guard let m = mr else let e = err_of(mr) {
            c.fail_code = 502;
            return err(e);
        }
        return connection_method(c, m);
    }
    return channel_frame(c, f);
}

fn connection_method(c: Conn, m: Method) -> result[bool, str] {
    if m.class_id != 10 {
        return err("amqp: method " + to_str(m.class_id) + "." + to_str(m.method_id) + " on channel 0");
    }
    if m.method_id == 50 {
        // the broker closes the connection: answer, then stop
        let reason = close_reason(m);
        let okm = method(10, 51, []);
        send_method(c, 0, okm);
        shut_down(c, reason);
        return ok(true);
    }
    if m.method_id == 51 {
        select {
            case chan_send(c.close_ok, true) { }
            default { }
        }
        return ok(true);
    }
    if m.method_id == 60 {
        c.blocked = true;
        c.blocked_reason = m.args[0].s;
        return ok(true);
    }
    if m.method_id == 61 {
        c.blocked = false;
        c.blocked_reason = "";
        return ok(true);
    }
    return err("amqp: unexpected method 10." + to_str(m.method_id));
}

// ---- writing --------------------------------------------------------------

fn write_raw(c: Conn, data: bytes, deadline: until) -> result[bool, str] {
    let off = 0;
    while off < len(data) {
        let chunk = data;
        if off > 0 {
            chunk = data[off..];
        }
        let r = net_send(c, chunk, deadline);
        guard let n = r else let e = err_of(r) { return err("amqp: write: " + e); }
        if n <= 0 {
            return err("amqp: write: the connection is closed");
        }
        off += n as int;
    }
    c.last_send = time.mono() as int;
    return ok(true);
}

fn net_send(c: Conn, b: bytes, deadline: until) -> result[i32, str] {
    if c.ssl != nullptr {
        return net.tls_send_until(c.ssl, b, deadline);
    }
    return net.send_until(c.fd, b, deadline);
}

fn send_method_raw(c: Conn, channel: int, m: Method, deadline: until) -> result[bool, str] {
    let er = encode_method(m);
    guard let p = er else let e = err_of(er) { return err(e); }
    return write_raw(c, encode_frame(1, channel, p), deadline);
}

// Frames written together, under the write lock. A failed write closes
// the connection: the stream is at an unknown offset.
fn send_frames(c: Conn, data: bytes) -> result[bool, str] {
    if c.state == STATE_CLOSED {
        return err(closed_error(c));
    }
    mutex_lock(c.wlock);
    let wr = write_raw(c, data, until_of(time.mono() + c.opts.write_timeout_ms * 1000000));
    mutex_unlock(c.wlock);
    guard let _w = wr else let e = err_of(wr) {
        shut_down(c, e);
        return err(e);
    }
    return ok(true);
}

fn send_method(c: Conn, channel: int, m: Method) -> result[bool, str] {
    let er = encode_method(m);
    guard let p = er else let e = err_of(er) { return err(e); }
    return send_frames(c, encode_frame(1, channel, p));
}

fn heartbeater(c: Conn) {
    let half = c.heartbeat * SECOND / 2;
    while c.state != STATE_CLOSED {
        time.sleep(half);
        if c.state == STATE_CLOSED {
            break;
        }
        if (time.mono() as int) - c.last_send >= half {
            send_frames(c, encode_frame(8, 0, b""));
        }
    }
}

// ---- deadlines ------------------------------------------------------------

fn timer(c: Conn) {
    while c.state != STATE_CLOSED {
        mutex_lock(c.wl);
        let any = len(c.waiters) > 0;
        mutex_unlock(c.wl);
        if !any {
            let t = chan_recv(c.tick);
            if let _v = t {
                continue;
            }
            break;
        }
        time.sleep(5000000);
        mutex_lock(c.wl);
        let keep: [Waiter] = [];
        for w in c.waiters {
            if w.done {
                continue;
            }
            if until_hit(w.deadline) {
                w.done = true;
                select {
                    case chan_send(w.wake, true) { }
                    default { }
                }
            } else {
                push(keep, w);
            }
        }
        c.waiters = keep;
        mutex_unlock(c.wl);
    }
}

// A waiter whose wake channel fires at deadline (or when the connection
// closes). Mark it done when the wait is over.
fn add_waiter(c: Conn, deadline: until) -> Waiter {
    let w = Waiter { deadline: deadline, wake: make_chan(1), done: false };
    if c.state == STATE_CLOSED {
        w.done = true;
        select {
            case chan_send(w.wake, true) { }
            default { }
        }
        return w;
    }
    mutex_lock(c.wl);
    push(c.waiters, w);
    mutex_unlock(c.wl);
    select {
        case chan_send(c.tick, true) { }
        default { }
    }
    return w;
}

// ---- closing --------------------------------------------------------------

// Closes the connection: connection.close, the broker's close-ok (or the
// deadline), then the socket. Every channel and consumer on it ends.
pub fn close(c: Conn, deadline: until) -> result[bool, str] {
    if c.state == STATE_CLOSED {
        return ok(true);
    }
    c.state = STATE_CLOSING;
    let m = method(10, 50, [arg_int(200), arg_str("goodbye"), arg_int(0), arg_int(0)]);
    let sr = send_method(c, 0, m);
    if let _s = sr {
        let w = add_waiter(c, deadline);
        select {
            case let _o = chan_recv(c.close_ok) { }
            case let _t = chan_recv(w.wake) { }
        }
        w.done = true;
    }
    shut_down(c, "amqp: the connection was closed");
    return ok(true);
}

// Tells the broker why (connection.close with code), then drops the
// connection without waiting.
fn hard_close(c: Conn, code: int, why: str) {
    let m = method(10, 50, [arg_int(code), arg_str(trim_short(why)), arg_int(0), arg_int(0)]);
    send_method(c, 0, m);
    shut_down(c, why);
}

fn trim_short(s: str) -> str {
    if len(s) <= 255 {
        return s;
    }
    return strings.slice(s, 0, 255);
}

fn shut_down(c: Conn, why: str) {
    if c.state == STATE_CLOSED {
        return;
    }
    c.state = STATE_CLOSED;
    if c.why == "" {
        c.why = why;
    }
    close_transport(c);
    close_channels(c);
    mutex_lock(c.wl);
    let ws = c.waiters;
    c.waiters = [];
    mutex_unlock(c.wl);
    for w in ws {
        select {
            case chan_send(w.wake, true) { }
            default { }
        }
    }
    select {
        case chan_send(c.close_ok, true) { }
        default { }
    }
    chan_close(c.tick);
}

fn close_transport(c: Conn) {
    if c.ssl != nullptr {
        net.tls_close(c.ssl);
        c.ssl = nullptr;
    }
    if c.fd >= 0 {
        net.close(c.fd);
        c.fd = -1;
    }
}

fn closed_error(c: Conn) -> str {
    if c.why != "" {
        return c.why;
    }
    return "amqp: the connection is closed";
}

pub fn is_closed(c: Conn) -> bool {
    return c.state == STATE_CLOSED;
}

// Why the connection closed ("" while it is open).
pub fn close_error(c: Conn) -> str {
    if c.state != STATE_CLOSED {
        return "";
    }
    return closed_error(c);
}

// Whether the broker has paused publishing (connection.blocked: memory or
// disk alarm), and why.
pub fn is_blocked(c: Conn) -> bool {
    return c.blocked;
}

pub fn blocked_reason(c: Conn) -> str {
    return c.blocked_reason;
}

pub fn server_properties(c: Conn) -> Table {
    return c.server_properties;
}

// The negotiated limits.
pub fn frame_max(c: Conn) -> int {
    return c.frame_max;
}

pub fn heartbeat(c: Conn) -> int {
    return c.heartbeat;
}

pub fn channel_max(c: Conn) -> int {
    return c.channel_max;
}
