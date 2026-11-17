// The connection against scripted fake brokers: one per scenario, each a
// task on an ephemeral port that plays the broker's side from the
// package's own codecs (checked byte for byte in tests/wire) and reports
// what it saw. Run with tests/run.sh.

import "amqp";
import "net";
import "strings";
import "time";



gc struct Srv {
    lfd: int,
    port: int,
    fd: int,
    data: bytes,
    pos: int,
    log: chan[str],
}

fn soon(s: int) -> until {
    return until_of(time.mono() + s * 1000000000);
}

fn listen() -> Srv {
    let lr = net.listen(0);
    guard let lfd = lr else let e = err_of(lr) { panic(e); }
    let pr = net.port(lfd);
    guard let port = pr else let e = err_of(pr) { panic(e); }
    return Srv { lfd: lfd as int, port: port as int, fd: -1, data: b"", pos: 0, log: make_chan(64) };
}

fn note(s: Srv, line: str) {
    chan_send(s.log, line);
}

fn accept(s: Srv) {
    let ar = net.accept(s.lfd);
    guard let fd = ar else let e = err_of(ar) { panic(e); }
    s.fd = fd as int;
}

// The next frame from the client; none if it closed or went quiet.
fn read_frame(s: Srv, secs: int) -> opt[amqp.Frame] {
    while true {
        let sr = amqp.scan_frame(s.data, s.pos, len(s.data), 0);
        guard let sc = sr else let e = err_of(sr) { note(s, "server: bad frame: " + e); return none; }
        if sc.status == 1 {
            s.pos = sc.next;
            return some(sc.frame);
        }
        let rr = net.recv_until(s.fd, 65536, soon(secs));
        guard let b = rr else { return none; }
        if len(b) == 0 {
            return none;
        }
        s.data = s.data[s.pos..] + b;
        s.pos = 0;
    }
    return none;
}

fn no_method() -> amqp.Method {
    return amqp.method(0, 0, [amqp.arg_int(0)]);
}

fn read_method(s: Srv) -> opt[amqp.Method] {
    while true {
        let f = read_frame(s, 5) ?? amqp.Frame { kind: 0, channel: 0, payload: b"" };
        if f.kind == 0 {
            return none;
        }
        if f.kind == 8 {
            continue;
        }
        let mr = amqp.decode_method(f.payload);
        guard let m = mr else let e = err_of(mr) { note(s, "server: " + e); return none; }
        return some(m);
    }
    return none;
}

fn send(s: Srv, data: bytes) {
    net.send_until(s.fd, data, soon(5));
}

fn send_method(s: Srv, ch: int, m: amqp.Method) {
    let er = amqp.encode_method(m);
    guard let p = er else let e = err_of(er) { panic(e); }
    send(s, amqp.encode_frame(1, ch, p));
}

fn start_method(mechs: str) -> amqp.Method {
    let props = amqp.table();
    amqp.table_set(props, "product", amqp.str_value("fake broker"));
    return amqp.method(10, 10, [amqp.arg_int(0), amqp.arg_int(9), amqp.arg_table(props), amqp.arg_bytes(to_bytes(mechs)),
                                amqp.arg_bytes(to_bytes("en_US"))]);
}

fn close_method(code: int, text: str) -> amqp.Method {
    return amqp.method(10, 50, [amqp.arg_int(code), amqp.arg_str(text), amqp.arg_int(0), amqp.arg_int(0)]);
}

// The protocol header, start, start-ok (noted), tune, tune-ok (noted),
// open, open-ok: a broker that lets the client in.
fn welcome(s: Srv, channel_max: int, frame_max: int, heartbeat: int) -> bool {
    accept(s);
    let hr = net.recv_until(s.fd, 8, soon(5));
    guard let h = hr else { return false; }
    if to_str(h[0..4]) != "AMQP" {
        note(s, "server: no protocol header");
        return false;
    }
    send_method(s, 0, start_method("AMQPLAIN PLAIN"));
    let sok = read_method(s) ?? no_method();
    if sok.class_id != 10 || sok.method_id != 11 {
        return false;
    }
    // PLAIN's response is NUL user NUL password
    let resp = sok.args[2].b;
    let user_pass = "";
    for k in 0..len(resp) {
        if resp[k] == 0 {
            if k > 0 {
                user_pass = user_pass + ":";
            }
        } else {
            user_pass = user_pass + to_str(resp[k..k + 1]);
        }
    }
    note(s, "server: start-ok " + sok.args[1].s + " " + user_pass + " product=" +
            (amqp.as_str(amqp.table_get(sok.args[0].t ?? amqp.table(), "product") ?? amqp.void_value()) ?? "?"));
    send_method(s, 0, amqp.method(10, 30, [amqp.arg_int(channel_max), amqp.arg_int(frame_max), amqp.arg_int(heartbeat)]));
    let tok = read_method(s) ?? no_method();
    note(s, "server: tune-ok channel_max=" + to_str(tok.args[0].i) + " frame_max=" + to_str(tok.args[1].i) +
            " heartbeat=" + to_str(tok.args[2].i));
    let open = read_method(s) ?? no_method();
    note(s, "server: open vhost=" + open.args[0].s);
    send_method(s, 0, amqp.method(10, 41, [amqp.arg_str("")]));
    return true;
}

fn drain_log(s: Srv) {
    while true {
        select {
            case let line = chan_recv(s.log) {
                println(line ?? "");
            }
            default {
                return;
            }
        }
    }
}

fn url(s: Srv, rest: str) -> str {
    return "amqp://app:s3cr%40t@127.0.0.1:" + to_str(s.port) + rest;
}

// ---- scenarios -----------------------------------------------------------

fn srv_ok(s: Srv) {
    if !welcome(s, 2047, 131072, 60) {
        return;
    }
    let m = read_method(s) ?? no_method();
    note(s, "server: client sent " + to_str(m.class_id) + "." + to_str(m.method_id) + " " + to_str(m.args[0].i));
    send_method(s, 0, amqp.method(10, 51, []));
}

fn scenario_ok() {
    println("-- handshake and graceful close");
    let s = listen();
    spawn srv_ok(s);
    let o = amqp.options();
    o.frame_max = 65536;
    o.connection_name = "tests";
    let cr = amqp.dial(url(s, "/%2Fprod?heartbeat=30"), o, soon(5));
    guard let c = cr else let e = err_of(cr) { println("dial: " + e); return; }
    println("client: frame_max=" + to_str(amqp.frame_max(c)) + " channel_max=" + to_str(amqp.channel_max(c)) +
            " heartbeat=" + to_str(amqp.heartbeat(c)) + " server=" +
            (amqp.as_str(amqp.table_get(amqp.server_properties(c), "product") ?? amqp.void_value()) ?? "?"));
    amqp.close(c, soon(5));
    println("client: closed=" + to_str(amqp.is_closed(c)) + " why=" + amqp.close_error(c));
    time.sleep(50000000);
    drain_log(s);
}

fn srv_refuse_login(s: Srv) {
    accept(s);
    net.recv_until(s.fd, 8, soon(5));
    send_method(s, 0, start_method("PLAIN"));
    read_method(s);
    send_method(s, 0, close_method(403, "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN."));
}

fn srv_drop_login(s: Srv) {
    accept(s);
    net.recv_until(s.fd, 8, soon(5));
    send_method(s, 0, start_method("PLAIN"));
    read_method(s);
    net.close(s.fd);
}

fn srv_refuse_vhost(s: Srv) {
    accept(s);
    net.recv_until(s.fd, 8, soon(5));
    send_method(s, 0, start_method("PLAIN"));
    read_method(s);
    send_method(s, 0, amqp.method(10, 30, [amqp.arg_int(0), amqp.arg_int(131072), amqp.arg_int(0)]));
    read_method(s);
    read_method(s);
    send_method(s, 0, close_method(530, "NOT_ALLOWED - vhost nope not found"));
}

fn srv_other_protocol(s: Srv) {
    accept(s);
    net.recv_until(s.fd, 8, soon(5));
    send(s, to_bytes("AMQP") + b"\x01\x01\x00\x0a");
    net.close(s.fd);
}

fn srv_no_plain(s: Srv) {
    accept(s);
    net.recv_until(s.fd, 8, soon(5));
    send_method(s, 0, start_method("AMQPLAIN"));
    read_method(s);
}

fn dial_error(name: str, f: fn(Srv), path: str) {
    println("-- " + name);
    let s = listen();
    spawn f(s);
    let cr = amqp.dial(url(s, path), amqp.options(), soon(5));
    guard let _c = cr else let e = err_of(cr) {
        println("client: " + e);
        return;
    }
    println("client: dialed, which it should not have");
}

fn srv_forced_close(s: Srv) {
    if !welcome(s, 0, 131072, 0) {
        return;
    }
    send_method(s, 0, close_method(320, "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"));
    let m = read_method(s) ?? no_method();
    note(s, "server: client answered " + to_str(m.class_id) + "." + to_str(m.method_id));
}

fn srv_bad_frame(s: Srv) {
    if !welcome(s, 0, 131072, 0) {
        return;
    }
    send(s, b"\x01\x00\x00\x00\x00\x00\x04\x00\x0a\x00\x33\x00");
    let m = read_method(s) ?? no_method();
    note(s, "server: client sent " + to_str(m.class_id) + "." + to_str(m.method_id) + " " + to_str(m.args[0].i));
}

fn srv_big_frame(s: Srv) {
    if !welcome(s, 0, 4096, 0) {
        return;
    }
    send(s, amqp.encode_frame(3, 1, strings.bytes_zero(5000)));
    let m = read_method(s) ?? no_method();
    note(s, "server: client sent " + to_str(m.class_id) + "." + to_str(m.method_id) + " " + to_str(m.args[0].i));
}

fn srv_unopened_channel(s: Srv) {
    if !welcome(s, 0, 131072, 0) {
        return;
    }
    send_method(s, 5, amqp.method(20, 41, []));
    let m = read_method(s) ?? no_method();
    note(s, "server: client sent " + to_str(m.class_id) + "." + to_str(m.method_id) + " " + to_str(m.args[0].i));
}

fn srv_silent(s: Srv) {
    if !welcome(s, 0, 131072, 1) {
        return;
    }
    // the client must send a heartbeat within a second
    let f = read_frame(s, 2) ?? amqp.Frame { kind: 0, channel: 0, payload: b"" };
    note(s, "server: got frame type " + to_str(f.kind) + " from an idle client");
}

fn srv_blocked(s: Srv) {
    if !welcome(s, 0, 131072, 0) {
        return;
    }
    send_method(s, 0, amqp.method(10, 60, [amqp.arg_str("low on memory")]));
    time.sleep(200000000);
    send_method(s, 0, amqp.method(10, 61, []));
    time.sleep(500000000);
}

// The connection, once the broker has done its part: closed or not, and why.
fn after(name: str, f: fn(Srv), wait_ms: int, opts: amqp.Options) -> amqp.Conn {
    println("-- " + name);
    let s = listen();
    spawn f(s);
    let cr = amqp.dial(url(s, ""), opts, soon(5));
    guard let c = cr else let e = err_of(cr) { panic("dial: " + e); }
    time.sleep(wait_ms * 1000000);
    println("client: closed=" + to_str(amqp.is_closed(c)) + " why=" + amqp.close_error(c));
    time.sleep(100000000);
    drain_log(s);
    return c;
}

scenario_ok();
dial_error("login refused", srv_refuse_login, "");
dial_error("login dropped", srv_drop_login, "");
dial_error("vhost refused", srv_refuse_vhost, "/nope");
dial_error("another protocol", srv_other_protocol, "");
dial_error("no PLAIN", srv_no_plain, "");
after("broker closes", srv_forced_close, 300, amqp.options());
after("malformed frame", srv_bad_frame, 300, amqp.options());
after("frame over frame-max", srv_big_frame, 300, amqp.options());
after("frame on an unopened channel", srv_unopened_channel, 300, amqp.options());
after("heartbeats", srv_silent, 2600, amqp.options());

println("-- blocked");
let bs = listen();
spawn srv_blocked(bs);
let bcr = amqp.dial(url(bs, ""), amqp.options(), soon(5));
guard let bc = bcr else let e = err_of(bcr) { panic(e); }
time.sleep(100000000);
println("client: blocked=" + to_str(amqp.is_blocked(bc)) + " reason=" + amqp.blocked_reason(bc));
time.sleep(300000000);
println("client: blocked=" + to_str(amqp.is_blocked(bc)));
amqp.close(bc, soon(1));
