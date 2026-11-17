// Channels: independent sessions multiplexed over one connection, and
// what is declared on them -- exchanges, queues, bindings -- and QoS.
//
//     let chr = amqp.channel(conn, deadline);
//     guard let ch = chr else let e = err_of(chr) { panic(e); }
//     let qr = amqp.queue_declare(ch, "jobs", amqp.durable(), deadline);    // result[Queue, str]
//     amqp.exchange_declare(ch, "events", "topic", amqp.exchange_options(), deadline);
//     amqp.queue_bind(ch, "jobs", "events", "order.*", amqp.table(), deadline);
//     amqp.qos(ch, 32, deadline);
//
// A channel makes one synchronous call at a time. An error the broker
// answers with closes the channel (404 NOT_FOUND, 406 PRECONDITION_FAILED,
// 403 ACCESS_REFUSED): the call returns the broker's reason and so does
// every later call on that channel; open another. A call that times out
// closes its channel too, as its late reply would be taken for the next
// call's.

import "builder";
import "strings";

let CH_OPEN = 1;
let CH_CLOSED = 2;

pub gc struct Channel {
    conn: Conn,
    id: int,
    state: int,
    // why the channel closed, once it has
    why: str,
    // one synchronous call at a time, and its reply
    rpc: mutex,
    replies: chan[Method],
    // channel.flow: false while the broker has paused this channel
    active: bool,
    // we sent channel.close and the broker has not answered: the number
    // stays taken, and every frame for it but close/close-ok is dropped
    closing: bool,
    // publishing: plock orders sequence numbers with writes; clock2
    // guards the confirm state below
    plock: mutex,
    clock2: mutex,
    confirming: bool,
    next_seq: int,
    unconfirmed: map[int]bool,
    nacked: map[int]bool,
    nack_seen: bool,
    confirm_waiters: [chan[bool]],
    returns: chan[Return],
    returns_dropped: int,
    // a message arriving: its method (in_kind 1 return, 2 deliver, 3
    // get-ok), then its header, then body frames up to in_size
    in_kind: int,
    in_method: Method,
    in_header: bool,
    in_props: Properties,
    in_size: int,
    in_body: builder.Bytes,
    in_got: int,
}

pub gc struct Queue {
    name: str,
    messages: int,
    consumers: int,
}

pub gc struct QueueOptions {
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    // only check that the queue exists (404 if not)
    passive: bool,
    // x-queue-type, x-message-ttl, x-max-priority, x-dead-letter-exchange, ...
    args: Table,
}

pub gc struct ExchangeOptions {
    durable: bool,
    auto_delete: bool,
    internal: bool,
    passive: bool,
    args: Table,
}

// Transient, not exclusive, not auto-deleted.
pub fn queue_options() -> QueueOptions {
    return QueueOptions { durable: false, exclusive: false, auto_delete: false, passive: false, args: table() };
}

// A durable queue: survives a broker restart.
pub fn durable() -> QueueOptions {
    let o = queue_options();
    o.durable = true;
    return o;
}

// Durable, as exchanges usually are.
pub fn exchange_options() -> ExchangeOptions {
    return ExchangeOptions { durable: true, auto_delete: false, internal: false, passive: false, args: table() };
}

// ---- opening and closing ---------------------------------------------------

// A new channel on c.
pub fn channel(c: Conn, deadline: until) -> result[Channel, str] {
    if c.state != STATE_OPEN {
        return err(closed_error(c));
    }
    mutex_lock(c.clock);
    let id = -1;
    let tries = 0;
    while tries < c.channel_max {
        let n = c.next_channel;
        c.next_channel += 1;
        if c.next_channel > c.channel_max {
            c.next_channel = 1;
        }
        tries += 1;
        if !has(c.chans, n) {
            id = n;
            break;
        }
    }
    if id < 0 {
        mutex_unlock(c.clock);
        return err("amqp: all " + to_str(c.channel_max) + " channels are open");
    }
    let ch = Channel { conn: c, id: id, state: CH_OPEN, why: "", rpc: make_mutex(), replies: make_chan(4),
                       active: true, closing: false, plock: make_mutex(), clock2: make_mutex(), confirming: false,
                       next_seq: 1, unconfirmed: {}, nacked: {}, nack_seen: false, confirm_waiters: [],
                       returns: make_chan(RETURN_BUFFER), returns_dropped: 0, in_kind: 0,
                       in_method: method(0, 0, []), in_header: false, in_props: properties(), in_size: 0,
                       in_body: builder.new_bytes(), in_got: 0 };
    c.chans[id] = ch;
    mutex_unlock(c.clock);
    let r = call(ch, method(20, 10, [arg_str("")]), 20, 11, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(ch);
}

// Closes the channel (channel.close, then the broker's close-ok or the
// deadline). Closing a closed channel does nothing.
pub fn close_channel(ch: Channel, deadline: until) -> result[bool, str] {
    if ch.state == CH_CLOSED {
        return ok(true);
    }
    let r = call(ch, method(20, 40, [arg_int(200), arg_str("bye"), arg_int(0), arg_int(0)]), 20, 41, deadline);
    end_channel(ch, "amqp: the channel was closed");
    guard let _m = r else let e = err_of(r) {
        if strings.contains(e, "closed") {
            return ok(true);
        }
        return err(e);
    }
    return ok(true);
}

pub fn channel_is_closed(ch: Channel) -> bool {
    return ch.state == CH_CLOSED;
}

// Why the channel closed: the broker's reason (404, 406, ...), the
// connection's, or "" while it is open.
pub fn channel_error(ch: Channel) -> str {
    if ch.state != CH_CLOSED {
        return "";
    }
    return ch.why;
}

// The channel is over: forget it, and wake a call waiting on it.
fn end_channel(ch: Channel, why: str) {
    if ch.state == CH_CLOSED {
        return;
    }
    ch.state = CH_CLOSED;
    if ch.why == "" {
        ch.why = why;
    }
    if !ch.closing {
        forget_channel(ch);
    }
    chan_close(ch.replies);
    chan_close(ch.returns);
    wake_confirm_waiters(ch);
}

// Frees the channel's number.
fn forget_channel(ch: Channel) {
    let c = ch.conn;
    mutex_lock(c.clock);
    if has(c.chans, ch.id) {
        del(c.chans, ch.id);
    }
    mutex_unlock(c.clock);
}

// The connection has closed: every channel on it ends with its reason.
fn close_channels(c: Conn) {
    mutex_lock(c.clock);
    let all: [Channel] = [];
    for _id, ch in c.chans {
        push(all, ch);
    }
    mutex_unlock(c.clock);
    for ch in all {
        end_channel(ch, closed_error(c));
    }
}

// ---- synchronous calls -----------------------------------------------------

// Sends m and waits for class.method in reply. The channel's reason is
// the error when the broker closes it instead.
fn call(ch: Channel, m: Method, cid: int, mid: int, deadline: until) -> result[Method, str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    mutex_lock(ch.rpc);
    let r = call_locked(ch, m, cid, mid, deadline);
    mutex_unlock(ch.rpc);
    return r;
}

fn call_locked(ch: Channel, m: Method, cid: int, mid: int, deadline: until) -> result[Method, str] {
    let sr = send_method(ch.conn, ch.id, m);
    guard let _s = sr else let e = err_of(sr) { return err(e); }
    let w = add_waiter(ch.conn, deadline);
    let got: opt[Method] = none;
    let timed_out = false;
    select {
        case let reply = chan_recv(ch.replies) {
            got = reply;
        }
        case let _t = chan_recv(w.wake) {
            timed_out = true;
        }
    }
    w.done = true;
    guard let reply = got else {
        if ch.state == CH_CLOSED {
            return err(ch.why);
        }
        if timed_out {
            // the reply may still come, and would be taken for the next
            // call's: this channel cannot be used again
            let why = "amqp: timed out waiting for the broker's reply to " + to_str(m.class_id) + "." +
                      to_str(m.method_id);
            ch.closing = true;
            end_channel(ch, why);
            send_method(ch.conn, ch.id, method(20, 40, [arg_int(200), arg_str("timed out"), arg_int(0), arg_int(0)]));
            return err(why);
        }
        return err(ch.why);
    }
    if reply.class_id != cid || reply.method_id != mid {
        let why = "amqp: expected method " + to_str(cid) + "." + to_str(mid) + " from the broker, got " +
                  to_str(reply.class_id) + "." + to_str(reply.method_id);
        hard_close(ch.conn, 505, why);
        return err(why);
    }
    return ok(reply);
}

// ---- frames from the reader ------------------------------------------------

// A frame for channel f.channel. A frame for a channel that is not open
// is a protocol error.
fn channel_frame(c: Conn, f: Frame) -> result[bool, str] {
    mutex_lock(c.clock);
    let found: opt[Channel] = none;
    if has(c.chans, f.channel) {
        found = some(c.chans[f.channel]);
    }
    mutex_unlock(c.clock);
    guard let ch = found else {
        c.fail_code = 504;
        return err("amqp: a frame for channel " + to_str(f.channel) + ", which is not open");
    }
    if ch.closing {
        // only the broker's close or close-ok ends a channel we closed
        if f.kind == 1 {
            let cr = decode_method(f.payload);
            if let m = cr {
                let k = m.class_id * 1000 + m.method_id;
                if k == 20040 {
                    send_method(c, ch.id, method(20, 41, []));
                }
                if k == 20040 || k == 20041 {
                    forget_channel(ch);
                }
            }
        }
        return ok(true);
    }
    if f.kind == 2 {
        return content_header(ch, f.payload);
    }
    if f.kind == 3 {
        return content_body(ch, f.payload);
    }
    let mr = decode_method(f.payload);
    guard let m = mr else let e = err_of(mr) {
        c.fail_code = 502;
        return err(e);
    }
    if ch.in_kind != 0 {
        c.fail_code = 505;
        return err("amqp: method " + to_str(m.class_id) + "." + to_str(m.method_id) + " in the middle of a message on channel " +
                   to_str(ch.id));
    }
    let k = m.class_id * 1000 + m.method_id;
    if k == 60050 || k == 60060 || k == 60071 {
        ch.in_kind = 1;
        if k == 60060 {
            ch.in_kind = 2;
        } else if k == 60071 {
            ch.in_kind = 3;
        }
        ch.in_method = m;
        ch.in_header = false;
        ch.in_got = 0;
        return ok(true);
    }
    if k == 60080 {
        return apply_confirm(ch, m.args[0].i, m.args[1].i != 0, true);
    }
    if k == 60120 {
        return apply_confirm(ch, m.args[0].i, m.args[1].i != 0, false);
    }
    return channel_method(ch, m);
}

// ---- messages from the broker ------------------------------------------------

fn content_header(ch: Channel, payload: bytes) -> result[bool, str] {
    let c = ch.conn;
    if ch.in_kind == 0 || ch.in_header {
        c.fail_code = 505;
        return err("amqp: a content header on channel " + to_str(ch.id) + " without a message method before it");
    }
    let hr = decode_header(payload);
    guard let h = hr else let e = err_of(hr) {
        c.fail_code = 502;
        return err(e);
    }
    if h.class_id != 60 {
        c.fail_code = 505;
        return err("amqp: a content header of class " + to_str(h.class_id) + " for a basic message");
    }
    if h.body_size > c.opts.max_body {
        c.fail_code = 505;
        return err("amqp: a message of " + to_str(h.body_size) + " bytes, over Options.max_body (" +
                   to_str(c.opts.max_body) + ")");
    }
    ch.in_header = true;
    ch.in_props = h.props;
    ch.in_size = h.body_size;
    ch.in_body = builder.new_bytes();
    ch.in_got = 0;
    if h.body_size == 0 {
        return message_done(ch);
    }
    return ok(true);
}

fn content_body(ch: Channel, payload: bytes) -> result[bool, str] {
    let c = ch.conn;
    if ch.in_kind == 0 || !ch.in_header {
        c.fail_code = 505;
        return err("amqp: a body frame on channel " + to_str(ch.id) + " without a content header before it");
    }
    ch.in_got += len(payload);
    if ch.in_got > ch.in_size {
        c.fail_code = 505;
        return err("amqp: body frames longer than the " + to_str(ch.in_size) + " bytes their header declared");
    }
    ch.in_body.write(payload);
    if ch.in_got == ch.in_size {
        return message_done(ch);
    }
    return ok(true);
}

// A whole message has arrived: hand it to whoever waits for its kind.
fn message_done(ch: Channel) -> result[bool, str] {
    let kind = ch.in_kind;
    let m = ch.in_method;
    let body = ch.in_body.finish();
    ch.in_kind = 0;
    ch.in_header = false;
    if kind == 1 {
        let r = Return { reply_code: m.args[0].i, reply_text: m.args[1].s, exchange: m.args[2].s, routing_key: m.args[3].s,
                         props: ch.in_props, body: body };
        select {
            case chan_send(ch.returns, r) { }
            default {
                ch.returns_dropped += 1;
            }
        }
        return ok(true);
    }
    ch.conn.fail_code = 505;
    return err("amqp: a delivery on channel " + to_str(ch.id) + ", which has no consumer");
}

fn channel_method(ch: Channel, m: Method) -> result[bool, str] {
    let k = m.class_id * 1000 + m.method_id;
    if k == 20040 {
        // the broker closes the channel: answer, then end it with its reason
        send_method(ch.conn, ch.id, method(20, 41, []));
        end_channel(ch, close_reason(m));
        return ok(true);
    }
    if k == 20020 {
        ch.active = m.args[0].i != 0;
        send_method(ch.conn, ch.id, method(20, 21, [arg_int(m.args[0].i)]));
        return ok(true);
    }
    if is_reply(k) {
        select {
            case chan_send(ch.replies, m) { }
            default {
                ch.conn.fail_code = 505;
                return err("amqp: a reply nobody asked for: method " + to_str(m.class_id) + "." + to_str(m.method_id));
            }
        }
        return ok(true);
    }
    ch.conn.fail_code = 505;
    return err("amqp: unexpected method " + to_str(m.class_id) + "." + to_str(m.method_id) + " on channel " +
               to_str(ch.id));
}

// The methods a synchronous call waits for.
fn is_reply(k: int) -> bool {
    return k == 20011 || k == 20041 || k == 40011 || k == 40021 || k == 40031 || k == 40051 || k == 50011 ||
           k == 50021 || k == 50031 || k == 50041 || k == 50051 || k == 60011 || k == 60021 || k == 60031 ||
           k == 60111 || k == 85011;
}

// ---- exchanges -------------------------------------------------------------

// Declares exchange name of kind "direct", "fanout", "topic", "headers"
// (or a plugin's, like "x-delayed-message").
pub fn exchange_declare(ch: Channel, name: str, kind: str, o: ExchangeOptions, deadline: until) -> result[bool, str] {
    let m = method(40, 10, [arg_int(0), arg_str(name), arg_str(kind), arg_bool(o.passive), arg_bool(o.durable),
                            arg_bool(o.auto_delete), arg_bool(o.internal), arg_bool(false), arg_table(o.args)]);
    let r = call(ch, m, 40, 11, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

pub fn exchange_delete(ch: Channel, name: str, if_unused: bool, deadline: until) -> result[bool, str] {
    let m = method(40, 20, [arg_int(0), arg_str(name), arg_bool(if_unused), arg_bool(false)]);
    let r = call(ch, m, 40, 21, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// Routes messages from exchange src to exchange dest (RabbitMQ's
// exchange-to-exchange bindings).
pub fn exchange_bind(ch: Channel, dest: str, src: str, key: str, args: Table, deadline: until) -> result[bool, str] {
    let m = method(40, 30, [arg_int(0), arg_str(dest), arg_str(src), arg_str(key), arg_bool(false), arg_table(args)]);
    let r = call(ch, m, 40, 31, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

pub fn exchange_unbind(ch: Channel, dest: str, src: str, key: str, args: Table, deadline: until) -> result[bool, str] {
    let m = method(40, 40, [arg_int(0), arg_str(dest), arg_str(src), arg_str(key), arg_bool(false), arg_table(args)]);
    let r = call(ch, m, 40, 51, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// ---- queues ----------------------------------------------------------------

// Declares queue name; "" asks the broker to name it (Queue.name has the
// name it chose: an exclusive reply queue for RPC, say).
pub fn queue_declare(ch: Channel, name: str, o: QueueOptions, deadline: until) -> result[Queue, str] {
    let m = method(50, 10, [arg_int(0), arg_str(name), arg_bool(o.passive), arg_bool(o.durable), arg_bool(o.exclusive),
                            arg_bool(o.auto_delete), arg_bool(false), arg_table(o.args)]);
    let r = call(ch, m, 50, 11, deadline);
    guard let ok_m = r else let e = err_of(r) { return err(e); }
    return ok(Queue { name: ok_m.args[0].s, messages: ok_m.args[1].i, consumers: ok_m.args[2].i });
}

// Routes messages from exchange to queue when their routing key matches
// key (a pattern for a topic exchange; args for a headers exchange).
pub fn queue_bind(ch: Channel, queue: str, exchange: str, key: str, args: Table, deadline: until) -> result[bool, str] {
    let m = method(50, 20, [arg_int(0), arg_str(queue), arg_str(exchange), arg_str(key), arg_bool(false), arg_table(args)]);
    let r = call(ch, m, 50, 21, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

pub fn queue_unbind(ch: Channel, queue: str, exchange: str, key: str, args: Table, deadline: until) -> result[bool, str] {
    let m = method(50, 50, [arg_int(0), arg_str(queue), arg_str(exchange), arg_str(key), arg_table(args)]);
    let r = call(ch, m, 50, 51, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// Drops every ready message from the queue; returns how many.
pub fn queue_purge(ch: Channel, queue: str, deadline: until) -> result[int, str] {
    let r = call(ch, method(50, 30, [arg_int(0), arg_str(queue), arg_bool(false)]), 50, 31, deadline);
    guard let m = r else let e = err_of(r) { return err(e); }
    return ok(m.args[0].i);
}

// Deletes the queue; returns how many messages it held. if_unused and
// if_empty make a queue with consumers or messages a 406 instead.
pub fn queue_delete(ch: Channel, queue: str, if_unused: bool, if_empty: bool, deadline: until) -> result[int, str] {
    let m = method(50, 40, [arg_int(0), arg_str(queue), arg_bool(if_unused), arg_bool(if_empty), arg_bool(false)]);
    let r = call(ch, m, 50, 41, deadline);
    guard let ok_m = r else let e = err_of(r) { return err(e); }
    return ok(ok_m.args[0].i);
}

// ---- QoS -------------------------------------------------------------------

// At most prefetch unacknowledged deliveries per consumer on this channel
// (0: no limit). Set it before consuming.
pub fn qos(ch: Channel, prefetch: int, deadline: until) -> result[bool, str] {
    if prefetch < 0 || prefetch > 65535 {
        return err("amqp: prefetch is 0 to 65535");
    }
    let r = call(ch, method(60, 10, [arg_int(0), arg_int(prefetch), arg_bool(false)]), 60, 11, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}
