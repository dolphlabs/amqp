// Publishing, publisher confirms and returned messages.
//
//     let msg = amqp.publishing(to_bytes("{\"id\":1}"));
//     msg.props.content_type = "application/json";
//     msg.props.delivery_mode = 2;                             // persistent
//     amqp.confirm_select(ch, deadline);
//     let pr = amqp.publish(ch, "", "jobs", msg, false);       // result[int, str]: the sequence number
//     guard let seq = pr else let e = err_of(pr) { panic(e); }
//     let cr = amqp.wait_confirm(ch, seq, deadline);           // result[bool, str]: acked, or nacked
//
// A message's frames (basic.publish, the content header, and the body in
// pieces of at most frame-max) are written together. In confirm mode the
// broker acknowledges every message, in publishing order, perhaps several
// at once; wait_confirm reports one, wait_confirms all that are
// outstanding.
//
// A mandatory message the broker cannot route comes back as a Return
// (next_return); without mandatory it is dropped silently. Up to
// RETURN_BUFFER returns wait to be read; past that the newest are dropped
// and counted in returns_dropped.

import "builder";

let RETURN_BUFFER = 256;

pub gc struct Publishing {
    body: bytes,
    props: Properties,
}

// A message returned by the broker: mandatory and unroutable.
pub gc struct Return {
    reply_code: int,
    reply_text: str,
    exchange: str,
    routing_key: str,
    props: Properties,
    body: bytes,
}

pub fn publishing(body: bytes) -> Publishing {
    return Publishing { body: body, props: properties() };
}

// Publishes msg to exchange ("" is the default exchange, which routes by
// queue name) with routing key key. Returns the message's sequence
// number in confirm mode, 0 otherwise. Nothing waits for the broker: a
// publish only fails when the channel or connection is closed, or the
// message cannot be encoded.
pub fn publish(ch: Channel, exchange: str, key: str, msg: Publishing, mandatory: bool) -> result[int, str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    if !ch.active {
        return err("amqp: the broker has paused this channel (channel.flow)");
    }
    let c = ch.conn;
    let mr = encode_method(method(60, 40, [arg_int(0), arg_str(exchange), arg_str(key), arg_bool(mandatory),
                                           arg_bool(false)]));
    guard let mp = mr else let e = err_of(mr) { return err(e); }
    let hr = encode_header(60, len(msg.body), msg.props);
    guard let hp = hr else let e = err_of(hr) { return err(e); }
    let w = builder.new_bytes();
    w.write(encode_frame(1, ch.id, mp));
    w.write(encode_frame(2, ch.id, hp));
    let piece = 131072;
    if c.frame_max > 0 {
        piece = c.frame_max - 8;
    }
    let off = 0;
    let n = len(msg.body);
    while off < n {
        let end = off + piece;
        if end > n {
            end = n;
        }
        w.write(encode_frame(3, ch.id, msg.body[off..end]));
        off = end;
    }
    let frames = w.finish();
    // the sequence number and the write go together: the broker counts
    // messages in the order it reads them
    mutex_lock(ch.plock);
    let seq = 0;
    if ch.confirming {
        seq = ch.next_seq;
        ch.next_seq += 1;
        mutex_lock(ch.clock2);
        ch.unconfirmed[seq] = true;
        mutex_unlock(ch.clock2);
    }
    let sr = send_frames(c, frames);
    mutex_unlock(ch.plock);
    guard let _s = sr else let e = err_of(sr) { return err(e); }
    return ok(seq);
}

// Puts the channel in confirm mode: the broker acknowledges every
// message published on it from now on.
pub fn confirm_select(ch: Channel, deadline: until) -> result[bool, str] {
    if ch.confirming {
        return ok(true);
    }
    let r = call(ch, method(85, 10, [arg_bool(false)]), 85, 11, deadline);
    guard let _m = r else let e = err_of(r) { return err(e); }
    ch.confirming = true;
    return ok(true);
}

// Whether the broker took message seq (true) or refused it (false: a
// nack, when it could not store the message). Waits until deadline.
pub fn wait_confirm(ch: Channel, seq: int, deadline: until) -> result[bool, str] {
    if !ch.confirming {
        return err("amqp: the channel is not in confirm mode (confirm_select)");
    }
    if seq < 1 || seq >= ch.next_seq {
        return err("amqp: no message was published with sequence number " + to_str(seq));
    }
    while true {
        // checked and, if still pending, registered for a wake in one
        // hold of the lock: a confirm applied in between is not missed
        let wake: chan[bool] = make_chan(1);
        mutex_lock(ch.clock2);
        let pending = has(ch.unconfirmed, seq);
        let nacked = has(ch.nacked, seq);
        if !pending && nacked {
            del(ch.nacked, seq);
        }
        if pending {
            push(ch.confirm_waiters, wake);
        }
        mutex_unlock(ch.clock2);
        if !pending {
            return ok(!nacked);
        }
        if ch.state == CH_CLOSED {
            return err(ch.why);
        }
        let wr = wait_confirm_event(ch, wake, deadline);
        guard let _w = wr else let e = err_of(wr) { return err(e); }
    }
    return err("amqp: unreachable");
}

// Waits until every message published so far is confirmed; true if none
// was nacked (since the last wait_confirms).
pub fn wait_confirms(ch: Channel, deadline: until) -> result[bool, str] {
    if !ch.confirming {
        return err("amqp: the channel is not in confirm mode (confirm_select)");
    }
    while true {
        let wake: chan[bool] = make_chan(1);
        mutex_lock(ch.clock2);
        let left = len(ch.unconfirmed);
        let any_nack = ch.nack_seen;
        if left == 0 {
            ch.nack_seen = false;
            ch.nacked = {};
        } else {
            push(ch.confirm_waiters, wake);
        }
        mutex_unlock(ch.clock2);
        if left == 0 {
            return ok(!any_nack);
        }
        if ch.state == CH_CLOSED {
            return err(ch.why);
        }
        let wr = wait_confirm_event(ch, wake, deadline);
        guard let _w = wr else let e = err_of(wr) { return err(e); }
    }
    return err("amqp: unreachable");
}

// Messages published in confirm mode and not yet confirmed.
pub fn unconfirmed(ch: Channel) -> int {
    mutex_lock(ch.clock2);
    let n = len(ch.unconfirmed);
    mutex_unlock(ch.clock2);
    return n;
}

// Parks on wake (registered by the caller) until the reader applies a
// confirm, the channel closes, or the deadline passes (an error).
fn wait_confirm_event(ch: Channel, wake: chan[bool], deadline: until) -> result[bool, str] {
    let w = add_waiter(ch.conn, deadline);
    let timed_out = false;
    select {
        case let _v = chan_recv(wake) { }
        case let _t = chan_recv(w.wake) {
            timed_out = true;
        }
    }
    w.done = true;
    if timed_out && ch.state != CH_CLOSED {
        return err("amqp: timed out waiting for the broker's confirm");
    }
    return ok(true);
}

// basic.ack or basic.nack from the broker, in confirm mode.
fn apply_confirm(ch: Channel, tag: int, multiple: bool, ack: bool) -> result[bool, str] {
    if !ch.confirming {
        ch.conn.fail_code = 505;
        return err("amqp: a publisher confirm on channel " + to_str(ch.id) + ", which is not in confirm mode");
    }
    mutex_lock(ch.clock2);
    let done: [int] = [];
    if multiple {
        for seq, _v in ch.unconfirmed {
            if seq <= tag {
                push(done, seq);
            }
        }
    } else if has(ch.unconfirmed, tag) {
        push(done, tag);
    }
    for seq in done {
        del(ch.unconfirmed, seq);
        if !ack {
            ch.nacked[seq] = true;
            ch.nack_seen = true;
        }
    }
    let ws = ch.confirm_waiters;
    ch.confirm_waiters = [];
    mutex_unlock(ch.clock2);
    for w in ws {
        select {
            case chan_send(w, true) { }
            default { }
        }
    }
    return ok(true);
}

// Wakes everything waiting on the channel's confirms (it closed).
fn wake_confirm_waiters(ch: Channel) {
    mutex_lock(ch.clock2);
    let ws = ch.confirm_waiters;
    ch.confirm_waiters = [];
    mutex_unlock(ch.clock2);
    for w in ws {
        select {
            case chan_send(w, true) { }
            default { }
        }
    }
}

// The next returned message, waiting until deadline: none when the
// deadline passes or the channel closes.
pub fn next_return(ch: Channel, deadline: until) -> result[opt[Return], str] {
    select {
        case let r = chan_recv(ch.returns) {
            return ok(r);
        }
        default { }
    }
    if ch.state == CH_CLOSED {
        return ok(none);
    }
    let w = add_waiter(ch.conn, deadline);
    let out: opt[Return] = none;
    select {
        case let r = chan_recv(ch.returns) {
            out = r;
        }
        case let _t = chan_recv(w.wake) { }
    }
    w.done = true;
    return ok(out);
}

// Returns dropped because next_return fell RETURN_BUFFER behind.
pub fn returns_dropped(ch: Channel) -> int {
    return ch.returns_dropped;
}
