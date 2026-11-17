// Consuming: deliveries pushed by the broker to a consumer, pulled with
// next; acknowledgements; polling with get.
//
//     amqp.qos(ch, 32, deadline);                      // at most 32 unacked at a time
//     let sr = amqp.consume(ch, "jobs", amqp.consume_options(), deadline);
//     guard let sub = sr else let e = err_of(sr) { panic(e); }
//     while true {
//         let nr = amqp.next(sub, deadline);           // result[opt[Delivery], str]
//         guard let got = nr else let e = err_of(nr) { break; }   // the consumer ended: why
//         guard let d = got else { continue; }         // none: the deadline passed
//         handle(d.body);
//         amqp.ack(ch, d.delivery_tag, false);
//     }
//
// Deliveries wait in the consumer's buffer until next takes them. With
// QoS set (qos before consume) the buffer holds the prefetch window,
// which the broker never exceeds; without it the buffer is
// ConsumeOptions.buffer deliveries (1024), and a consumer that falls
// further behind closes its channel with an error rather than stall every
// other channel on the connection.

let DEFAULT_BUFFER = 1024;

pub gc struct Delivery {
    consumer_tag: str,
    // what ack, nack and reject take
    delivery_tag: int,
    // the broker delivered this message before (to this consumer or another)
    redelivered: bool,
    exchange: str,
    routing_key: str,
    // basic.get only: messages left in the queue
    message_count: int,
    props: Properties,
    body: bytes,
}

pub gc struct ConsumeOptions {
    // "" lets the broker choose one (Consumer.tag)
    tag: str,
    // the broker forgets each message once sent: no ack, and a crash
    // loses what was in flight
    no_ack: bool,
    exclusive: bool,
    // x-priority, x-stream-offset, ...
    args: Table,
    // deliveries buffered when QoS is not set
    buffer: int,
}

pub gc struct Consumer {
    ch: Channel,
    tag: str,
    deliveries: chan[Delivery],
    closed: bool,
    why: str,
}

pub fn consume_options() -> ConsumeOptions {
    return ConsumeOptions { tag: "", no_ack: false, exclusive: false, args: table(), buffer: DEFAULT_BUFFER };
}

// Starts consuming from queue.
pub fn consume(ch: Channel, queue: str, o: ConsumeOptions, deadline: until) -> result[Consumer, str] {
    let cap = o.buffer;
    if ch.prefetch > 0 && !o.no_ack {
        cap = ch.prefetch;
    }
    if cap < 1 {
        return err("amqp: ConsumeOptions.buffer must be at least 1");
    }
    let sub = Consumer { ch: ch, tag: o.tag, deliveries: make_chan(cap), closed: false, why: "" };
    // registered before the request (under the tag asked for, or as the
    // one consume-ok will name), so a delivery right behind consume-ok
    // finds it; the call lock keeps two consumes from crossing
    mutex_lock(ch.rpc);
    mutex_lock(ch.clock2);
    if o.tag != "" {
        if has(ch.consumers, o.tag) {
            mutex_unlock(ch.clock2);
            mutex_unlock(ch.rpc);
            return err("amqp: consumer tag " + o.tag + " is already in use on this channel");
        }
        ch.consumers[o.tag] = sub;
    } else {
        ch.awaiting_tag = some(sub);
    }
    mutex_unlock(ch.clock2);
    let m = method(60, 20, [arg_int(0), arg_str(queue), arg_str(o.tag), arg_bool(false), arg_bool(o.no_ack),
                            arg_bool(o.exclusive), arg_bool(false), arg_table(o.args)]);
    let r: result[Method, str] = err(ch.why);
    if ch.state != CH_CLOSED {
        r = call_locked(ch, m, 60, 21, deadline);
    }
    mutex_unlock(ch.rpc);
    guard let okm = r else let e = err_of(r) {
        mutex_lock(ch.clock2);
        if o.tag != "" {
            del(ch.consumers, o.tag);
        }
        ch.awaiting_tag = none;
        mutex_unlock(ch.clock2);
        return err(e);
    }
    sub.tag = okm.args[0].s;
    return ok(sub);
}

// The next delivery, waiting until deadline: none when the deadline
// passes. Once the consumer has ended -- cancelled, or its channel or
// connection closed -- the deliveries already buffered come first, then
// the reason, as an error.
pub fn next(sub: Consumer, deadline: until) -> result[opt[Delivery], str] {
    select {
        case let d = chan_recv(sub.deliveries) {
            guard let got = d else { return err(sub.why); }
            return ok(some(got));
        }
        default { }
    }
    if sub.closed {
        return err(sub.why);
    }
    let w = add_waiter(sub.ch.conn, deadline);
    let out: opt[Delivery] = none;
    let ended = false;
    select {
        case let d = chan_recv(sub.deliveries) {
            if let got = d {
                out = some(got);
            } else {
                ended = true;
            }
        }
        case let _t = chan_recv(w.wake) { }
    }
    w.done = true;
    if ended {
        return err(sub.why);
    }
    return ok(out);
}

// Stops the consumer: the broker sends no more, and next returns what is
// buffered, then "amqp: the consumer was cancelled".
pub fn cancel(sub: Consumer, deadline: until) -> result[bool, str] {
    if sub.closed {
        return ok(true);
    }
    let r = call(sub.ch, method(60, 30, [arg_str(sub.tag), arg_bool(false)]), 60, 31, deadline);
    end_consumer(sub.ch, sub.tag, "amqp: the consumer was cancelled");
    guard let _m = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// Acknowledges delivery tag (and, with multiple, every earlier one on
// the channel): the broker forgets the message.
pub fn ack(ch: Channel, tag: int, multiple: bool) -> result[bool, str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    return send_method(ch.conn, ch.id, method(60, 80, [arg_int(tag), arg_bool(multiple)]));
}

// Refuses delivery tag (and, with multiple, every earlier one): requeue
// puts it back in the queue, otherwise it is dropped or dead-lettered.
pub fn nack(ch: Channel, tag: int, multiple: bool, requeue: bool) -> result[bool, str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    return send_method(ch.conn, ch.id, method(60, 120, [arg_int(tag), arg_bool(multiple), arg_bool(requeue)]));
}

// nack of one message, in the original 0-9-1 form.
pub fn reject(ch: Channel, tag: int, requeue: bool) -> result[bool, str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    return send_method(ch.conn, ch.id, method(60, 90, [arg_int(tag), arg_bool(requeue)]));
}

// Takes one message from queue, if it has one (polling; consume is the
// efficient way to read a queue continuously).
pub fn get(ch: Channel, queue: str, no_ack: bool, deadline: until) -> result[opt[Delivery], str] {
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    mutex_lock(ch.rpc);
    let r = get_locked(ch, queue, no_ack, deadline);
    mutex_unlock(ch.rpc);
    return r;
}

fn get_locked(ch: Channel, queue: str, no_ack: bool, deadline: until) -> result[opt[Delivery], str] {
    let sr = send_method(ch.conn, ch.id, method(60, 70, [arg_int(0), arg_str(queue), arg_bool(no_ack)]));
    guard let _s = sr else let e = err_of(sr) { return err(e); }
    let w = add_waiter(ch.conn, deadline);
    let out: opt[Delivery] = none;
    let state = 0;
    select {
        case let d = chan_recv(ch.gets) {
            if let got = d {
                out = some(got);
                state = 1;
            }
        }
        case let m = chan_recv(ch.replies) {
            if let _empty = m {
                state = 2;
            }
        }
        case let _t = chan_recv(w.wake) {
            state = 3;
        }
    }
    w.done = true;
    if state == 1 || state == 2 {
        return ok(out);
    }
    if ch.state == CH_CLOSED {
        return err(ch.why);
    }
    let why = "amqp: timed out waiting for the broker's reply to basic.get";
    ch.closing = true;
    end_channel(ch, why);
    send_method(ch.conn, ch.id, method(20, 40, [arg_int(200), arg_str("timed out"), arg_int(0), arg_int(0)]));
    return err(why);
}

// ---- from the reader -------------------------------------------------------

// A whole basic.deliver: to its consumer. One with no consumer (it raced
// a cancel) goes back to the queue.
fn deliver(ch: Channel, d: Delivery) -> result[bool, str] {
    mutex_lock(ch.clock2);
    let found: opt[Consumer] = none;
    if has(ch.consumers, d.consumer_tag) {
        found = some(ch.consumers[d.consumer_tag]);
    } else if let pending = ch.awaiting_tag {
        // consume-ok is still on its way; the delivery behind it is for
        // the consumer that asked
        pending.tag = d.consumer_tag;
        ch.consumers[d.consumer_tag] = pending;
        ch.awaiting_tag = none;
        found = some(pending);
    }
    mutex_unlock(ch.clock2);
    guard let sub = found else {
        send_method(ch.conn, ch.id, method(60, 90, [arg_int(d.delivery_tag), arg_bool(true)]));
        return ok(true);
    }
    if sub.closed {
        send_method(ch.conn, ch.id, method(60, 90, [arg_int(d.delivery_tag), arg_bool(true)]));
        return ok(true);
    }
    select {
        case chan_send(sub.deliveries, d) { }
        default {
            // next has fallen behind the buffer: stop this channel rather
            // than stall every channel behind the reader
            let why = "amqp: consumer " + sub.tag + " fell more than its buffer of deliveries behind; set qos " +
                      "(prefetch) before consuming";
            ch.closing = true;
            send_method(ch.conn, ch.id, method(20, 40, [arg_int(406), arg_str(trim_short(why)), arg_int(0), arg_int(0)]));
            end_channel(ch, why);
        }
    }
    return ok(true);
}

// consume-ok: file a broker-chosen tag before the deliveries behind it.
fn consume_ok(ch: Channel, tag: str) {
    mutex_lock(ch.clock2);
    if let pending = ch.awaiting_tag {
        pending.tag = tag;
        ch.consumers[tag] = pending;
        ch.awaiting_tag = none;
    }
    mutex_unlock(ch.clock2);
}

// The consumer is over: no more deliveries; what is buffered stays.
fn end_consumer(ch: Channel, tag: str, why: str) {
    mutex_lock(ch.clock2);
    let found: opt[Consumer] = none;
    if has(ch.consumers, tag) {
        found = some(ch.consumers[tag]);
        del(ch.consumers, tag);
    }
    mutex_unlock(ch.clock2);
    if let sub = found {
        finish_consumer(sub, why);
    }
}

fn finish_consumer(sub: Consumer, why: str) {
    if sub.closed {
        return;
    }
    sub.closed = true;
    sub.why = why;
    chan_close(sub.deliveries);
}

// The channel has ended: so has every consumer on it, with its reason.
fn end_consumers(ch: Channel) {
    mutex_lock(ch.clock2);
    let subs: [Consumer] = [];
    for _t, sub in ch.consumers {
        push(subs, sub);
    }
    ch.consumers = {};
    let pending = ch.awaiting_tag;
    ch.awaiting_tag = none;
    mutex_unlock(ch.clock2);
    for sub in subs {
        finish_consumer(sub, ch.why);
    }
    if let p = pending {
        finish_consumer(p, ch.why);
    }
}
