// The client against a real RabbitMQ: tests/live/run.sh starts one in
// Docker (TLS on 5671 with a throwaway CA) and runs this with
//
//     AMQP_URL=amqp://app:s3cret@127.0.0.1:5672/
//     AMQPS_URL=amqps://app:s3cret@localhost:5671/
//     AMQP_CA=/path/to/ca.crt
//     AMQP_CLOSE_CMD (a command that closes every client connection)

import "amqp";
import "io";
import "os";
import "proc";
import "strings";
import "time";

fn soon(s: int) -> until {
    return until_of(time.mono() + s * 1000000000);
}

fn env(name: str) -> str {
    let v = proc.getenv(name) ?? "";
    if v == "" {
        panic(name + " is not set");
    }
    return v;
}

fn must[T](what: str, r: result[T, str]) -> T {
    guard let v = r else let e = err_of(r) { panic(what + ": " + e); }
    return v;
}

fn outcome[T](r: result[T, str]) -> str {
    guard let _v = r else let e = err_of(r) { return e; }
    return "ok";
}

fn dial(url: str) -> amqp.Conn {
    return must("dial", amqp.dial(url, amqp.options(), soon(10)));
}

fn chan_of(c: amqp.Conn) -> amqp.Channel {
    return must("channel", amqp.channel(c, soon(5)));
}

fn fresh_queue(ch: amqp.Channel, name: str, o: amqp.QueueOptions) {
    amqp.queue_delete(ch, name, false, false, soon(5));
    must("declare " + name, amqp.queue_declare(ch, name, o, soon(5)));
}

fn text_msg(body: str) -> amqp.Publishing {
    let m = amqp.publishing(to_bytes(body));
    m.props.content_type = "text/plain";
    return m;
}

fn take(sub: amqp.Consumer) -> amqp.Delivery {
    let nr = amqp.next(sub, soon(10));
    guard let got = nr else let e = err_of(nr) { panic("next: " + e); }
    guard let d = got else { panic("next: nothing within 10 s"); }
    return d;
}

fn scenario_connect(url: str) {
    println("-- connect");
    let c = dial(url);
    let product = amqp.as_str(amqp.table_get(amqp.server_properties(c), "product") ?? amqp.void_value()) ?? "?";
    println("server " + product + ", frame_max " + to_str(amqp.frame_max(c)) + ", channel_max " +
            to_str(amqp.channel_max(c)) + ", heartbeat " + to_str(amqp.heartbeat(c)));
    must("close", amqp.close(c, soon(5)));
    println("closed: " + to_str(amqp.is_closed(c)));
}

fn scenario_work_queue(url: str) {
    println("-- work queue: 1000 persistent messages, confirmed, consumed in order");
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "live.jobs", amqp.durable());
    must("confirm_select", amqp.confirm_select(ch, soon(5)));
    for i in 0..1000 {
        let m = text_msg("job " + to_str(i));
        m.props.delivery_mode = 2;
        must("publish", amqp.publish(ch, "", "live.jobs", m, false));
    }
    println("all confirmed and acked: " + to_str(must("wait_confirms", amqp.wait_confirms(ch, soon(30)))));
    let q = must("passive", amqp.queue_declare(ch, "live.jobs", passive(), soon(5)));
    println("queue holds " + to_str(q.messages));
    let cch = chan_of(c);
    must("qos", amqp.qos(cch, 50, soon(5)));
    let sub = must("consume", amqp.consume(cch, "live.jobs", amqp.consume_options(), soon(5)));
    let in_order = true;
    for i in 0..1000 {
        let d = take(sub);
        if to_str(d.body) != "job " + to_str(i) {
            in_order = false;
        }
        must("ack", amqp.ack(cch, d.delivery_tag, false));
    }
    println("consumed 1000, in order: " + to_str(in_order));
    must("cancel", amqp.cancel(sub, soon(5)));
    let after = must("passive", amqp.queue_declare(ch, "live.jobs", passive(), soon(5)));
    println("queue holds " + to_str(after.messages) + " after the acks");
    amqp.queue_delete(ch, "live.jobs", false, false, soon(5));
    amqp.close(c, soon(5));
}

fn passive() -> amqp.QueueOptions {
    let o = amqp.queue_options();
    o.passive = true;
    return o;
}

fn scenario_topic(url: str) {
    println("-- topic exchange");
    let c = dial(url);
    let ch = chan_of(c);
    must("exchange", amqp.exchange_declare(ch, "live.events", "topic", amqp.exchange_options(), soon(5)));
    fresh_queue(ch, "live.orders", amqp.queue_options());
    fresh_queue(ch, "live.all", amqp.queue_options());
    must("bind", amqp.queue_bind(ch, "live.orders", "live.events", "order.*", amqp.table(), soon(5)));
    must("bind", amqp.queue_bind(ch, "live.all", "live.events", "#", amqp.table(), soon(5)));
    let keys = ["order.created", "order.paid", "user.signup", "order.paid.late"];
    for k in keys {
        must("publish", amqp.publish(ch, "live.events", k, text_msg(k), false));
    }
    time.sleep(300000000);
    println("order.* got " + to_str(must("q", amqp.queue_declare(ch, "live.orders", passive(), soon(5))).messages) +
            ", # got " + to_str(must("q", amqp.queue_declare(ch, "live.all", passive(), soon(5))).messages));
    let d = must("get", amqp.get(ch, "live.orders", true, soon(5)));
    if let first = d {
        println("first in live.orders: " + first.routing_key + " = " + to_str(first.body) + " (" +
                to_str(first.message_count) + " more)");
    }
    amqp.queue_delete(ch, "live.orders", false, false, soon(5));
    amqp.queue_delete(ch, "live.all", false, false, soon(5));
    amqp.exchange_delete(ch, "live.events", false, soon(5));
    amqp.close(c, soon(5));
}

fn responder(url: str, ready: chan[bool]) {
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "live.rpc", amqp.queue_options());
    let sub = must("consume", amqp.consume(ch, "live.rpc", amqp.consume_options(), soon(5)));
    chan_send(ready, true);
    for i in 0..3 {
        let d = take(sub);
        let reply = text_msg(strings.to_upper(to_str(d.body)));
        reply.props.correlation_id = d.props.correlation_id;
        must("reply", amqp.publish(ch, "", d.props.reply_to, reply, false));
        must("ack", amqp.ack(ch, d.delivery_tag, false));
    }
    amqp.close(c, soon(5));
}

fn scenario_rpc(url: str) {
    println("-- RPC with a server-named reply queue");
    let ready: chan[bool] = make_chan(1);
    spawn responder(url, ready);
    chan_recv(ready);
    let c = dial(url);
    let ch = chan_of(c);
    let o = amqp.queue_options();
    o.exclusive = true;
    o.auto_delete = true;
    let rq = must("reply queue", amqp.queue_declare(ch, "", o, soon(5)));
    println("reply queue named by the broker: " + to_str(strings.has_prefix(rq.name, "amq.gen-")));
    let opts = amqp.consume_options();
    opts.no_ack = true;
    let replies = must("consume", amqp.consume(ch, rq.name, opts, soon(5)));
    let asks = ["ping", "hello", "slang"];
    for i in 0..3 {
        let m = text_msg(asks[i]);
        m.props.reply_to = rq.name;
        m.props.correlation_id = "req-" + to_str(i);
        must("publish", amqp.publish(ch, "", "live.rpc", m, false));
        let d = take(replies);
        println(asks[i] + " -> " + to_str(d.body) + " (" + d.props.correlation_id + ")");
    }
    amqp.queue_delete(ch, "live.rpc", false, false, soon(5));
    amqp.close(c, soon(5));
}

fn scenario_returns_and_errors(url: str) {
    println("-- returns, nacks and broker errors");
    let c = dial(url);
    let ch = chan_of(c);
    must("exchange", amqp.exchange_declare(ch, "live.nowhere", "direct", amqp.exchange_options(), soon(5)));
    must("confirm_select", amqp.confirm_select(ch, soon(5)));
    let seq = must("publish", amqp.publish(ch, "live.nowhere", "no.route", text_msg("lost"), true));
    let rr = must("next_return", amqp.next_return(ch, soon(5)));
    if let r = rr {
        println("returned " + to_str(r.reply_code) + " " + r.reply_text + " body=" + to_str(r.body));
    }
    println("and acked: " + to_str(must("confirm", amqp.wait_confirm(ch, seq, soon(5)))));

    let lo = amqp.queue_options();
    amqp.table_set(lo.args, "x-max-length", amqp.i32_value(1));
    amqp.table_set(lo.args, "x-overflow", amqp.str_value("reject-publish"));
    fresh_queue(ch, "live.tiny", lo);
    let a = must("publish", amqp.publish(ch, "", "live.tiny", text_msg("fits"), false));
    let b = must("publish", amqp.publish(ch, "", "live.tiny", text_msg("overflows"), false));
    println("x-max-length 1: first acked " + to_str(must("c", amqp.wait_confirm(ch, a, soon(5)))) + ", second acked " +
            to_str(must("c", amqp.wait_confirm(ch, b, soon(5)))));

    let ech = chan_of(c);
    println("passive declare of a missing queue: " + outcome(amqp.queue_declare(ech, "live.missing", passive(), soon(5))));
    println("a call after it: " + outcome(amqp.qos(ech, 1, soon(5))));
    let ech2 = chan_of(c);
    println("redeclare live.tiny as durable: " + outcome(amqp.queue_declare(ech2, "live.tiny", amqp.durable(), soon(5))));
    println("the connection is open: " + to_str(!amqp.is_closed(c)));
    amqp.queue_delete(ch, "live.tiny", false, false, soon(5));
    amqp.exchange_delete(ch, "live.nowhere", false, soon(5));
    amqp.close(c, soon(5));
}

fn every_type() -> amqp.Table {
    let inner = amqp.table();
    amqp.table_set(inner, "nested", amqp.str_value("yes"));
    let t = amqp.table();
    let b = amqp.int_value(0);
    b.code = 98;
    b.i = -100;
    amqp.table_set(t, "i8", b);
    let ub = amqp.int_value(0);
    ub.code = 66;
    ub.i = 250;
    amqp.table_set(t, "u8", ub);
    let sh = amqp.int_value(0);
    sh.code = 115;
    sh.i = -30000;
    amqp.table_set(t, "i16", sh);
    let us = amqp.int_value(0);
    us.code = 117;
    us.i = 60000;
    amqp.table_set(t, "u16", us);
    amqp.table_set(t, "i32", amqp.i32_value(-2000000000));
    let ui = amqp.int_value(0);
    ui.code = 105;
    ui.i = 4000000000;
    amqp.table_set(t, "u32", ui);
    amqp.table_set(t, "i64", amqp.int_value(-9000000000000000000));
    let f = amqp.float_value(1.5);
    f.code = 102;
    amqp.table_set(t, "f32", f);
    amqp.table_set(t, "f64", amqp.float_value(-0.1));
    amqp.table_set(t, "decimal", amqp.decimal_value(2, 12345));
    amqp.table_set(t, "string", amqp.str_value("é☃😀"));
    amqp.table_set(t, "bytes", amqp.bytes_value(strings.bytes_zero(3)));
    amqp.table_set(t, "bool", amqp.bool_value(true));
    amqp.table_set(t, "timestamp", amqp.timestamp_value(1700000000));
    amqp.table_set(t, "void", amqp.void_value());
    amqp.table_set(t, "table", amqp.table_value(inner));
    amqp.table_set(t, "array", amqp.array_value([amqp.i32_value(1), amqp.str_value("two")]));
    return t;
}

fn render(v: amqp.Value) -> str {
    let one = strings.bytes_zero(1);
    one[0] = v.code;
    let code = to_str(one);
    if v.code == 102 || v.code == 100 {
        return code + to_str(v.f);
    }
    if v.code == 68 {
        return code + to_str(v.scale) + "/" + to_str(v.i);
    }
    if v.code == 83 || v.code == 120 {
        return code + to_str(len(v.b));
    }
    if v.code == 70 {
        let t = amqp.as_table(v) ?? amqp.table();
        return code + "{" + strings.join(t.keys, ",") + "}";
    }
    if v.code == 65 {
        return code + "[" + to_str(len(amqp.as_array(v) ?? [])) + "]";
    }
    if v.code == 86 {
        return code;
    }
    return code + to_str(v.i);
}

fn scenario_headers_and_size(url: str) {
    println("-- every header type and a 5 MB body through RabbitMQ");
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "live.headers", amqp.queue_options());
    let m = text_msg("typed");
    m.props.headers = some(every_type());
    m.props.priority = 3;
    m.props.timestamp = 1700000000;
    m.props.message_id = "m-1";
    must("publish", amqp.publish(ch, "", "live.headers", m, false));
    let big = amqp.publishing(strings.bytes_zero(5 * 1024 * 1024));
    for i in 0..len(big.body) {
        big.body[i] = i % 251;
    }
    must("publish", amqp.publish(ch, "", "live.headers", big, false));
    let sub = must("consume", amqp.consume(ch, "live.headers", amqp.consume_options(), soon(5)));
    let d = take(sub);
    let h = d.props.headers ?? amqp.table();
    let sent = every_type();
    let same = len(h.keys) == len(sent.keys);
    for i in 0..len(sent.keys) {
        let got = amqp.table_get(h, sent.keys[i]) ?? amqp.str_value("missing");
        let line = sent.keys[i] + ": sent " + render(sent.vals[i]) + ", got " + render(got);
        if render(got) != render(sent.vals[i]) {
            same = false;
            println(line + "  <- differs");
        }
    }
    println("headers came back the same: " + to_str(same) + " (" + to_str(len(h.keys)) + " keys); priority " +
            to_str(d.props.priority) + ", timestamp " + to_str(d.props.timestamp) + ", id " + d.props.message_id);
    amqp.ack(ch, d.delivery_tag, false);
    let d2 = take(sub);
    let intact = len(d2.body) == 5 * 1024 * 1024;
    for i in 0..len(d2.body) {
        if d2.body[i] != i % 251 {
            intact = false;
            break;
        }
    }
    println("5 MB body intact: " + to_str(intact) + " (frame_max " + to_str(amqp.frame_max(c)) + ")");
    amqp.ack(ch, d2.delivery_tag, false);
    amqp.queue_delete(ch, "live.headers", false, false, soon(5));
    amqp.close(c, soon(5));
}

fn scenario_broker_cancel(url: str) {
    println("-- the queue deleted under a consumer");
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "live.doomed", amqp.queue_options());
    must("publish", amqp.publish(ch, "", "live.doomed", text_msg("still here"), false));
    let cch = chan_of(c);
    let sub = must("consume", amqp.consume(cch, "live.doomed", amqp.consume_options(), soon(5)));
    let d = take(sub);
    println("got " + to_str(d.body));
    must("delete", amqp.queue_delete(ch, "live.doomed", false, false, soon(5)));
    let nr = amqp.next(sub, soon(5));
    let why = outcome(nr);
    println("next: the broker cancelled the consumer: " + to_str(strings.has_prefix(why, "amqp: the broker cancelled consumer amq.ctag-")));
    amqp.close(c, soon(5));
}

fn scenario_forced_close(url: str, cmd: str) {
    println("-- the broker closes the connection");
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "live.forced", amqp.queue_options());
    let sub = must("consume", amqp.consume(ch, "live.forced", amqp.consume_options(), soon(5)));
    let env = os.environ();
    let pr = proc.spawn(["/bin/sh", "-c", cmd], env, "", proc.SPAWN_NULL_STDIN);
    guard let p = pr else let e = err_of(pr) { panic(e); }
    proc.wait(p, soon(30));
    let nr = amqp.next(sub, soon(10));
    println("next: " + outcome(nr));
    println("closed: " + to_str(amqp.is_closed(c)) + ", why: " + strings.slice(amqp.close_error(c), 0, 30));
    let c2 = dial(url);
    let ch2 = chan_of(c2);
    amqp.queue_delete(ch2, "live.forced", false, false, soon(5));
    amqp.close(c2, soon(5));
}

fn scenario_tls(url: str, ca: str) {
    println("-- TLS");
    let o = amqp.options();
    o.ca_path = ca;
    let c = must("dial amqps", amqp.dial(url, o, soon(10)));
    let ch = chan_of(c);
    fresh_queue(ch, "live.tls", amqp.queue_options());
    must("publish", amqp.publish(ch, "", "live.tls", text_msg("over TLS"), false));
    time.sleep(200000000);
    let g = must("get", amqp.get(ch, "live.tls", true, soon(5)));
    if let d = g {
        println("got " + to_str(d.body) + " over amqps");
    }
    amqp.queue_delete(ch, "live.tls", false, false, soon(5));
    amqp.close(c, soon(5));
    let bad = amqp.options();
    let r = amqp.dial(url, bad, soon(10));
    println("without the CA: " + strings.slice(outcome(r), 0, 16) + "...");
}

fn run_peer(peer: str, args: [str]) {
    io.flush();
    let argv = [peer];
    for a in args {
        push(argv, a);
    }
    let pr = proc.spawn(argv, os.environ(), "", proc.SPAWN_NULL_STDIN);
    guard let p = pr else let e = err_of(pr) { panic(e); }
    let wr = proc.wait(p, soon(30));
    guard let code = wr else let e = err_of(wr) { panic(e); }
    if code != 0 {
        panic("the Go peer exited with " + to_str(code));
    }
}

fn scenario_interop(url: str, peer: str) {
    println("-- with Go's amqp091-go");
    let c = dial(url);
    let ch = chan_of(c);
    fresh_queue(ch, "interop.from_slang", amqp.queue_options());
    fresh_queue(ch, "interop.from_go", amqp.queue_options());
    must("confirm_select", amqp.confirm_select(ch, soon(5)));
    for i in 0..2 {
        let m = amqp.publishing(to_bytes("{\"slang\":" + to_str(i) + "}"));
        m.props.content_type = "application/json";
        m.props.correlation_id = "slang-" + to_str(i);
        m.props.delivery_mode = 2;
        let h = amqp.table();
        amqp.table_set(h, "from", amqp.str_value("slang"));
        amqp.table_set(h, "n", amqp.i32_value(i));
        amqp.table_set(h, "big", amqp.int_value(1099511627776));
        amqp.table_set(h, "ok", amqp.bool_value(true));
        amqp.table_set(h, "pi", amqp.float_value(3.25));
        amqp.table_set(h, "list", amqp.array_value([amqp.str_value("a"), amqp.i32_value(2)]));
        m.props.headers = some(h);
        must("publish", amqp.publish(ch, "", "interop.from_slang", m, false));
    }
    must("confirms", amqp.wait_confirms(ch, soon(10)));
    run_peer(peer, ["consume", url, "interop.from_slang", "2"]);
    run_peer(peer, ["publish", url, "interop.from_go", "2"]);
    let sub = must("consume", amqp.consume(ch, "interop.from_go", amqp.consume_options(), soon(5)));
    for i in 0..2 {
        let d = take(sub);
        let h = d.props.headers ?? amqp.table();
        let parts: [str] = [];
        let keys = ["big", "from", "list", "n", "ok", "pi"];
        for k in keys {
            push(parts, k + "=" + render(amqp.table_get(h, k) ?? amqp.str_value("missing")));
        }
        println("slang got " + to_str(d.body) + " type=" + d.props.content_type + " corr=" + d.props.correlation_id +
                " mode=" + to_str(d.props.delivery_mode) + " prio=" + to_str(d.props.priority) + " headers=" +
                strings.join(parts, ";"));
        amqp.ack(ch, d.delivery_tag, false);
    }
    amqp.queue_delete(ch, "interop.from_slang", false, false, soon(5));
    amqp.queue_delete(ch, "interop.from_go", false, false, soon(5));
    amqp.close(c, soon(5));
}

let url = env("AMQP_URL");
scenario_connect(url);
scenario_work_queue(url);
scenario_topic(url);
scenario_rpc(url);
scenario_returns_and_errors(url);
scenario_headers_and_size(url);
scenario_broker_cancel(url);
scenario_forced_close(url, env("AMQP_CLOSE_CMD"));
scenario_tls(env("AMQPS_URL"), env("AMQP_CA"));
scenario_interop(url, env("AMQP_PEER"));
