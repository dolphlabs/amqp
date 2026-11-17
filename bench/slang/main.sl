// The slang side of bench/run.sh: one scenario per run.
//
//     main publish|confirm|consume|latency URL N SIZE
//
// prints: rate=<msgs/s> or p50_us=.. p99_us=..

import "amqp";
import "proc";
import "sort";
import "strings";
import "time";

fn soon(s: int) -> until {
    return until_of(time.mono() + s * 1000000000);
}

fn must[T](what: str, r: result[T, str]) -> T {
    guard let v = r else let e = err_of(r) { panic(what + ": " + e); }
    return v;
}

fn passive() -> amqp.QueueOptions {
    let o = amqp.queue_options();
    o.passive = true;
    return o;
}

let argv = proc.args();
let mode = argv[1];
let url = argv[2];
let n = must("N", to_int(argv[3]));
let size = must("SIZE", to_int(argv[4]));
let c = must("dial", amqp.dial(url, amqp.options(), soon(10)));
let ch = must("channel", amqp.channel(c, soon(5)));
let body = strings.bytes_zero(size);

if mode == "publish" || mode == "confirm" {
    must("declare", amqp.queue_declare(ch, "bench.q", amqp.queue_options(), soon(5)));
    must("purge", amqp.queue_purge(ch, "bench.q", soon(5)));
    if mode == "confirm" {
        must("confirm_select", amqp.confirm_select(ch, soon(5)));
    }
    let msg = amqp.publishing(body);
    let t0 = time.mono();
    for i in 0..n {
        must("publish", amqp.publish(ch, "", "bench.q", msg, false));
        if mode == "confirm" && (i + 1) % 1000 == 0 {
            must("confirms", amqp.wait_confirms(ch, soon(60)));
        }
    }
    if mode == "confirm" {
        must("confirms", amqp.wait_confirms(ch, soon(60)));
    }
    // a round trip on the same channel: the broker has read every publish
    must("barrier", amqp.queue_declare(ch, "bench.q", passive(), soon(60)));
    let secs = (time.mono() - t0) as float / 1000000000.0;
    println("rate=" + to_str((n as float / secs) as int));
} else if mode == "consume" {
    must("qos", amqp.qos(ch, 500, soon(5)));
    let sub = must("consume", amqp.consume(ch, "bench.q", amqp.consume_options(), soon(5)));
    let t0 = time.mono();
    for i in 0..n {
        let got = must("next", amqp.next(sub, soon(60)));
        guard let d = got else { panic("no delivery within 60 s"); }
        must("ack", amqp.ack(ch, d.delivery_tag, false));
    }
    let secs = (time.mono() - t0) as float / 1000000000.0;
    println("rate=" + to_str((n as float / secs) as int));
} else if mode == "latency" {
    let o = amqp.queue_options();
    o.exclusive = true;
    o.auto_delete = true;
    let q = must("declare", amqp.queue_declare(ch, "", o, soon(5)));
    let cc = must("dial", amqp.dial(url, amqp.options(), soon(10)));
    let cch = must("channel", amqp.channel(cc, soon(5)));
    let no_ack = amqp.consume_options();
    no_ack.no_ack = true;
    let sub = must("consume", amqp.consume(ch, q.name, no_ack, soon(5)));
    let msg = amqp.publishing(body);
    let lat: [int] = [];
    for i in 0..n {
        let t0 = time.mono();
        must("publish", amqp.publish(cch, "", q.name, msg, false));
        let got = must("next", amqp.next(sub, soon(10)));
        guard let _d = got else { panic("no delivery within 10 s"); }
        push(lat, ((time.mono() - t0) as int) / 1000);
    }
    lat = sort.ints(lat);
    println("p50_us=" + to_str(lat[n / 2]) + " p99_us=" + to_str(lat[n * 99 / 100]));
    amqp.close(cc, soon(5));
}
amqp.close(c, soon(5));
