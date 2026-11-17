// URL parsing and negotiation. The handshake, close and heartbeats run
// against scripted brokers in tests/fake.

fn target(url: str) -> Target {
    let r = parse_url(url, options());
    guard let t = r else let e = err_of(r) { panic(e); }
    return t;
}

fn url_err(url: str) -> str {
    let r = parse_url(url, options());
    guard let _t = r else let e = err_of(r) { return e; }
    panic("parsed " + url);
}

fn test_should_parse_urls() {
    let t = target("amqp://localhost");
    assert(t.host == "localhost" && t.port == 5672 && !t.tls && t.user == "guest" && t.vhost == "/", "defaults");
    let t2 = target("amqps://app:p%40ss@mq.internal:5000/prod%2Fa");
    assert(t2.tls && t2.port == 5000 && t2.user == "app" && t2.pass == "p@ss" && t2.vhost == "prod/a", "everything");
    assert(target("amqps://mq").port == 5671, "the amqps port");
    assert(target("amqp://h/%2F").vhost == "/", "the default vhost spelled out");
    assert(target("amqp://h/").vhost == "/", "an empty path");
    let t3 = target("amqp://[::1]:5673/v");
    assert(t3.host == "::1" && t3.port == 5673, "IPv6");
    assert(target("amqp://").host == "localhost", "no host");
    assert(target("amqps://h?auth_mechanism=external").external, "EXTERNAL");
}

fn test_should_read_url_options() {
    let o = options();
    let r = parse_url("amqp://h/?heartbeat=10&frame_max=8192&channel_max=16&connection_name=api%20v2", o);
    guard let _t = r else let e = err_of(r) { panic(e); }
    assert(o.heartbeat == 10 && o.frame_max == 8192 && o.channel_max == 16 && o.connection_name == "api v2", "options");
}

fn test_should_refuse_bad_urls() {
    assert(url_err("http://h") == "amqp: the URL must start with amqp:// or amqps://", "scheme");
    assert(url_err("amqp://h:0") == "amqp: the URL's port must be 1 to 65535", "port");
    assert(url_err("amqp://h:x") == "amqp: the URL's port must be a number", "port text");
    assert(url_err("amqp://h/?nope=1") == "amqp: unknown URL option nope", "an option");
    assert(url_err("amqp://h/?heartbeat=-1") == "amqp: URL option heartbeat must not be negative", "negative");
    assert(url_err("amqp://h/?auth_mechanism=cram") == "amqp: auth_mechanism is plain or external", "mechanism");
    assert(url_err("amqp://[::1") == "amqp: an IPv6 host without its closing ]", "IPv6");
}

fn test_should_negotiate_limits() {
    assert(negotiate(2047, 0) == 2047 && negotiate(0, 100) == 100 && negotiate(0, 0) == 0, "0 is no limit");
    assert(negotiate(131072, 4096) == 4096 && negotiate(60, 600) == 60, "the smaller");
}
