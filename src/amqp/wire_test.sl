// Unit tests: `slangc test src/amqp`. Byte-for-byte vectors checked
// against pika are tests/wire.

import "builder";
import "encoding";
import "strings";

fn table_bytes(t: Table) -> bytes {
    let w = builder.new_bytes();
    let r = encode_table(w, t, 0);
    guard let _ok = r else let e = err_of(r) { panic(e); }
    return w.finish();
}

fn test_should_set_and_get_table_values() {
    let t = table();
    table_set(t, "x-max-priority", i32_value(10));
    table_set(t, "x-queue-type", str_value("quorum"));
    table_set(t, "x-max-priority", i32_value(5));
    assert(len(t.keys) == 2, "setting a key again replaces it");
    let v = table_get(t, "x-max-priority") ?? void_value();
    assert((as_int(v) ?? 0) == 5, "the replaced value");
    assert((as_str(table_get(t, "x-queue-type") ?? void_value()) ?? "") == "quorum", "a string");
    assert((as_str(i32_value(1)) ?? "none") == "none", "an int is not a string");
    assert((as_int(table_get(t, "nope") ?? str_value("x")) ?? -1) == -1, "a missing key");
}

fn test_should_round_trip_every_value_type() {
    let inner = table();
    table_set(inner, "n", int_value(-9223372036854775807 - 1));
    let t = table();
    table_set(t, "b", bool_value(true));
    table_set(t, "f", float_value(-0.1));
    table_set(t, "s", str_value("é☃"));
    table_set(t, "x", bytes_value(strings.bytes_zero(3)));
    table_set(t, "ts", timestamp_value(1700000000));
    table_set(t, "d", decimal_value(2, 1234));
    table_set(t, "v", void_value());
    table_set(t, "nested", table_value(inner));
    table_set(t, "list", array_value([i32_value(-1), str_value("a")]));
    let bb = table_bytes(t);
    let r = reader(bb, 0, len(bb));
    let back = decode_table(r, 0);
    assert(r.failed == "", r.failed);
    assert(encoding.hex_encode(table_bytes(back)) == encoding.hex_encode(bb), "re-encoded the same");
    assert((as_float(table_get(back, "f") ?? void_value()) ?? 0.0) == -0.1, "a double is exact");
    let n = as_table(table_get(back, "nested") ?? void_value()) ?? table();
    assert((as_int(table_get(n, "n") ?? void_value()) ?? 0) == -9223372036854775807 - 1, "i64 min");
}

fn test_should_refuse_what_cannot_be_encoded() {
    let t = table();
    table_set(t, strings.repeat("k", 256), void_value());
    let w = builder.new_bytes();
    let r = encode_table(w, t, 0);
    guard let _a = r else let e = err_of(r) { assert(strings.contains(e, "1 to 255 bytes"), e); return; }
    panic("a 256-byte key encoded");
}

fn test_should_refuse_an_int_outside_its_type() {
    let t = table();
    table_set(t, "k", i32_value(2147483648));
    let w = builder.new_bytes();
    let r = encode_table(w, t, 0);
    guard let _a = r else let e = err_of(r) { assert(strings.contains(e, "does not fit field type 'I'"), e); return; }
    panic("an i32 field held 2^31");
}

fn test_should_refuse_a_long_short_string_in_a_method() {
    let m = method(50, 10, [arg_int(0), arg_str(strings.repeat("q", 256)), arg_bool(false), arg_bool(true),
                            arg_bool(false), arg_bool(false), arg_bool(false), arg_table(table())]);
    let r = encode_method(m);
    guard let _b = r else let e = err_of(r) { assert(strings.contains(e, "over 255 bytes"), e); return; }
    panic("a 256-byte queue name encoded");
}

fn test_should_refuse_the_wrong_argument_count() {
    let r = encode_method(method(60, 80, [arg_int(1)]));
    guard let _b = r else let e = err_of(r) { assert(strings.contains(e, "takes 2 arguments, given 1"), e); return; }
    panic("basic.ack encoded with one argument");
}

fn test_should_pack_bits_into_octets() {
    // queue.declare: durable and auto-delete set -> bits 0b01010 after the name
    let m = method(50, 10, [arg_int(0), arg_str("q"), arg_bool(false), arg_bool(true), arg_bool(false),
                            arg_bool(true), arg_bool(false), arg_table(table())]);
    let r = encode_method(m);
    guard let b = r else let e = err_of(r) { panic(e); }
    assert(encoding.hex_encode(b) == "0032000a00000171" + "0a" + "00000000", encoding.hex_encode(b));
    let dr = decode_method(b);
    guard let back = dr else let e = err_of(dr) { panic(e); }
    assert(back.args[3].i == 1 && back.args[5].i == 1 && back.args[4].i == 0, "bits read back");
}

fn test_should_scan_frames_from_one_buffer() {
    let a = encode_frame(1, 3, to_bytes("abc"));
    let b = encode_frame(8, 0, b"");
    let w = builder.new_bytes();
    w.write(a);
    w.write(b);
    w.write(a[0..5]);
    let buf = w.finish();
    let r1 = scan_frame(buf, 0, len(buf), 0);
    guard let s1 = r1 else let e = err_of(r1) { panic(e); }
    assert(s1.status == 1 && s1.frame.channel == 3 && to_str(s1.frame.payload) == "abc", "first frame");
    let r2 = scan_frame(buf, s1.next, len(buf), 0);
    guard let s2 = r2 else let e = err_of(r2) { panic(e); }
    assert(s2.status == 1 && s2.frame.kind == 8, "heartbeat");
    let r3 = scan_frame(buf, s2.next, len(buf), 0);
    guard let s3 = r3 else let e = err_of(r3) { panic(e); }
    assert(s3.status == 0 && s3.next == s2.next, "a partial frame waits");
}

fn test_should_write_the_protocol_header() {
    assert(encoding.hex_encode(protocol_header()) == "414d515000000901", "AMQP 0 0 9 1");
}

fn test_should_leave_absent_properties_out() {
    let r = encode_header(60, 5, properties());
    guard let b = r else let e = err_of(r) { panic(e); }
    assert(encoding.hex_encode(b) == "003c000000000000000000050000", encoding.hex_encode(b));
    let p = properties();
    p.delivery_mode = 2;
    p.priority = 0;
    let r2 = encode_header(60, 5, p);
    guard let b2 = r2 else let e = err_of(r2) { panic(e); }
    let hr = decode_header(b2);
    guard let h = hr else let e = err_of(hr) { panic(e); }
    assert(h.props.delivery_mode == 2 && h.props.priority == 0 && h.props.timestamp == -1, "priority 0 is present");
}
