// Frames, the protocol header, and content headers (a message's size and
// properties).
//
// A frame is a type octet, a channel (short), a payload length (long),
// the payload, and the end octet 0xCE. A message is a method frame
// (basic.publish, basic.deliver, basic.get-ok, basic.return), a content
// header frame, and body frames of at most frame-max - 8 bytes each.

import "builder";

let FRAME_METHOD = 1;
let FRAME_HEADER = 2;
let FRAME_BODY = 3;
let FRAME_HEARTBEAT = 8;
let FRAME_END = 206;

pub gc struct Frame {
    kind: int,
    channel: int,
    payload: bytes,
}

// What scan_frame found: status 0 needs more bytes, 1 found a frame
// that ends at next.
pub gc struct FrameScan {
    status: int,
    frame: Frame,
    next: int,
}

// "AMQP" 0 0 9 1: what a client sends before anything else.
pub fn protocol_header() -> bytes {
    let w = builder.new_bytes();
    w.write(to_bytes("AMQP"));
    wr_u8(w, 0);
    wr_u8(w, 0);
    wr_u8(w, 9);
    wr_u8(w, 1);
    return w.finish();
}

pub fn encode_frame(kind: int, channel: int, payload: bytes) -> bytes {
    let w = builder.new_bytes();
    wr_u8(w, kind);
    wr_u16(w, channel);
    wr_u32(w, len(payload));
    w.write(payload);
    wr_u8(w, FRAME_END);
    return w.finish();
}

// The frame starting at b[from], if b[from..to] holds all of it. A frame
// longer than frame_max (when it is not 0) or without its end octet is an
// error: the connection cannot be read past it.
pub fn scan_frame(b: bytes, from: int, to: int, frame_max: int) -> result[FrameScan, str] {
    let none_yet = FrameScan { status: 0, frame: Frame { kind: 0, channel: 0, payload: b"" }, next: from };
    if to - from < 7 {
        return ok(none_yet);
    }
    let r = reader(b, from, to);
    let kind = rd_u8(r);
    let ch = rd_u16(r);
    let size = rd_u32(r);
    if frame_max > 0 && size + 8 > frame_max {
        return err("amqp: a frame of " + to_str(size + 8) + " bytes, over the negotiated frame-max of " +
                   to_str(frame_max));
    }
    if kind != FRAME_METHOD && kind != FRAME_HEADER && kind != FRAME_BODY && kind != FRAME_HEARTBEAT {
        return err("amqp: an unknown frame type " + to_str(kind));
    }
    if to - from < 8 + size {
        return ok(none_yet);
    }
    if b[from + 7 + size] != FRAME_END {
        return err("amqp: a frame without its end octet (0xCE)");
    }
    let f = Frame { kind: kind, channel: ch, payload: b[from + 7..from + 7 + size] };
    return ok(FrameScan { status: 1, frame: f, next: from + 8 + size });
}

// ---- content headers --------------------------------------------------

// A message's properties (basic class). An empty string, a 0
// delivery_mode, a -1 priority or timestamp and a none headers are
// absent: not sent, and how an absent one reads.
pub gc struct Properties {
    content_type: str,
    content_encoding: str,
    headers: opt[Table],
    // 1 transient, 2 persistent
    delivery_mode: int,
    // 0-9 (or the queue's x-max-priority)
    priority: int,
    correlation_id: str,
    reply_to: str,
    // milliseconds, as a decimal string ("60000")
    expiration: str,
    message_id: str,
    // seconds since the Unix epoch
    timestamp: int,
    type_name: str,
    user_id: str,
    app_id: str,
    cluster_id: str,
}

pub fn properties() -> Properties {
    return Properties { content_type: "", content_encoding: "", headers: none, delivery_mode: 0, priority: -1,
                        correlation_id: "", reply_to: "", expiration: "", message_id: "", timestamp: -1,
                        type_name: "", user_id: "", app_id: "", cluster_id: "" };
}

pub gc struct ContentHeader {
    class_id: int,
    body_size: int,
    props: Properties,
}

// The content header frame's payload for a message of body_size bytes.
pub fn encode_header(class_id: int, body_size: int, p: Properties) -> result[bytes, str] {
    let shorts = [p.content_type, p.content_encoding, p.correlation_id, p.reply_to, p.expiration, p.message_id,
                  p.type_name, p.user_id, p.app_id, p.cluster_id];
    for s in shorts {
        if !short_ok(s) {
            return err("amqp: a message property over 255 bytes");
        }
    }
    if p.delivery_mode < 0 || p.delivery_mode > 255 || p.priority > 255 {
        return err("amqp: delivery_mode and priority are octets");
    }
    let flags = 0;
    if p.content_type != "" {
        flags = flags | 32768;
    }
    if p.content_encoding != "" {
        flags = flags | 16384;
    }
    let has_headers = false;
    if let _h = p.headers {
        has_headers = true;
        flags = flags | 8192;
    }
    if p.delivery_mode != 0 {
        flags = flags | 4096;
    }
    if p.priority >= 0 {
        flags = flags | 2048;
    }
    if p.correlation_id != "" {
        flags = flags | 1024;
    }
    if p.reply_to != "" {
        flags = flags | 512;
    }
    if p.expiration != "" {
        flags = flags | 256;
    }
    if p.message_id != "" {
        flags = flags | 128;
    }
    if p.timestamp >= 0 {
        flags = flags | 64;
    }
    if p.type_name != "" {
        flags = flags | 32;
    }
    if p.user_id != "" {
        flags = flags | 16;
    }
    if p.app_id != "" {
        flags = flags | 8;
    }
    if p.cluster_id != "" {
        flags = flags | 4;
    }
    let w = builder.new_bytes();
    wr_u16(w, class_id);
    wr_u16(w, 0);
    wr_u64(w, body_size);
    wr_u16(w, flags);
    if p.content_type != "" {
        wr_shortstr(w, p.content_type);
    }
    if p.content_encoding != "" {
        wr_shortstr(w, p.content_encoding);
    }
    if has_headers {
        let tr = encode_table(w, p.headers ?? table(), 0);
        guard let _t = tr else let e = err_of(tr) { return err(e); }
    }
    if p.delivery_mode != 0 {
        wr_u8(w, p.delivery_mode);
    }
    if p.priority >= 0 {
        wr_u8(w, p.priority);
    }
    if p.correlation_id != "" {
        wr_shortstr(w, p.correlation_id);
    }
    if p.reply_to != "" {
        wr_shortstr(w, p.reply_to);
    }
    if p.expiration != "" {
        wr_shortstr(w, p.expiration);
    }
    if p.message_id != "" {
        wr_shortstr(w, p.message_id);
    }
    if p.timestamp >= 0 {
        wr_u64(w, p.timestamp);
    }
    if p.type_name != "" {
        wr_shortstr(w, p.type_name);
    }
    if p.user_id != "" {
        wr_shortstr(w, p.user_id);
    }
    if p.app_id != "" {
        wr_shortstr(w, p.app_id);
    }
    if p.cluster_id != "" {
        wr_shortstr(w, p.cluster_id);
    }
    return ok(w.finish());
}

pub fn decode_header(payload: bytes) -> result[ContentHeader, str] {
    let r = reader(payload, 0, len(payload));
    let cid = rd_u16(r);
    let _weight = rd_u16(r);
    let size = rd_u64(r);
    let flags = rd_u16(r);
    if r.failed != "" {
        return err(r.failed);
    }
    if (flags & 1) != 0 {
        return err("amqp: a content header with more property flags than the basic class has");
    }
    if (flags & 2) != 0 {
        return err("amqp: a content header with an unknown property (flag bit 1)");
    }
    if size < 0 {
        return err("amqp: a body size over 2^63");
    }
    let p = properties();
    if (flags & 32768) != 0 {
        p.content_type = rd_shortstr(r);
    }
    if (flags & 16384) != 0 {
        p.content_encoding = rd_shortstr(r);
    }
    if (flags & 8192) != 0 {
        p.headers = some(decode_table(r, 0));
    }
    if (flags & 4096) != 0 {
        p.delivery_mode = rd_u8(r);
    }
    if (flags & 2048) != 0 {
        p.priority = rd_u8(r);
    }
    if (flags & 1024) != 0 {
        p.correlation_id = rd_shortstr(r);
    }
    if (flags & 512) != 0 {
        p.reply_to = rd_shortstr(r);
    }
    if (flags & 256) != 0 {
        p.expiration = rd_shortstr(r);
    }
    if (flags & 128) != 0 {
        p.message_id = rd_shortstr(r);
    }
    if (flags & 64) != 0 {
        p.timestamp = rd_u64(r);
    }
    if (flags & 32) != 0 {
        p.type_name = rd_shortstr(r);
    }
    if (flags & 16) != 0 {
        p.user_id = rd_shortstr(r);
    }
    if (flags & 8) != 0 {
        p.app_id = rd_shortstr(r);
    }
    if (flags & 4) != 0 {
        p.cluster_id = rd_shortstr(r);
    }
    if r.failed != "" {
        return err(r.failed);
    }
    if r.pos != r.end {
        return err("amqp: " + to_str(r.end - r.pos) + " bytes after a content header's properties");
    }
    return ok(ContentHeader { class_id: cid, body_size: size, props: p });
}
