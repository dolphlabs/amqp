// Channels: independent sessions multiplexed over one connection.

pub gc struct Channel {
    conn: Conn,
    id: int,
}

// A frame for channel f.channel, from the reader. A frame for a channel
// that is not open is a protocol error.
fn channel_frame(c: Conn, f: Frame) -> result[bool, str] {
    mutex_lock(c.clock);
    let open = has(c.chans, f.channel);
    mutex_unlock(c.clock);
    if !open {
        c.fail_code = 504;
        return err("amqp: a frame for channel " + to_str(f.channel) + ", which is not open");
    }
    return ok(true);
}

// The connection has closed: every channel on it ends.
fn close_channels(c: Conn) {
    mutex_lock(c.clock);
    c.chans = {};
    mutex_unlock(c.clock);
}
