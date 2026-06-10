package com.simplj.flair.cache.hlc;

import java.nio.ByteBuffer;

public record HLCTimestamp(long logical, long counter) implements Comparable<HLCTimestamp> {

    public static final int BYTES = 16;

    public HLCTimestamp {
        if (logical < 0) throw new IllegalArgumentException("logical must be non-negative: " + logical);
        if (counter < 0) throw new IllegalArgumentException("counter must be non-negative: " + counter);
    }

    @Override
    public int compareTo(HLCTimestamp other) {
        int c = Long.compare(this.logical, other.logical);
        return c != 0 ? c : Long.compare(this.counter, other.counter);
    }

    public boolean isAfter(HLCTimestamp other) {
        return compareTo(other) > 0;
    }

    public boolean isBefore(HLCTimestamp other) {
        return compareTo(other) < 0;
    }

    public void encode(ByteBuffer buf) {
        buf.putLong(logical);
        buf.putLong(counter);
    }

    public ByteBuffer encode() {
        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        encode(buf);
        buf.flip();
        return buf;
    }

    public byte[] encodeToBytes() {
        return encode().array();
    }

    public static HLCTimestamp decode(ByteBuffer buf) {
        return new HLCTimestamp(buf.getLong(), buf.getLong());
    }

    public static HLCTimestamp decode(byte[] bytes) {
        return decode(ByteBuffer.wrap(bytes));
    }
}
