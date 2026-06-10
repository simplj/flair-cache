package com.simplj.flair.cache.hlc;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class HLCTimestampTest {

    @Test
    void compareTo_logicalDominates() {
        HLCTimestamp a = new HLCTimestamp(1000, 99);
        HLCTimestamp b = new HLCTimestamp(2000, 0);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }

    @Test
    void compareTo_counterBreaksTie() {
        HLCTimestamp a = new HLCTimestamp(1000, 1);
        HLCTimestamp b = new HLCTimestamp(1000, 2);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }

    @Test
    void compareTo_equalTimestamps() {
        HLCTimestamp a = new HLCTimestamp(1000, 5);
        HLCTimestamp b = new HLCTimestamp(1000, 5);
        assertEquals(0, a.compareTo(b));
    }

    @Test
    void isAfter_isBefore_consistency() {
        HLCTimestamp earlier = new HLCTimestamp(100, 0);
        HLCTimestamp later   = new HLCTimestamp(200, 0);
        assertTrue(later.isAfter(earlier));
        assertTrue(earlier.isBefore(later));
        assertFalse(earlier.isAfter(later));
        assertFalse(later.isBefore(earlier));
    }

    @Test
    void isAfter_equalTimestamps_returnsFalse() {
        HLCTimestamp ts = new HLCTimestamp(500, 3);
        assertFalse(ts.isAfter(ts));
        assertFalse(ts.isBefore(ts));
    }

    @Test
    void encode_decode_byteBuffer_roundTrip() {
        // 0x7AFE... has bit 63 clear — a valid non-negative long with dense bit pattern
        HLCTimestamp original = new HLCTimestamp(0x7AFEBABADEADBEEFL, 0x0102030405060708L);
        ByteBuffer buf = ByteBuffer.allocate(HLCTimestamp.BYTES);
        original.encode(buf);
        buf.flip();
        assertEquals(original, HLCTimestamp.decode(buf));
        assertFalse(buf.hasRemaining(), "decode must consume exactly 16 bytes");
    }

    @Test
    void encode_byteBuffer_noArg_returnsFreshFlippedBuffer() {
        HLCTimestamp ts = new HLCTimestamp(1234L, 5L);
        ByteBuffer buf = ts.encode();
        assertEquals(HLCTimestamp.BYTES, buf.remaining());
        assertEquals(ts, HLCTimestamp.decode(buf));
        assertFalse(buf.hasRemaining(), "decode must consume exactly 16 bytes");
    }

    @Test
    void encodeToBytes_roundTrip() {
        HLCTimestamp original = new HLCTimestamp(System.currentTimeMillis(), 42);
        byte[] bytes = original.encodeToBytes();
        assertEquals(HLCTimestamp.BYTES, bytes.length);
        assertEquals(original, HLCTimestamp.decode(bytes));
    }

    @Test
    void encodeToBytes_matchesEncodeByteBuffer() {
        HLCTimestamp ts = new HLCTimestamp(9999L, 7L);
        ByteBuffer buf = ts.encode();
        byte[] fromBuf = new byte[HLCTimestamp.BYTES];
        buf.get(fromBuf);
        assertArrayEquals(fromBuf, ts.encodeToBytes());
    }

    @Test
    void encode_isBigEndian() {
        HLCTimestamp ts = new HLCTimestamp(0x0102030405060708L, 0x090A0B0C0D0E0F10L);
        byte[] bytes = ts.encodeToBytes();
        // logical occupies bytes 0-7
        assertEquals(0x01, bytes[0] & 0xFF);
        assertEquals(0x08, bytes[7] & 0xFF);
        // counter occupies bytes 8-15
        assertEquals(0x09, bytes[8] & 0xFF);
        assertEquals(0x10, bytes[15] & 0xFF);
    }

    @Test
    void bytes_constant_is16() {
        assertEquals(16, HLCTimestamp.BYTES);
    }

    @Test
    void encode_extremeValues_roundTrip() {
        HLCTimestamp zero = new HLCTimestamp(0L, 0L);
        HLCTimestamp max  = new HLCTimestamp(Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(zero, HLCTimestamp.decode(zero.encodeToBytes()));
        assertEquals(max,  HLCTimestamp.decode(max.encodeToBytes()));
    }

    // ── Constructor validation ─────────────────────────────────────────────────

    @Test
    void constructor_negativeLogical_throws() {
        assertThrows(IllegalArgumentException.class, () -> new HLCTimestamp(-1, 0));
    }

    @Test
    void constructor_negativeCounter_throws() {
        assertThrows(IllegalArgumentException.class, () -> new HLCTimestamp(0, -1));
    }

    @Test
    void decode_corruptWire_negativeLogical_throws() {
        // MSB set on first byte → signed negative long for the logical field
        byte[] bytes = new byte[HLCTimestamp.BYTES];
        bytes[0] = (byte) 0x80;
        assertThrows(IllegalArgumentException.class, () -> HLCTimestamp.decode(bytes));
    }
}
