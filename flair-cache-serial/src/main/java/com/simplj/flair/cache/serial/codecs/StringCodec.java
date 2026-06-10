package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

public final class StringCodec implements Codec<String> {

    public static final StringCodec INSTANCE = new StringCodec();

    public static final int MAX_UTF8_BYTES = 0xFFFF;

    private static final ThreadLocal<CharsetEncoder> ENCODER = ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);

    private StringCodec() {}

    @Override
    public void serialize(String obj, ByteBuffer buf) {
        if (obj == null || obj.isEmpty()) {
            buf.putShort((short) 0);
            return;
        }
        // Reserve 2 bytes for the length prefix, encode directly into buf, then backfill length.
        int lenPos = buf.position();
        buf.putShort((short) 0);
        CharsetEncoder enc = ENCODER.get();
        enc.reset();
        CoderResult result = enc.encode(CharBuffer.wrap(obj), buf, true);
        if (result.isOverflow()) {
            throw new BufferOverflowException();
        }
        result = enc.flush(buf);
        if (result.isOverflow()) {
            throw new BufferOverflowException();
        }
        int byteCount = buf.position() - lenPos - 2;
        if (byteCount > MAX_UTF8_BYTES) {
            throw new IllegalArgumentException("String exceeds max serialized length of " + MAX_UTF8_BYTES + " bytes");
        }
        buf.putShort(lenPos, (short) byteCount);
    }

    @Override
    public int sizeOf(String obj) {
        if (obj == null || obj.isEmpty()) return 2;
        int byteCount = utf8ByteCount(obj);
        if (byteCount > MAX_UTF8_BYTES) {
            throw new IllegalArgumentException("String exceeds max serialized length of " + MAX_UTF8_BYTES + " bytes");
        }
        return 2 + byteCount;
    }

    @Override
    public String deserialize(ByteBuffer buf) {
        int len = buf.getShort() & 0xFFFF;
        if (len == 0) return "";
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // Zero-allocation UTF-8 byte count — avoids the byte[] created by String.getBytes().
    private static int utf8ByteCount(String s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char c = s.charAt(i);
            if (c <= 0x7F) {
                count++;
            } else if (c <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                count += 4; // surrogate pair → 4-byte UTF-8 sequence
                i++;
            } else {
                count += 3;
            }
        }
        return count;
    }
}
