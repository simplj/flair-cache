package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Codec for {@link Set}{@code <E>}. Wire format: count(4) + elements (each encoded by
 * the element codec).
 *
 * <p>{@code null} and empty sets both serialize as count=0 and deserialize as an empty
 * {@link LinkedHashSet}. Insertion order is preserved.
 *
 * <p>Wire format is identical to {@link ListCodec} — the distinction is the Java type
 * used for deserialization ({@code LinkedHashSet} vs {@code ArrayList}).
 */
public final class SetCodec<E> implements Codec<Set<E>> {

    private final Codec<E> elementCodec;

    public SetCodec(Codec<E> elementCodec) {
        this.elementCodec = elementCodec;
    }

    @Override
    public void serialize(Set<E> obj, ByteBuffer buf) {
        if (obj == null || obj.isEmpty()) { buf.putInt(0); return; }
        buf.putInt(obj.size());
        for (E e : obj) elementCodec.serialize(e, buf);
    }

    @Override
    public int sizeOf(Set<E> obj) {
        if (obj == null || obj.isEmpty()) return 4;
        int size = 4;
        for (E e : obj) size += elementCodec.sizeOf(e);
        return size;
    }

    private static final int MAX_ELEMENTS = 10_000_000;

    @Override
    public Set<E> deserialize(ByteBuffer buf) {
        int count = buf.getInt();
        if (count < 0 || count > MAX_ELEMENTS)
            throw new IllegalArgumentException("Set element count out of range: " + count);
        if (count == 0) return new LinkedHashSet<>();
        LinkedHashSet<E> result = new LinkedHashSet<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) result.add(elementCodec.deserialize(buf));
        return result;
    }
}
