package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.Optional;

public final class OptionalCodec<T> implements Codec<Optional<T>> {

    private final Codec<T> valueCodec;

    public OptionalCodec(Codec<T> valueCodec) {
        this.valueCodec = valueCodec;
    }

    @Override
    public void serialize(Optional<T> obj, ByteBuffer buf) {
        if (obj == null || !obj.isPresent()) {
            buf.put((byte) 0);
            return;
        }
        buf.put((byte) 1);
        valueCodec.serialize(obj.get(), buf);
    }

    @Override
    public int sizeOf(Optional<T> obj) {
        if (obj == null || !obj.isPresent()) return 1;
        return 1 + valueCodec.sizeOf(obj.get());
    }

    @Override
    public Optional<T> deserialize(ByteBuffer buf) {
        if (buf.get() == 0) return Optional.empty();
        return Optional.of(valueCodec.deserialize(buf));
    }
}
