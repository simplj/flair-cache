package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class ListCodec<T> implements Codec<List<T>> {

    private final Codec<T> elementCodec;

    public ListCodec(Codec<T> elementCodec) {
        this.elementCodec = elementCodec;
    }

    @Override
    public void serialize(List<T> obj, ByteBuffer buf) {
        if (obj == null || obj.isEmpty()) {
            buf.putInt(0);
            return;
        }
        buf.putInt(obj.size());
        for (T item : obj) {
            elementCodec.serialize(item, buf);
        }
    }

    @Override
    public int sizeOf(List<T> obj) {
        if (obj == null || obj.isEmpty()) return 4;
        int size = 4;
        for (T item : obj) {
            size += elementCodec.sizeOf(item);
        }
        return size;
    }

    @Override
    public List<T> deserialize(ByteBuffer buf) {
        int count = buf.getInt();
        List<T> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(elementCodec.deserialize(buf));
        }
        return list;
    }
}
