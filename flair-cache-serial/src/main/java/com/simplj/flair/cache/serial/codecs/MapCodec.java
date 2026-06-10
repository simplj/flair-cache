package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class MapCodec<K, V> implements Codec<Map<K, V>> {

    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    public MapCodec(Codec<K> keyCodec, Codec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public void serialize(Map<K, V> obj, ByteBuffer buf) {
        if (obj == null || obj.isEmpty()) {
            buf.putInt(0);
            return;
        }
        buf.putInt(obj.size());
        for (Map.Entry<K, V> entry : obj.entrySet()) {
            keyCodec.serialize(entry.getKey(), buf);
            valueCodec.serialize(entry.getValue(), buf);
        }
    }

    @Override
    public int sizeOf(Map<K, V> obj) {
        if (obj == null || obj.isEmpty()) return 4;
        int size = 4;
        for (Map.Entry<K, V> entry : obj.entrySet()) {
            size += keyCodec.sizeOf(entry.getKey()) + valueCodec.sizeOf(entry.getValue());
        }
        return size;
    }

    @Override
    public Map<K, V> deserialize(ByteBuffer buf) {
        int count = buf.getInt();
        Map<K, V> map = new HashMap<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) {
            K key = keyCodec.deserialize(buf);
            V value = valueCodec.deserialize(buf);
            map.put(key, value);
        }
        return map;
    }
}
