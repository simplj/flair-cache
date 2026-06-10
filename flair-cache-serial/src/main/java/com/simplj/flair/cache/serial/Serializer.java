package com.simplj.flair.cache.serial;

import java.nio.ByteBuffer;

public interface Serializer<T> {

    void serialize(T obj, ByteBuffer buf);

    int sizeOf(T obj);
}
