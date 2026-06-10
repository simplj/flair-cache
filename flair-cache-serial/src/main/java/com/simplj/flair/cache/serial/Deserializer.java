package com.simplj.flair.cache.serial;

import java.nio.ByteBuffer;

public interface Deserializer<T> {

    T deserialize(ByteBuffer buf);
}
