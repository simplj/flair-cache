package com.simplj.flair.cache.serial;

import java.util.concurrent.ConcurrentHashMap;

public final class CodecRegistry {

    private final ConcurrentHashMap<Class<?>, Codec<?>> codecs = new ConcurrentHashMap<>();

    public <T> void register(Class<T> type, Codec<T> codec) {
        codecs.put(type, codec);
    }

    public void deregister(Class<?> type) {
        codecs.remove(type);
    }

    @SuppressWarnings("unchecked")
    public <T> Codec<T> lookup(Class<T> type) {
        return (Codec<T>) codecs.get(type);
    }
}
