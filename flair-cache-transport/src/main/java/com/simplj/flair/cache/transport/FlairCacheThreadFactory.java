package com.simplj.flair.cache.transport;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

final class FlairCacheThreadFactory implements ThreadFactory {
    private final String name;
    private final boolean indexed;
    private final AtomicInteger counter = new AtomicInteger();

    FlairCacheThreadFactory(String name) {
        this(name, false);
    }

    FlairCacheThreadFactory(String name, boolean indexed) {
        this.name = name;
        this.indexed = indexed;
    }

    @Override
    public Thread newThread(Runnable r) {
        String threadName = indexed ? name + "-" + counter.getAndIncrement() : name;
        Thread t = new Thread(r, threadName);
        t.setDaemon(true);
        return t;
    }
}
