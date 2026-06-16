package com.simplj.flair.cache.commons;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The single {@link ThreadFactory} used for all thread creation across every FlairCache module.
 *
 * <p>All threads produced are daemon threads with deterministic names. When {@code indexed} is
 * {@code true}, each created thread is suffixed with a monotonically increasing index, which is
 * how per-peer writer threads and pooled worker threads are named.</p>
 *
 * <p>This factory is the only sanctioned way to create threads in FlairCache — {@code new Thread(...)}
 * is never used directly.</p>
 */
public final class FlairCacheThreadFactory implements ThreadFactory {
    private final String name;
    private final boolean indexed;
    private final AtomicInteger counter = new AtomicInteger();

    public FlairCacheThreadFactory(String name) {
        this(name, false);
    }

    public FlairCacheThreadFactory(String name, boolean indexed) {
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
