package com.simplj.flair.cache.gossip;

import java.util.concurrent.atomic.AtomicLong;

final class IncarnationClock {
    private final AtomicLong value = new AtomicLong(0);

    long current()   { return value.get(); }
    long increment() { return value.incrementAndGet(); }
}
