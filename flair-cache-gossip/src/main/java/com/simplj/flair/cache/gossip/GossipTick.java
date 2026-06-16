package com.simplj.flair.cache.gossip;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

final class GossipTick {
    private static final Logger log = Logger.getLogger(GossipTick.class.getName());

    private final GossipNode node;
    private final long       tickIntervalMs;
    private ScheduledExecutorService scheduler;

    GossipTick(GossipNode node, long tickIntervalMs) {
        this.node           = node;
        this.tickIntervalMs = tickIntervalMs;
    }

    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new FlairCacheThreadFactory("flaircache-gossip-tick"));
        // Fixed delay: next tick starts after current one completes, preventing pile-up
        scheduler.scheduleWithFixedDelay(this::tick, tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS);
    }

    void stop() {
        if (scheduler != null) scheduler.shutdown();
    }

    private void tick() {
        try {
            node.onTick();
        } catch (Exception e) {
            log.log(Level.WARNING, "Gossip tick error", e);
        }
    }
}
