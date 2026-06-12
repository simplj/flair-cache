package com.simplj.flair.cache.gossip;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

final class PiggybackQueue {

    private static final class Entry {
        final NodeInfo info;
        final long     generation;   // bumped on every add(); used to detect concurrent replacement
        final int      transmitCount;

        Entry(NodeInfo info, long generation, int transmitCount) {
            this.info          = info;
            this.generation    = generation;
            this.transmitCount = transmitCount;
        }
    }

    // Keyed by nodeId — latest state per node
    private final ConcurrentHashMap<UUID, Entry> queue   = new ConcurrentHashMap<>();
    private final AtomicLong                     nextGen = new AtomicLong(0);

    void add(NodeInfo info) {
        long gen = nextGen.incrementAndGet();
        queue.merge(info.id(), new Entry(info, gen, 0), (existing, incoming) -> {
            // Higher incarnation wins; same incarnation: DEAD > SUSPECTED > ALIVE
            if (dominates(incoming.info, existing.info)) return incoming;
            return existing;
        });
    }

    List<NodeInfo> drain(int maxDeltas, int clusterSize) {
        int maxTransmits = Math.max(1, (int) (Math.log(Math.max(clusterSize, 2)) / Math.log(2)));

        List<Entry> eligible = new ArrayList<>(queue.size());
        for (Entry e : queue.values()) {
            if (e.transmitCount < maxTransmits) eligible.add(e);
        }
        eligible.sort(PRIORITY_ORDER);

        List<NodeInfo> result = new ArrayList<>(Math.min(maxDeltas, eligible.size()));
        for (int i = 0; i < eligible.size() && i < maxDeltas; i++) {
            Entry e = eligible.get(i);
            result.add(e.info);
            // Use generation to detect if a concurrent add() replaced this entry since we
            // snapshotted it. If generation changed, the new entry starts at count=0 and
            // will be picked up on the next drain; don't increment the stale entry's count.
            queue.computeIfPresent(e.info.id(), (id, cur) ->
                    cur.generation == e.generation
                            ? new Entry(cur.info, cur.generation, cur.transmitCount + 1)
                            : cur);
        }

        // Evict fully-transmitted entries
        queue.values().removeIf(e -> e.transmitCount >= maxTransmits);
        return result;
    }

    void clear() { queue.clear(); }

    // DEAD(0) > SUSPECTED(1) > ALIVE(2); fewer transmits first
    private static final Comparator<Entry> PRIORITY_ORDER = (a, b) -> {
        int pa = statusPriority(a.info.status()), pb = statusPriority(b.info.status());
        return pa != pb ? Integer.compare(pa, pb) : Integer.compare(a.transmitCount, b.transmitCount);
    };

    private static int statusPriority(NodeStatus s) {
        switch (s) {
            case DEAD:      return 0;
            case SUSPECTED: return 1;
            default:        return 2;
        }
    }

    private static boolean dominates(NodeInfo incoming, NodeInfo existing) {
        if (incoming.incarnation() > existing.incarnation()) return true;
        if (incoming.incarnation() < existing.incarnation()) return false;
        return statusPriority(incoming.status()) < statusPriority(existing.status());
    }
}
