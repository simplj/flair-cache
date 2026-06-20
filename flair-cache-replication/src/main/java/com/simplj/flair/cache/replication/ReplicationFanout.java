package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import com.simplj.flair.cache.gossip.MembershipList;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.transport.RawFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

final class ReplicationFanout implements Runnable {

    private static final Logger log = Logger.getLogger(ReplicationFanout.class.getName());

    record QueuedEvent(ReplicationEvent event, long frameId, boolean needsAck) {}

    private final LinkedBlockingQueue<QueuedEvent> queue;
    private final PeerRegistry peerRegistry;
    private final MembershipList membershipList;
    private final UUID localNodeId;
    private final long batchWindowMs;
    private final int batchMaxFrames;

    private final Thread thread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile boolean running = true;
    // Counts total per-peer frame sends: each fanout of 1 frame to N peers increments by N.
    // Used by awaitReplication() to detect when all distributed frames have been applied.
    private final LongAdder totalDistributed = new LongAdder();
    // Tracks frames from offer() until after fanout() completes. Unlike queue.size() (which drops
    // to 0 when the fanout thread polls the frame but before totalDistributed is incremented),
    // this counter stays positive until fanout() finishes updating totalDistributed. This closes
    // the race where awaitReplication() sees pendingFrameCount=0 and totalDist=0 simultaneously
    // (0>=0 false positive) while a frame is between queue.poll() and fanout() completion.
    private final AtomicInteger pendingFanout = new AtomicInteger(0);

    ReplicationFanout(PeerRegistry peerRegistry, MembershipList membershipList, UUID localNodeId,
                      long batchWindowMs, int batchMaxFrames) {
        this.peerRegistry = peerRegistry;
        this.membershipList = membershipList;
        this.localNodeId = localNodeId;
        this.batchWindowMs = batchWindowMs;
        this.batchMaxFrames = batchMaxFrames;
        this.queue = new LinkedBlockingQueue<>(65536);
        this.thread = new FlairCacheThreadFactory("flaircache-replication-fan").newThread(this);
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            thread.start();
        }
    }

    void shutdown() {
        running = false;
        thread.interrupt();
    }

    boolean offer(QueuedEvent event) {
        pendingFanout.incrementAndGet(); // claim before enqueue
        if (queue.offer(event)) {
            return true;
        }
        pendingFanout.decrementAndGet(); // queue full — roll back
        return false;
    }

    int queueSize() {
        return pendingFanout.get();
    }

    long totalDistributed() {
        return totalDistributed.sum();
    }

    @Override
    public void run() {
        List<QueuedEvent> batch = new ArrayList<>(batchMaxFrames);

        while (running) {
            try {
                // Reconcile peer connections on every cycle so joins/leaves detected by gossip
                // are reflected in the outgoing connection map without needing a listener hook.
                reconcilePeers();

                QueuedEvent first = queue.poll(batchWindowMs, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    while (batch.size() < batchMaxFrames) {
                        QueuedEvent next = queue.poll();
                        if (next == null) break;
                        batch.add(next);
                    }
                }

                if (!batch.isEmpty()) {
                    int batchCount = batch.size();
                    try {
                        fanout(batch);
                    } finally {
                        // Decrement after fanout() — guarantees pendingFanout reaches 0 only
                        // after totalDistributed has been updated for every frame in the batch.
                        pendingFanout.addAndGet(-batchCount);
                        batch.clear();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.log(Level.WARNING, "Fanout error", e);
            }
        }

        // Drain remaining events on shutdown (best-effort; QUORUM/STRONG futures fail via AckTracker.shutdown)
        List<QueuedEvent> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            try {
                fanout(remaining);
            } catch (Exception e) {
                log.log(Level.WARNING, "Final fanout flush error", e);
            } finally {
                pendingFanout.addAndGet(-remaining.size());
            }
        }
    }

    private void reconcilePeers() {
        List<NodeInfo> alive = membershipList.alive();
        for (NodeInfo node : alive) {
            if (!node.id().equals(localNodeId)) {
                peerRegistry.connectAsync(node);
            }
        }
    }

    private void fanout(List<QueuedEvent> batch) {
        // Enumerate all known peers: both live TCP connections and peers whose connection
        // (or TLS handshake) is still in progress. sendOrBuffer() delivers directly to
        // live peers and holds frames in a bounded per-peer queue for connecting peers,
        // flushing them in order once the connection becomes alive.
        List<UUID> targets = peerRegistry.knownPeerIds();
        if (targets.isEmpty()) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Fanout: no known peers, skipping " + batch.size() + " event(s)");
            }
            return;
        }

        for (QueuedEvent queued : batch) {
            RawFrame frame = encode(queued);
            if (frame == null) continue;
            for (UUID peerId : targets) {
                peerRegistry.sendOrBuffer(peerId, frame);
                totalDistributed.increment();
            }
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Fanned out frameId=" + queued.frameId()
                        + " to " + targets.size() + " peer(s)");
            }
        }
    }

    private RawFrame encode(QueuedEvent queued) {
        ReplicationEvent event = queued.event();
        if (event instanceof ReplicationEvent.PutEvent put) {
            return FrameEncoder.encodePut(queued.frameId(), queued.needsAck(), put);
        } else if (event instanceof ReplicationEvent.DeleteEvent del) {
            return FrameEncoder.encodeDelete(queued.frameId(), queued.needsAck(), del);
        }
        log.warning("Unknown ReplicationEvent type: " + event.getClass().getName());
        return null;
    }
}
