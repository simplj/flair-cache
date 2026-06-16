package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import com.simplj.flair.cache.gossip.MembershipListener;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.NioEventLoop;
import com.simplj.flair.cache.transport.RawFrame;
import com.simplj.flair.cache.transport.TcpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages outgoing TCP connections to peer nodes.
 * All connections share the server's NioEventLoop so the entire node runs on a single
 * flaircache-nio-selector thread, with incoming frames dispatched by the same IncomingHandler
 * that was wired into the TcpServer.
 */
final class PeerRegistry implements MembershipListener {

    private static final Logger log = Logger.getLogger(PeerRegistry.class.getName());

    private final UUID localNodeId;
    private final NioEventLoop sharedEventLoop;
    private final ConcurrentHashMap<UUID, Connection> outgoing    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Long>       lastPongMs  = new ConcurrentHashMap<>();
    // In-flight guard: prevents submitting a new doConnect while one is already running per peer.
    private final Set<UUID>                           connecting  = ConcurrentHashMap.newKeySet();
    // Earliest wall-clock time at which the next connect attempt is permitted per peer.
    private final ConcurrentHashMap<UUID, Long>       nextRetryMs = new ConcurrentHashMap<>();
    // Consecutive failure count per peer — drives exponential backoff calculation.
    private final ConcurrentHashMap<UUID, Integer>    failureCount = new ConcurrentHashMap<>();
    private final ExecutorService connectPool;
    private final AckTracker ackTracker;

    // Bounded flush window for a graceful peer leave — give the writer a short window to drain
    // pending frames before the connection is torn down. Default 10× the batch window.
    private final long leaveFlushTimeoutMs;

    // Invoked with the number of frames discarded when a dead peer's queue is drained. Wired by
    // the facade to ReplicationMetricsMBean.recordDroppedFrame (replication cannot depend on
    // the metrics module). No-op by default.
    private volatile java.util.function.LongConsumer onFramesDropped = n -> {};

    PeerRegistry(UUID localNodeId, NioEventLoop sharedEventLoop, AckTracker ackTracker) {
        this(localNodeId, sharedEventLoop, ackTracker, 20L);
    }

    PeerRegistry(UUID localNodeId, NioEventLoop sharedEventLoop, AckTracker ackTracker,
                 long leaveFlushTimeoutMs) {
        this.localNodeId = localNodeId;
        this.sharedEventLoop = sharedEventLoop;
        this.ackTracker = ackTracker;
        this.leaveFlushTimeoutMs = leaveFlushTimeoutMs;
        this.connectPool = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                new FlairCacheThreadFactory("flaircache-peer-connect", true));
    }

    /** Registers a sink for the count of frames discarded when a dead peer's queue is drained. */
    void onFramesDropped(java.util.function.LongConsumer sink) {
        if (sink != null) this.onFramesDropped = sink;
    }

    /** Test-only seam: install an outgoing connection directly, bypassing the TCP connect path. */
    void installConnectionForTest(UUID peerId, Connection conn) {
        outgoing.put(peerId, conn);
    }

    void connectAsync(NodeInfo node) {
        if (node.id().equals(localNodeId)) return;
        Connection existing = outgoing.get(node.id());
        if (existing != null && existing.isAlive()) return;

        // In-flight guard: if a doConnect is already running for this peer, skip.
        // The running attempt will update state (success clears backoff; failure sets it).
        if (!connecting.add(node.id())) return;

        // Backoff guard: if the previous failure set a retry deadline that hasn't passed, skip.
        Long retryAt = nextRetryMs.get(node.id());
        if (retryAt != null && System.currentTimeMillis() < retryAt) {
            connecting.remove(node.id());
            return;
        }

        connectPool.submit(() -> {
            try {
                doConnect(node);
            } finally {
                connecting.remove(node.id());
            }
        });
    }

    void send(UUID peerId, RawFrame frame) {
        Connection conn = outgoing.get(peerId);
        if (conn != null && conn.isAlive()) {
            conn.send(frame);
        }
    }

    void sendAll(RawFrame frame) {
        for (Connection conn : outgoing.values()) {
            if (conn.isAlive()) {
                conn.send(frame);
            }
        }
    }

    List<UUID> alivePeerIds() {
        List<UUID> ids = new ArrayList<>();
        for (java.util.Map.Entry<UUID, Connection> e : outgoing.entrySet()) {
            if (e.getValue().isAlive()) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    Collection<Connection> aliveConnections() {
        List<Connection> conns = new ArrayList<>();
        for (Connection c : outgoing.values()) {
            if (c.isAlive()) conns.add(c);
        }
        return conns;
    }

    int aliveCount() {
        int count = 0;
        for (Connection c : outgoing.values()) {
            if (c.isAlive()) count++;
        }
        return count;
    }

    void onPong(UUID peerId) {
        lastPongMs.put(peerId, System.currentTimeMillis());
    }

    void sendPingToAll(UUID localNodeId) {
        RawFrame ping = FrameEncoder.encodePing(localNodeId);
        for (Connection conn : outgoing.values()) {
            if (conn.isAlive()) {
                conn.send(ping);
            }
        }
    }

    void disconnectStalePeers(long pongTimeoutMs) {
        long now = System.currentTimeMillis();
        for (java.util.Map.Entry<UUID, Connection> e : outgoing.entrySet()) {
            UUID peerId = e.getKey();
            if (!e.getValue().isAlive()) continue;
            Long lastPong = lastPongMs.get(peerId);
            if (lastPong != null && now - lastPong > pongTimeoutMs) {
                log.warning("Peer " + peerId + " missed keepalive; closing stale connection");
                disconnectPeer(peerId);
            }
        }
    }

    void disconnectPeer(UUID nodeId) {
        lastPongMs.remove(nodeId);
        Connection conn = outgoing.remove(nodeId);
        if (conn != null) {
            conn.close();
        }
        reevaluateInFlight(nodeId);
    }

    /**
     * Graceful leave: attempt a bounded flush of the peer's pending frames before closing.
     * The flush is capped at {@link #leaveFlushTimeoutMs} so a slow or stuck peer never blocks
     * membership processing indefinitely.
     */
    void disconnectPeerGraceful(UUID nodeId) {
        lastPongMs.remove(nodeId);
        Connection conn = outgoing.remove(nodeId);
        if (conn != null) {
            conn.closeGraceful(leaveFlushTimeoutMs);
        }
        reevaluateInFlight(nodeId);
    }

    /**
     * Dead peer: the peer is unreachable, so drain and discard its queued frames rather than
     * attempting to send them. The number of discarded frames is reported via
     * {@link #onFramesDropped} (wired to the DroppedFrameCount metric by the facade).
     */
    void disconnectPeerDead(UUID nodeId) {
        lastPongMs.remove(nodeId);
        Connection conn = outgoing.remove(nodeId);
        if (conn != null) {
            int discarded = conn.closeAndDiscard();
            if (discarded > 0) {
                onFramesDropped.accept(discarded);
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Discarded " + discarded + " queued frame(s) for dead peer " + nodeId);
                }
            }
        }
        reevaluateInFlight(nodeId);
    }

    // Re-evaluate in-flight QUORUM/STRONG writes immediately: a removed peer can no longer ACK,
    // so any write waiting on it should complete or fail now rather than block for the full
    // ACK timeout.
    private void reevaluateInFlight(UUID nodeId) {
        if (ackTracker != null) {
            ackTracker.onPeerDead(nodeId);
        }
    }

    void shutdown() {
        connectPool.shutdownNow();
        for (Connection conn : outgoing.values()) {
            conn.close();
        }
        outgoing.clear();
        lastPongMs.clear();
        connecting.clear();
        nextRetryMs.clear();
        failureCount.clear();
    }

    // ── MembershipListener ────────────────────────────────────────────────────

    @Override
    public void onJoin(NodeInfo node) {
        clearRetryState(node.id()); // fresh join — no prior failure history applies
        connectAsync(node);
        log.fine("Peer joined, initiating connection: " + node.addressString());
    }

    @Override
    public void onSuspect(NodeInfo node) {
        // Keep the connection — peer may recover
    }

    @Override
    public void onRecover(NodeInfo node) {
        clearRetryState(node.id()); // recovered from SUSPECT — allow immediate reconnect
        Connection existing = outgoing.get(node.id());
        if (existing == null || !existing.isAlive()) {
            connectAsync(node);
        }
    }

    @Override
    public void onLeave(NodeInfo node) {
        clearRetryState(node.id()); // SWIM has removed this peer — no future retries needed
        // Graceful leave: bounded-flush pending frames before tearing the connection down.
        disconnectPeerGraceful(node.id());
        log.fine("Peer left, flushed and closed connection: " + node.id());
    }

    @Override
    public void onDead(NodeInfo node) {
        clearRetryState(node.id()); // SWIM has removed this peer — no future retries needed
        // Dead peer is unreachable: drain-and-discard queued frames, counting them as dropped.
        disconnectPeerDead(node.id());
        log.fine("Peer declared dead, discarded queue and closed connection: " + node.id());
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void doConnect(NodeInfo node) {
        // Guard: if a live connection already exists (raced with another connect attempt), skip
        Connection existing = outgoing.get(node.id());
        if (existing != null && existing.isAlive()) return;

        // Remove the stale dead connection so putIfAbsent below can succeed
        if (existing != null) {
            outgoing.remove(node.id(), existing);
        }

        try {
            // Sharing the server's event loop means all TCP I/O — incoming and outgoing —
            // is multiplexed on one flaircache-nio-selector thread. The shared loop's
            // FrameHandler (IncomingHandler) is inherited automatically; frames from the peer
            // (ACKs, etc.) are dispatched through the same handler as server-side frames.
            Connection conn = TcpClient.builder()
                    .remoteAddress(node.address().getHostAddress())
                    .remotePort(node.port())
                    .eventLoop(sharedEventLoop)
                    .connectTimeoutMs(3000)
                    .build()
                    .connect();
            Connection prev = outgoing.putIfAbsent(node.id(), conn);
            if (prev != null) {
                // Another connect raced and won; close the duplicate
                conn.close();
            } else {
                // Success: clear any accumulated backoff so future disconnects reconnect promptly.
                failureCount.remove(node.id());
                nextRetryMs.remove(node.id());
                // Seed the pong timestamp so disconnectStalePeers gives this connection
                // a full grace period before it expects a PONG reply.
                lastPongMs.put(node.id(), System.currentTimeMillis());
                log.fine("Connected to peer " + node.id() + " at " + node.addressString());
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to connect to peer " + node.addressString(), e);
            // Exponential backoff: 100ms → 200ms → 400ms → 800ms → 1600ms → 3200ms → 10s cap.
            // Prevents reconcilePeers() (runs every 2ms) from flooding the connect pool with
            // attempts against an unreachable peer.
            int failures = failureCount.merge(node.id(), 1, Integer::sum);
            long backoffMs = (failures > 6) ? 10_000L : (100L << (failures - 1));
            nextRetryMs.put(node.id(), System.currentTimeMillis() + backoffMs);
        }
    }

    private void clearRetryState(UUID nodeId) {
        failureCount.remove(nodeId);
        nextRetryMs.remove(nodeId);
        // 'connecting' entry is always removed by the finally block in connectPool.submit()
    }
}
