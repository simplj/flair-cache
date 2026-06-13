package com.simplj.flair.cache.replication;

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
    private final ConcurrentHashMap<UUID, Connection> outgoing = new ConcurrentHashMap<>();
    private final ExecutorService connectPool;

    PeerRegistry(UUID localNodeId, NioEventLoop sharedEventLoop) {
        this.localNodeId = localNodeId;
        this.sharedEventLoop = sharedEventLoop;
        this.connectPool = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                new FlairCacheThreadFactory("flaircache-peer-connect", true));
    }

    void connectAsync(NodeInfo node) {
        if (node.id().equals(localNodeId)) return;
        Connection existing = outgoing.get(node.id());
        if (existing != null && existing.isAlive()) return;
        connectPool.submit(() -> doConnect(node));
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

    void disconnectPeer(UUID nodeId) {
        Connection conn = outgoing.remove(nodeId);
        if (conn != null) {
            conn.close();
        }
    }

    void shutdown() {
        connectPool.shutdownNow();
        for (Connection conn : outgoing.values()) {
            conn.close();
        }
        outgoing.clear();
    }

    // ── MembershipListener ────────────────────────────────────────────────────

    @Override
    public void onJoin(NodeInfo node) {
        connectAsync(node);
        log.fine("Peer joined, initiating connection: " + node.addressString());
    }

    @Override
    public void onSuspect(NodeInfo node) {
        // Keep the connection — peer may recover
    }

    @Override
    public void onRecover(NodeInfo node) {
        // Single lookup — avoids TOCTOU between containsKey() and get()
        Connection existing = outgoing.get(node.id());
        if (existing == null || !existing.isAlive()) {
            connectAsync(node);
        }
    }

    @Override
    public void onLeave(NodeInfo node) {
        disconnectPeer(node.id());
        log.fine("Peer left, closed connection: " + node.id());
    }

    @Override
    public void onDead(NodeInfo node) {
        disconnectPeer(node.id());
        log.fine("Peer declared dead, closed connection: " + node.id());
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
                log.fine("Connected to peer " + node.id() + " at " + node.addressString());
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to connect to peer " + node.addressString(), e);
        }
    }
}
