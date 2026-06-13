package com.simplj.flair.cache.gossip;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class GossipNode {
    private static final Logger log = Logger.getLogger(GossipNode.class.getName());

    private final UUID               nodeId;
    private final InetAddress        bindAddress;
    private final int                bindPort;
    private final GossipConfig       config;

    private final MembershipList    members;
    private final PiggybackQueue    piggyback;
    private final IncarnationClock  incarnation;
    private final FailureDetector   failureDetector;
    // CopyOnWriteArrayList: addMembershipListener() may race with tick/receive threads calling onXxx()
    private final java.util.concurrent.CopyOnWriteArrayList<MembershipListener> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    // For PING_REQ forwarding: targetId → set of original requesters.
    // A Set (not single address) because two nodes can simultaneously ask us to probe the same target.
    private final ConcurrentHashMap<UUID, Set<InetSocketAddress>> indirectFor   = new ConcurrentHashMap<>();
    // Tracks when each SUSPECTED node was first marked; used to enforce suspicionTimeout.
    private final ConcurrentHashMap<UUID, Long>                   suspectedSince = new ConcurrentHashMap<>();

    private DatagramSocket   socket;
    private int              localPort;  // cached once in start(); never re-query a potentially-closed socket
    private GossipReceiver   receiver;
    private GossipTick       tick;
    private volatile boolean running;

    private GossipNode(Builder b) throws IOException {
        this.nodeId      = b.nodeId;
        this.bindAddress = InetAddress.getByName(b.bindAddress);
        this.bindPort    = b.bindPort;
        this.config      = GossipConfig.builder()
                .tickIntervalMs(b.tickIntervalMs)
                .probeTimeoutMs(b.probeTimeoutMs)
                .indirectTimeoutMs(b.indirectTimeoutMs)
                .suspicionTimeoutMs(b.suspicionTimeoutMs)
                .fanout(b.fanout)
                .seedPeers(b.seedPeers)
                .build();
        if (b.listener != null) this.listeners.add(b.listener);
        this.members         = new MembershipList();
        this.piggyback       = new PiggybackQueue();
        this.incarnation     = new IncarnationClock();
        this.failureDetector = new FailureDetector();
    }

    public void start() throws IOException {
        socket    = new DatagramSocket(new InetSocketAddress(bindAddress, bindPort));
        localPort = socket.getLocalPort();  // cache immediately; used by handleSelfUpdate during shutdown races
        running   = true;

        NodeInfo self = new NodeInfo(nodeId, bindAddress, localPort,
                NodeStatus.ALIVE, incarnation.current(), System.currentTimeMillis());
        members.addOrUpdate(self);

        receiver = new GossipReceiver(socket, this);
        receiver.start();

        tick = new GossipTick(this, config.tickIntervalMs);
        tick.start();

        for (String seed : config.seedPeers) {
            trySendJoin(seed);
        }

        log.info("GossipNode started: id=" + nodeId + " on port " + localPort);
    }

    public void shutdown() {
        if (!running) return;
        running = false;

        GossipMessage leave = GossipMessage.leave(nodeId, incarnation.current());
        for (NodeInfo peer : members.alive()) {
            if (!peer.id().equals(nodeId)) {
                try { sendTo(leave, peer.address(), peer.port()); }
                catch (IOException e) { log.log(Level.FINE, "LEAVE send failed to " + peer.addressString(), e); }
            }
        }

        tick.stop();
        receiver.stop();
        log.info("GossipNode stopped: id=" + nodeId);
    }

    /** Stops the node without broadcasting LEAVE — simulates a crash for testing. */
    void simulateFail() {
        running = false;
        tick.stop();
        receiver.stop();
    }

    /**
     * Marks a peer SUSPECTED in both the membership list and the piggyback queue,
     * then immediately sends a PING to that peer carrying the SUSPECTED delta.
     * The peer receives it, refutes (increments incarnation, broadcasts ALIVE),
     * and sends PONG back — exercising the real refutation path.
     * Only for integration testing.
     */
    void injectSuspectForTest(UUID peerId) {
        members.find(peerId).ifPresent(n -> {
            NodeInfo suspected = n.withStatus(NodeStatus.SUSPECTED);
            members.addOrUpdate(suspected);
            piggyback.add(suspected);
            suspectedSince.putIfAbsent(peerId, System.currentTimeMillis());
            notifyListeners(l -> l.onSuspect(suspected));
            if (running && socket != null) {
                GossipMessage ping = GossipMessage.ping(nodeId, incarnation.current(), List.of(suspected));
                try { sendTo(ping, n.address(), n.port()); }
                catch (IOException ignored) {}
            }
        });
    }

    public MembershipList members() { return members; }

    public int localPort() { return localPort; }

    // ── Tick ─────────────────────────────────────────────────────────────────

    void onTick() {
        if (!running) return;
        long now = System.currentTimeMillis();
        checkProbeTimeouts(now);
        checkSuspicionTimeouts(now);
        sendPings();
    }

    private void sendPings() {
        int fanout = Math.min(config.fanout, Math.max(0, members.size() - 1));
        List<NodeInfo> targets = members.randomAlive(fanout, nodeId);
        // Drain once for the whole fanout round — draining per-target would increment each
        // entry's transmitCount on the first drain and evict everything before the second
        // target runs (maxTransmits=1 in small clusters), starving all subsequent targets.
        List<NodeInfo> deltas = piggyback.drain(GossipProtocol.MAX_PIGGYBACKED, members.size());

        for (NodeInfo target : targets) {
            if (failureDetector.isProbing(target.id())) continue;
            failureDetector.startProbe(target.id());
            GossipMessage ping = GossipMessage.ping(nodeId, incarnation.current(), deltas);
            try {
                sendTo(ping, target.address(), target.port());
                if (log.isLoggable(Level.FINEST)) {
                    log.finest("PING -> " + target.addressString());
                }
            } catch (IOException e) {
                log.log(Level.WARNING, "PING failed to " + target.addressString(), e);
                failureDetector.clear(target.id());
            }
        }
    }

    private void checkProbeTimeouts(long now) {
        for (NodeInfo peer : members.all()) {
            if (peer.id().equals(nodeId)) continue;
            FailureDetector.ProbeEntry probe = failureDetector.get(peer.id());
            if (probe == null) continue;

            if (probe.state == FailureDetector.ProbeState.PROBING) {
                if (now >= probe.probeSentMs + config.probeTimeoutMs) {
                    sendIndirectProbe(peer);
                }
            } else if (probe.state == FailureDetector.ProbeState.INDIRECT_PROBING) {
                if (now >= probe.probeSentMs + config.indirectTimeoutMs) {
                    markSuspected(peer);
                }
            }
        }
    }

    private void checkSuspicionTimeouts(long now) {
        for (NodeInfo peer : members.suspected()) {
            if (peer.id().equals(nodeId)) continue;
            Long since = suspectedSince.get(peer.id());
            if (since != null && now - since >= config.suspicionTimeoutMs) {
                markDead(peer);
            }
        }
    }

    private void sendIndirectProbe(NodeInfo target) {
        List<NodeInfo> indirects = members.randomAlive(2, nodeId);
        indirects.removeIf(n -> n.id().equals(target.id()));

        if (indirects.isEmpty()) {
            // No intermediaries available (e.g., 2-node cluster). Advance to INDIRECT_PROBING
            // so the full indirectTimeoutMs still elapses before marking SUSPECTED — skipping
            // straight to markSuspected on the first direct timeout violates SWIM.
            failureDetector.markIndirect(target.id());
            return;
        }

        failureDetector.markIndirect(target.id());
        GossipMessage pingReq = GossipMessage.pingReq(nodeId, incarnation.current(), target.id(), List.of());
        for (NodeInfo indirect : indirects) {
            try {
                sendTo(pingReq, indirect.address(), indirect.port());
                if (log.isLoggable(Level.FINEST)) {
                    log.finest("PING_REQ -> " + indirect.addressString() + " for " + target.id());
                }
            } catch (IOException e) {
                log.log(Level.WARNING, "PING_REQ failed to " + indirect.addressString(), e);
            }
        }
    }

    // ── Message handling ──────────────────────────────────────────────────────

    void onMessage(GossipMessage msg, InetAddress senderAddr, int senderPort) {
        if (!running) return;
        applyPiggybacked(msg.piggybacked);

        switch (msg.type) {
            case PING:     handlePing(msg, senderAddr, senderPort);    break;
            case PONG:     handlePong(msg, senderAddr, senderPort);    break;
            case PING_REQ: handlePingReq(msg, senderAddr, senderPort); break;
            case JOIN:     handleJoin(msg, senderAddr, senderPort);    break;
            case JOIN_ACK: handleJoinAck(msg);                         break;
            case LEAVE:    handleLeave(msg);                           break;
        }
    }

    private void handlePing(GossipMessage msg, InetAddress senderAddr, int senderPort) {
        ensureMember(msg.senderId, senderAddr, senderPort, msg.incarnation);
        List<NodeInfo> deltas = piggyback.drain(GossipProtocol.MAX_PIGGYBACKED, members.size());
        GossipMessage  pong   = GossipMessage.pong(nodeId, incarnation.current(), deltas);
        try {
            sendTo(pong, senderAddr, senderPort);
        } catch (IOException e) {
            log.log(Level.WARNING, "PONG send failed to " + senderAddr + ":" + senderPort, e);
        }
    }

    private void handlePong(GossipMessage msg, InetAddress senderAddr, int senderPort) {
        failureDetector.onPong(msg.senderId);

        // Direct PONG is first-party liveness proof — clear SUSPECTED without incarnation increment.
        // ALIVE(same-inc) never dominates SUSPECTED via the normal dominance path, so we need
        // clearSuspicion() to bypass that check.
        members.find(msg.senderId).ifPresent(n -> {
            if (n.status() == NodeStatus.SUSPECTED) {
                boolean recovered = members.clearSuspicion(msg.senderId);
                if (recovered) {
                    suspectedSince.remove(msg.senderId);
                    NodeInfo alive = members.find(msg.senderId).orElse(n.withStatus(NodeStatus.ALIVE));
                    piggyback.add(alive);
                    notifyListeners(l -> l.onRecover(alive));
                }
            } else {
                members.addOrUpdate(n.withLastSeen(System.currentTimeMillis()));
            }
        });

        // Forward PONG to all nodes that originally asked us to probe this target via PING_REQ.
        // senderId must be msg.senderId (the actual target's ID), NOT nodeId (this intermediary) —
        // the requester calls failureDetector.onPong(senderId) and must clear the probe keyed by
        // the target's UUID. Fall back to senderAddr if the target was concurrently evicted.
        Set<InetSocketAddress> requesters = indirectFor.remove(msg.senderId);
        if (requesters != null && !requesters.isEmpty()) {
            NodeInfo liveTarget = members.find(msg.senderId)
                    .orElseGet(() -> new NodeInfo(msg.senderId, senderAddr, senderPort,
                            NodeStatus.ALIVE, msg.incarnation, System.currentTimeMillis()));
            GossipMessage forwarded = GossipMessage.pong(msg.senderId, incarnation.current(), List.of(liveTarget));
            for (InetSocketAddress requester : requesters) {
                try {
                    sendTo(forwarded, requester.getAddress(), requester.getPort());
                } catch (IOException e) {
                    log.log(Level.FINE, "Forward PONG failed to " + requester, e);
                }
            }
        }
    }

    private void handlePingReq(GossipMessage msg, InetAddress senderAddr, int senderPort) {
        if (msg.targetId == null) return;
        Optional<NodeInfo> targetOpt = members.find(msg.targetId);
        if (targetOpt.isEmpty()) return;
        NodeInfo target = targetOpt.get();

        // Use compute (not computeIfAbsent + add) to make the Set creation and requester insertion
        // atomic — prevents the TOCTOU race where handlePong removes the Set between computeIfAbsent
        // returning it and .add(requester) executing.
        InetSocketAddress requester = new InetSocketAddress(senderAddr, senderPort);
        indirectFor.compute(msg.targetId, (k, existing) -> {
            Set<InetSocketAddress> s = (existing != null) ? existing : ConcurrentHashMap.newKeySet();
            s.add(requester);
            return s;
        });
        GossipMessage ping = GossipMessage.ping(nodeId, incarnation.current(), List.of());
        try {
            sendTo(ping, target.address(), target.port());
        } catch (IOException e) {
            log.log(Level.WARNING, "PING_REQ relay failed for " + target.addressString(), e);
            Set<InetSocketAddress> set = indirectFor.get(msg.targetId);
            if (set != null) set.remove(requester);
        }
    }

    private void handleJoin(GossipMessage msg, InetAddress senderAddr, int senderPort) {
        // Capture pre-mutation state for callback selection
        boolean wasKnown     = members.find(msg.senderId).isPresent();
        boolean wasSuspected = members.find(msg.senderId)
                .filter(n -> n.status() == NodeStatus.SUSPECTED)
                .isPresent();

        if (wasSuspected) {
            // JOIN is first-party liveness proof — clear SUSPECTED regardless of incarnation.
            // The joining node may have restarted at a lower incarnation than the SUSPECTED entry,
            // so the normal dominance path (ALIVE(low) vs SUSPECTED(high)) would reject it.
            // Removing the entry writes a tombstone at the SUSPECTED incarnation; effectiveInc
            // below will then bump the joiner past that tombstone.
            members.remove(msg.senderId);
            suspectedSince.remove(msg.senderId);
            failureDetector.clear(msg.senderId);
            indirectFor.remove(msg.senderId);
        }

        // If the joining incarnation is at or below the tombstone (from this or a prior life),
        // bump to tombstone+1 so stale DEAD(N) deltas from a previous life cannot immediately
        // re-evict the freshly re-joining node via dominatesGossip(DEAD(N), ALIVE(N))=true.
        Long tombstone = members.getTombstone(msg.senderId);
        long effectiveInc = (tombstone != null && msg.incarnation <= tombstone)
                ? tombstone + 1
                : msg.incarnation;

        NodeInfo joiner = new NodeInfo(msg.senderId, senderAddr, senderPort,
                NodeStatus.ALIVE, effectiveInc, System.currentTimeMillis());
        boolean accepted = members.addOrUpdate(joiner);
        if (accepted) {
            piggyback.add(joiner);
            if (wasSuspected) {
                notifyListeners(l -> l.onRecover(joiner));
            } else if (!wasKnown) {
                notifyListeners(l -> l.onJoin(joiner));
            }
        }

        // Always reply with membership snapshot so the joiner can bootstrap cluster state
        List<NodeInfo> all      = members.all();
        int            limit    = Math.min(all.size(), GossipProtocol.MAX_PIGGYBACKED);
        List<NodeInfo> snapshot = all.size() <= limit ? all : all.subList(0, limit);
        GossipMessage  ack      = GossipMessage.joinAck(nodeId, incarnation.current(), snapshot);
        try {
            sendTo(ack, senderAddr, senderPort);
        } catch (IOException e) {
            log.log(Level.WARNING, "JOIN_ACK send failed to " + senderAddr + ":" + senderPort, e);
        }
    }

    private void handleJoinAck(GossipMessage msg) {
        for (NodeInfo member : msg.piggybacked) {
            if (member.id().equals(nodeId)) continue;
            boolean isNew    = members.find(member.id()).isEmpty();
            boolean accepted = members.addOrUpdate(member);
            if (accepted) {
                piggyback.add(member);  // schedule for epidemic re-dissemination from this joiner
            }
            if (isNew && accepted) notifyListeners(l -> l.onJoin(member));
        }
    }

    private void handleLeave(GossipMessage msg) {
        members.find(msg.senderId).ifPresent(n -> {
            boolean removed = members.remove(msg.senderId);
            failureDetector.clear(msg.senderId);
            suspectedSince.remove(msg.senderId);
            indirectFor.remove(msg.senderId);
            if (removed) {
                piggyback.add(n.withStatus(NodeStatus.DEAD));  // epidemic dissemination of graceful leave
                notifyListeners(l -> l.onLeave(n));
            }
        });
        log.fine("Node left: " + msg.senderId);
    }

    // ── State transitions ─────────────────────────────────────────────────────

    private void applyPiggybacked(List<NodeInfo> deltas) {
        for (NodeInfo delta : deltas) {
            if (delta.id().equals(nodeId)) {
                handleSelfUpdate(delta);
                continue;
            }

            Optional<NodeInfo> existingOpt = members.find(delta.id());
            if (existingOpt.isEmpty()) {
                if (delta.status() == NodeStatus.ALIVE) {
                    // Unknown node: tombstone check inside addOrUpdate blocks stale ghost re-adds
                    boolean added = members.addOrUpdate(delta);
                    if (added) {
                        piggyback.add(delta);
                        notifyListeners(l -> l.onJoin(delta));
                    }
                } else if (delta.status() == NodeStatus.DEAD) {
                    // Write tombstone even for unknown nodes — without it, a stale ALIVE(higher-inc)
                    // arriving later would pass addOrUpdate with no barrier and ghost the dead node back.
                    members.writeTombstone(delta.id(), delta.incarnation());
                    piggyback.add(delta);
                }
                continue;
            }

            NodeInfo   current    = existingOpt.get();
            NodeStatus prevStatus = current.status();
            NodeStatus newStatus  = delta.status();

            // Reject anything that doesn't dominate current state — same rule as MembershipList.
            // This prevents stale ALIVE(same-inc) from triggering callbacks for a SUSPECTED node.
            if (!dominatesGossip(delta, current)) continue;

            if (newStatus == NodeStatus.DEAD) {
                // Write DEAD directly to remove, not via addOrUpdate, to avoid a racy window
                // where DEAD is visible in the live list between addOrUpdate and remove.
                boolean removed = members.remove(delta.id());
                failureDetector.clear(delta.id());
                suspectedSince.remove(delta.id());
                indirectFor.remove(delta.id());
                if (removed) {
                    piggyback.add(delta);
                    notifyListeners(l -> l.onDead(delta));
                }
            } else {
                // Now that dominance is confirmed, clear probe for fresh ALIVE (not before the check —
                // a stale ALIVE at lower incarnation must not clear an active probe).
                if (newStatus == NodeStatus.ALIVE) {
                    failureDetector.onPong(delta.id());
                }

                boolean changed = members.addOrUpdate(delta);
                if (!changed) continue;  // concurrent update already applied; skip callbacks
                piggyback.add(delta);

                if (newStatus == NodeStatus.SUSPECTED && prevStatus == NodeStatus.ALIVE) {
                    // Update probe state so checkProbeTimeouts won't escalate and fire onSuspect again
                    failureDetector.markSuspected(delta.id());
                    suspectedSince.putIfAbsent(delta.id(), System.currentTimeMillis());
                    notifyListeners(l -> l.onSuspect(delta));
                } else if (newStatus == NodeStatus.ALIVE && prevStatus == NodeStatus.SUSPECTED) {
                    suspectedSince.remove(delta.id());
                    failureDetector.clear(delta.id());
                    notifyListeners(l -> l.onRecover(delta));
                }
            }
        }
    }

    private void handleSelfUpdate(NodeInfo delta) {
        // Refutation: we see ourselves as not-ALIVE → increment incarnation and re-broadcast.
        // Guard on delta.incarnation() >= current so a stale SUSPECTED(old-inc) delivered late
        // does not cause an unnecessary incarnation increment and piggyback queue churn.
        if (delta.status() != NodeStatus.ALIVE && delta.incarnation() >= incarnation.current()) {
            long newInc = incarnation.increment();
            // Use cached localPort — socket may be closed during a shutdown race
            NodeInfo self = new NodeInfo(nodeId, bindAddress, localPort,
                    NodeStatus.ALIVE, newInc, System.currentTimeMillis());
            members.addOrUpdate(self);
            piggyback.add(self);
            log.fine("Refuted SUSPECTED/DEAD for self; incarnation now " + newInc);
        }
    }

    private void markSuspected(NodeInfo peer) {
        NodeInfo suspected = peer.withStatus(NodeStatus.SUSPECTED);
        boolean changed = members.addOrUpdate(suspected);
        if (!changed) {
            // Already SUSPECTED (gossip applied it concurrently); still pin probe state so
            // checkProbeTimeouts doesn't escalate and call onSuspect a second time. Only do
            // this if the node is still SUSPECTED — it may have been promoted to ALIVE at a
            // higher incarnation, in which case clobbering a fresh PROBING entry would be wrong.
            members.find(peer.id())
                   .filter(n -> n.status() == NodeStatus.SUSPECTED)
                   .ifPresent(n -> failureDetector.markSuspected(peer.id()));
            return;
        }
        failureDetector.markSuspected(peer.id());
        suspectedSince.putIfAbsent(peer.id(), System.currentTimeMillis());
        piggyback.add(suspected);
        notifyListeners(l -> l.onSuspect(suspected));
        log.fine("Node suspected: " + peer.id());
    }

    private void markDead(NodeInfo peer) {
        // Use removeIfSuspected: if the node recovered to ALIVE at a higher incarnation between
        // checkSuspicionTimeouts and here, we must not evict it. Only remove if still SUSPECTED.
        boolean removed = members.removeIfSuspected(peer.id());
        failureDetector.clear(peer.id());
        suspectedSince.remove(peer.id());
        indirectFor.remove(peer.id());
        if (removed) {
            NodeInfo dead = peer.withStatus(NodeStatus.DEAD);
            piggyback.add(dead);
            notifyListeners(l -> l.onDead(dead));
            log.fine("Node dead: " + peer.id());
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private void ensureMember(UUID id, InetAddress addr, int port, long inc) {
        if (members.find(id).isEmpty()) {
            NodeInfo n = new NodeInfo(id, addr, port, NodeStatus.ALIVE, inc, System.currentTimeMillis());
            boolean added = members.addOrUpdate(n);
            if (added) {
                piggyback.add(n);  // epidemic-disseminate nodes discovered via direct PING
                notifyListeners(l -> l.onJoin(n));
            }
        }
    }

    private void trySendJoin(String seed) {
        try {
            int         colon = seed.lastIndexOf(':');
            InetAddress addr  = InetAddress.getByName(seed.substring(0, colon));
            int         port  = Integer.parseInt(seed.substring(colon + 1));
            sendTo(GossipMessage.join(nodeId, incarnation.current()), addr, port);
        } catch (Exception e) {
            log.log(Level.WARNING, "JOIN failed to seed " + seed, e);
        }
    }

    private void sendTo(GossipMessage msg, InetAddress addr, int port) throws IOException {
        byte[]         data   = GossipProtocol.encode(msg);
        DatagramPacket packet = new DatagramPacket(data, data.length, addr, port);
        socket.send(packet);
    }

    // Mirrors MembershipList.dominates — determines whether a piggybacked delta should be applied.
    private static boolean dominatesGossip(NodeInfo incoming, NodeInfo existing) {
        if (incoming.incarnation() > existing.incarnation()) return true;
        if (incoming.incarnation() < existing.incarnation()) return false;
        return statusWeight(incoming.status()) > statusWeight(existing.status());
    }

    private static int statusWeight(NodeStatus s) {
        switch (s) {
            case DEAD:      return 2;
            case SUSPECTED: return 1;
            default:        return 0;
        }
    }

    /**
     * Registers an additional membership event listener. Safe to call at any time,
     * including after {@link #start()}. All listeners are notified in registration order.
     */
    public void addMembershipListener(MembershipListener listener) {
        listeners.add(listener);
    }

    private void notifyListeners(java.util.function.Consumer<MembershipListener> action) {
        for (MembershipListener l : listeners) {
            try { action.accept(l); } catch (Exception e) { log.log(Level.WARNING, "MembershipListener threw", e); }
        }
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private UUID               nodeId             = UUID.randomUUID();
        private String             bindAddress        = "0.0.0.0";
        private int                bindPort           = 7891;
        private List<String>       seedPeers          = new ArrayList<>();
        private long               tickIntervalMs     = 500;
        private long               probeTimeoutMs     = 2000;
        private long               indirectTimeoutMs  = 4000;
        private long               suspicionTimeoutMs = 10000;
        private int                fanout             = 3;
        private MembershipListener listener           = null;

        public Builder nodeId(UUID v)                 { nodeId = v;             return this; }
        public Builder bindAddress(String v)          { bindAddress = v;        return this; }
        public Builder bindPort(int v)                { bindPort = v;           return this; }
        public Builder seedPeers(List<String> v)      { seedPeers = new ArrayList<>(v); return this; }
        public Builder tickIntervalMs(long v)         { tickIntervalMs = v;     return this; }
        public Builder probeTimeoutMs(long v)         { probeTimeoutMs = v;     return this; }
        public Builder indirectTimeoutMs(long v)      { indirectTimeoutMs = v;  return this; }
        public Builder suspicionTimeoutMs(long v)     { suspicionTimeoutMs = v; return this; }
        public Builder fanout(int v)                  { fanout = v;             return this; }
        public Builder listener(MembershipListener v) { listener = v;           return this; }

        public GossipNode build() throws IOException {
            return new GossipNode(this);
        }
    }
}
