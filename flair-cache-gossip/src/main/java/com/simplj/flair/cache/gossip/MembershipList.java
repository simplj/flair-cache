package com.simplj.flair.cache.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class MembershipList {

    // Lock-free snapshot reads via COWAL; O(1) lookup via index map
    private final CopyOnWriteArrayList<NodeInfo>    list       = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<UUID, NodeInfo> index      = new ConcurrentHashMap<>();
    // Tombstone: records the last-known incarnation of each removed node to block stale ghost re-adds
    private final ConcurrentHashMap<UUID, Long>     tombstones = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();

    /**
     * Attempts to add or update the node. Returns {@code true} if membership actually changed
     * (new insert or dominant update accepted), {@code false} if rejected (existing state
     * dominates, or a tombstone blocks a stale re-add of a previously-removed node).
     */
    boolean addOrUpdate(NodeInfo incoming) {
        long now = System.currentTimeMillis();
        NodeInfo stamped = incoming.withLastSeen(now);
        synchronized (writeLock) {
            NodeInfo existing = index.get(incoming.id());
            if (existing == null) {
                Long tombstone = tombstones.get(incoming.id());
                if (tombstone != null && incoming.incarnation() <= tombstone) {
                    return false;  // stale re-add of a previously-dead/left node
                }
                tombstones.remove(incoming.id());
                index.put(incoming.id(), stamped);
                list.add(stamped);
                return true;
            } else if (dominates(stamped, existing)) {
                index.put(incoming.id(), stamped);
                int idx = indexInList(incoming.id());
                if (idx >= 0) list.set(idx, stamped);
                return true;
            }
            return false;
        }
    }

    /**
     * Removes the node unconditionally. Returns {@code true} if a node was actually removed,
     * {@code false} if it was already absent. Callers must check the return value before
     * firing death callbacks to avoid double-firing.
     */
    boolean remove(UUID nodeId) {
        synchronized (writeLock) {
            NodeInfo removed = index.remove(nodeId);
            if (removed != null) {
                tombstones.put(nodeId, removed.incarnation());
            }
            list.removeIf(n -> n.id().equals(nodeId));
            return removed != null;
        }
    }

    /**
     * Removes the node only if it is currently in SUSPECTED state. Returns {@code true} if
     * removal happened. Use this from {@code markDead} to prevent removing a node that was
     * concurrently promoted to ALIVE (higher incarnation) between the suspicion-timeout check
     * and the actual removal.
     */
    boolean removeIfSuspected(UUID nodeId) {
        synchronized (writeLock) {
            NodeInfo existing = index.get(nodeId);
            if (existing == null || existing.status() != NodeStatus.SUSPECTED) return false;
            index.remove(nodeId);
            tombstones.put(nodeId, existing.incarnation());
            list.removeIf(n -> n.id().equals(nodeId));
            return true;
        }
    }

    /**
     * Clears SUSPECTED status for a node in-place, restoring it to ALIVE at the same incarnation.
     * Used by {@code handlePong} as a first-party liveness signal that bypasses the dominance
     * check (ALIVE at the same incarnation never dominates SUSPECTED via the normal path).
     * Returns {@code true} if the node was found in SUSPECTED state and cleared.
     */
    boolean clearSuspicion(UUID nodeId) {
        synchronized (writeLock) {
            NodeInfo existing = index.get(nodeId);
            if (existing == null || existing.status() != NodeStatus.SUSPECTED) return false;
            NodeInfo alive = new NodeInfo(existing.id(), existing.address(), existing.port(),
                    NodeStatus.ALIVE, existing.incarnation(), System.currentTimeMillis());
            index.put(nodeId, alive);
            int idx = indexInList(nodeId);
            if (idx >= 0) list.set(idx, alive);
            return true;
        }
    }

    /**
     * Records a tombstone for {@code nodeId} at {@code inc}, preventing ghost re-admission at
     * equal or lower incarnation. Used when a DEAD delta arrives for a node never seen locally —
     * no entry exists to remove, but we still need to block a subsequent stale ALIVE re-add.
     */
    void writeTombstone(UUID nodeId, long inc) {
        synchronized (writeLock) {
            Long existing = tombstones.get(nodeId);
            if (existing == null || inc > existing) {
                tombstones.put(nodeId, inc);
            }
        }
    }

    /** Returns the tombstone incarnation for {@code nodeId}, or {@code null} if none. Lock-free. */
    Long getTombstone(UUID nodeId) {
        return tombstones.get(nodeId);
    }

    /** O(1) — lock-free. */
    public Optional<NodeInfo> find(UUID nodeId) {
        return Optional.ofNullable(index.get(nodeId));
    }

    /** Lock-free snapshot — ALIVE members only. */
    public List<NodeInfo> alive() {
        List<NodeInfo> result = new ArrayList<>();
        for (NodeInfo n : list) {
            if (n.status() == NodeStatus.ALIVE) result.add(n);
        }
        return Collections.unmodifiableList(result);
    }

    /** Lock-free snapshot — SUSPECTED members only. */
    public List<NodeInfo> suspected() {
        List<NodeInfo> result = new ArrayList<>();
        for (NodeInfo n : list) {
            if (n.status() == NodeStatus.SUSPECTED) result.add(n);
        }
        return Collections.unmodifiableList(result);
    }

    /** Lock-free snapshot — all known members. */
    public List<NodeInfo> all() {
        return Collections.unmodifiableList(new ArrayList<>(list));
    }

    public int size() { return list.size(); }

    /** Returns up to {@code count} random ALIVE members, excluding {@code excludeId}. */
    List<NodeInfo> randomAlive(int count, UUID excludeId) {
        List<NodeInfo> candidates = new ArrayList<>();
        for (NodeInfo n : list) {
            if (n.status() == NodeStatus.ALIVE && !n.id().equals(excludeId)) candidates.add(n);
        }
        Collections.shuffle(candidates);
        int limit = Math.min(count, candidates.size());
        return limit == candidates.size() ? candidates : new ArrayList<>(candidates.subList(0, limit));
    }

    // Called inside writeLock — safe to scan the list
    private int indexInList(UUID id) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).id().equals(id)) return i;
        }
        return -1;
    }

    private static boolean dominates(NodeInfo incoming, NodeInfo existing) {
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
}
