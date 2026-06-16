package com.simplj.flair.cache.replication;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class AckTrackerTest {

    // ── track + onAck ─────────────────────────────────────────────────────────

    @Test
    void single_ack_required_completes_future() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(1L, 1);
            tracker.track(pw);
            tracker.onAck(1L);
            assertTrue(pw.future.isDone());
            assertFalse(pw.future.isCompletedExceptionally());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void quorum_two_required_does_not_complete_on_first_ack() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(2L, 2);
            tracker.track(pw);
            tracker.onAck(2L);
            assertFalse(pw.future.isDone(), "must not complete before required count");
            tracker.onAck(2L);
            assertTrue(pw.future.isDone());
            assertFalse(pw.future.isCompletedExceptionally());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void quorum_three_required_completes_only_after_third_ack() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(3L, 3);
            tracker.track(pw);
            tracker.onAck(3L);
            assertFalse(pw.future.isDone());
            tracker.onAck(3L);
            assertFalse(pw.future.isDone());
            tracker.onAck(3L);
            assertTrue(pw.future.isDone());
            assertFalse(pw.future.isCompletedExceptionally());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void onAck_unknown_frameId_is_noop() {
        AckTracker tracker = new AckTracker();
        try {
            assertDoesNotThrow(() -> tracker.onAck(999L));
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void duplicate_onAck_does_not_double_complete() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(4L, 1);
            tracker.track(pw);
            tracker.onAck(4L); // completes → removes from map
            tracker.onAck(4L); // no-op: already removed
            assertTrue(pw.future.isDone());
            assertFalse(pw.future.isCompletedExceptionally());
        } finally {
            tracker.shutdown();
        }
    }

    // ── cancel ────────────────────────────────────────────────────────────────

    @Test
    void cancel_removes_entry_and_cancels_future() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(5L, 2);
            tracker.track(pw);
            tracker.cancel(5L);
            assertTrue(pw.future.isCancelled());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void cancel_unknown_frameId_is_noop() {
        AckTracker tracker = new AckTracker();
        try {
            assertDoesNotThrow(() -> tracker.cancel(999L));
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void onAck_after_cancel_does_not_complete_cancelled_future() {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = pending(6L, 1);
            tracker.track(pw);
            tracker.cancel(6L);          // removes from map, cancels future
            tracker.onAck(6L);           // no-op: not in map
            assertTrue(pw.future.isCancelled()); // still cancelled, not completed
        } finally {
            tracker.shutdown();
        }
    }

    // ── sweep ─────────────────────────────────────────────────────────────────

    @Test
    void sweep_expires_overdue_entry_with_timeout_exception() throws Exception {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = new PendingWrite(7L, 1, System.currentTimeMillis() - 1); // already expired
            tracker.track(pw);
            Thread.sleep(300); // one sweep cycle is 100ms; wait for it to fire
            assertTrue(pw.future.isCompletedExceptionally());
            ExecutionException ex = assertThrows(ExecutionException.class, pw.future::get);
            assertInstanceOf(ReplicationTimeoutException.class, ex.getCause());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void sweep_does_not_expire_live_entry() throws InterruptedException {
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = new PendingWrite(8L, 1, System.currentTimeMillis() + 60_000);
            tracker.track(pw);
            Thread.sleep(250); // let sweep run once
            assertFalse(pw.future.isDone(), "live entry must not be expired by sweep");
        } finally {
            tracker.shutdown();
        }
    }

    // ── onPeerDead — re-evaluation on peer disconnect ─────────────────────────

    @Test
    void onPeerDead_completes_write_when_remaining_acks_satisfy_quorum() {
        // QUORUM: 2 ACKs required out of 3 peers. One ACK arrives, then one of the remaining
        // two peers dies. With the cluster shrunk, received(1) + dead(1) >= required(2) → success.
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = new PendingWrite(20L, 2, 3, System.currentTimeMillis() + 60_000);
            tracker.track(pw);
            tracker.onAck(20L);
            assertFalse(pw.future.isDone(), "one ACK alone must not satisfy quorum of 2");

            tracker.onPeerDead(java.util.UUID.randomUUID());

            assertTrue(pw.future.isDone(), "peer death must complete the write immediately");
            assertFalse(pw.future.isCompletedExceptionally(), "write must complete successfully");
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void onPeerDead_fails_write_when_quorum_becomes_unreachable() throws Exception {
        // 3 of 4 peers must ACK. No ACKs arrive. After 1 death, 3 peers remain reachable — quorum
        // of 3 still achievable, so the write stays pending. After a 2nd death only 2 reachable
        // remain (max achievable = expected 4 - dead 2 = 2 < required 3) → fail immediately.
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = new PendingWrite(21L, 3, 4, System.currentTimeMillis() + 60_000);
            tracker.track(pw);

            tracker.onPeerDead(java.util.UUID.randomUUID());
            assertFalse(pw.future.isDone(),
                    "after 1 death, 3 of 4 peers reachable — quorum of 3 still achievable");

            tracker.onPeerDead(java.util.UUID.randomUUID());
            assertTrue(pw.future.isDone(),
                    "after 2 deaths only 2 of 4 reachable — quorum of 3 unreachable, must fail now");
            assertTrue(pw.future.isCompletedExceptionally(), "write must fail, not succeed");
            ExecutionException ex = assertThrows(ExecutionException.class, pw.future::get);
            assertInstanceOf(ReplicationTimeoutException.class, ex.getCause());
        } finally {
            tracker.shutdown();
        }
    }

    @Test
    void onPeerDead_strong_consistency_fails_on_first_death() throws Exception {
        // STRONG: all 3 peers must ACK. No ACKs yet. The first death makes it impossible to ever
        // collect 3 ACKs (a dead peer never ACKs) → fail immediately rather than block on timeout.
        AckTracker tracker = new AckTracker();
        try {
            PendingWrite pw = new PendingWrite(22L, 3, 3, System.currentTimeMillis() + 60_000);
            tracker.track(pw);

            tracker.onPeerDead(java.util.UUID.randomUUID());

            assertTrue(pw.future.isDone(), "STRONG write must fail the moment any peer dies");
            assertTrue(pw.future.isCompletedExceptionally());
            ExecutionException ex = assertThrows(ExecutionException.class, pw.future::get);
            assertInstanceOf(ReplicationTimeoutException.class, ex.getCause());
        } finally {
            tracker.shutdown();
        }
    }

    // ── shutdown ──────────────────────────────────────────────────────────────

    @Test
    void shutdown_completes_all_pending_futures_exceptionally() throws Exception {
        AckTracker tracker = new AckTracker();
        PendingWrite pw1 = pending(10L, 2);
        PendingWrite pw2 = pending(11L, 3);
        tracker.track(pw1);
        tracker.track(pw2);
        tracker.shutdown();
        assertTrue(pw1.future.isCompletedExceptionally());
        assertTrue(pw2.future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, pw1.future::get);
        assertInstanceOf(ReplicationTimeoutException.class, ex.getCause());
    }

    @Test
    void shutdown_with_empty_pending_is_noop() {
        AckTracker tracker = new AckTracker();
        assertDoesNotThrow(tracker::shutdown);
    }

    // ── helper ────────────────────────────────────────────────────────────────

    private static PendingWrite pending(long frameId, int required) {
        return new PendingWrite(frameId, required, System.currentTimeMillis() + 60_000);
    }
}
