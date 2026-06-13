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
