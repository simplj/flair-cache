package com.simplj.flair.cache.watch;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-subscription bounded async dispatch queue.
 * Events are consumed by a dedicated drain thread named {@code flaircache-watch-{subscriptionId}}.
 * When the queue is full the oldest event is dropped and a WARNING is logged.
 */
final class DispatchQueue<K, V> {

    static final int CAPACITY = 1024;

    private static final Logger log = Logger.getLogger(DispatchQueue.class.getName());

    private final ArrayBlockingQueue<ChangeEvent<K, V>> queue = new ArrayBlockingQueue<>(CAPACITY);
    private final AtomicBoolean  running       = new AtomicBoolean(true);
    private final CountDownLatch done          = new CountDownLatch(1);
    private final String         subscriptionId;
    private final LongAdder      droppedCounter;
    private final Thread         drainThread;

    DispatchQueue(String subscriptionId, LongAdder droppedCounter,
                  Consumer<ChangeEvent<K, V>> listener) {
        this.subscriptionId = subscriptionId;
        this.droppedCounter = droppedCounter;
        FlairCacheThreadFactory tf =
                new FlairCacheThreadFactory("flaircache-watch-" + subscriptionId);
        this.drainThread = tf.newThread(() -> drain(listener));
        this.drainThread.start();
    }

    /**
     * Offers an event to the queue. If full, evicts the oldest entry and re-offers the new one.
     *
     * <p><strong>Best-effort semantics under concurrency:</strong> between the {@code poll()}
     * that evicts the oldest entry and the second {@code offer()} that inserts the new one,
     * another concurrent producer may claim the freed slot. In that case both the eviction and
     * the re-offer can fail, so "drop oldest" degrades to "drop some event" under high-concurrency
     * flood conditions. The dropped-event counter is incremented accurately in all cases.</p>
     */
    void offer(ChangeEvent<K, V> event) {
        if (!queue.offer(event)) {
            queue.poll();                  // evict oldest
            droppedCounter.increment();
            if (log.isLoggable(Level.WARNING)) {
                log.warning("Watch[" + subscriptionId + "] dispatch queue full — dropped oldest event");
            }
            if (!queue.offer(event)) {     // re-offer; can still fail under high concurrency
                droppedCounter.increment();
                if (log.isLoggable(Level.WARNING)) {
                    log.warning("Watch[" + subscriptionId
                            + "] re-offer after eviction also failed — new event dropped");
                }
            }
        }
    }

    /** Signals the drain thread to stop after draining remaining events. */
    void stop() {
        running.set(false);
        drainThread.interrupt();
    }

    /**
     * Blocks until the drain thread has fully exited (including its post-stop flush), or the
     * timeout elapses.
     *
     * @return {@code true} if the thread exited within the timeout; {@code false} otherwise
     */
    boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    private void drain(Consumer<ChangeEvent<K, V>> listener) {
        try {
            while (running.get()) {
                try {
                    ChangeEvent<K, V> event = queue.poll(50, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        deliver(event, listener);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            // flush anything left after stop()
            ChangeEvent<K, V> event;
            while ((event = queue.poll()) != null) {
                deliver(event, listener);
            }
        } finally {
            done.countDown();
        }
    }

    private void deliver(ChangeEvent<K, V> event, Consumer<ChangeEvent<K, V>> listener) {
        try {
            listener.accept(event);
        } catch (Exception e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING,
                        "Watch[" + subscriptionId + "] listener threw — event dropped", e);
            }
        }
    }
}
