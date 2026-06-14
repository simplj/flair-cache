package com.simplj.flair.cache.watch;

import java.util.concurrent.TimeUnit;

/**
 * {@link WatchHandle} backed by a {@link WatchSubscription}.
 * The owning {@link WatchRegistry} is notified on cancel so it can remove
 * the subscription from its dispatch list.
 */
final class WatchHandleImpl<K, V> implements WatchHandle {

    private final WatchSubscription<K, V> subscription;
    private final WatchRegistry<K, V>     registry;

    WatchHandleImpl(WatchSubscription<K, V> subscription, WatchRegistry<K, V> registry) {
        this.subscription = subscription;
        this.registry     = registry;
    }

    @Override
    public String subscriptionId() {
        return subscription.subscriptionId;
    }

    @Override
    public boolean isActive() {
        return subscription.isActive();
    }

    @Override
    public void cancel() {
        subscription.cancel();
        registry.remove(subscription.subscriptionId);
    }

    @Override
    public long eventsReceived() {
        return subscription.eventsReceived();
    }

    @Override
    public long eventsDropped() {
        return subscription.eventsDropped();
    }

    @Override
    public long eventsSkipped() {
        return subscription.eventsSkipped();
    }

    @Override
    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return subscription.awaitDone(timeout, unit);
    }
}
