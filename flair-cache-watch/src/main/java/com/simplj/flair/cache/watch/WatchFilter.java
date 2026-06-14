package com.simplj.flair.cache.watch;

/**
 * Key predicate applied by {@link WatchRegistry} before dispatching an event.
 * Only events whose key satisfies {@link #test} are forwarded to the subscriber.
 */
@FunctionalInterface
public interface WatchFilter<K> {
    boolean test(K key);
}
