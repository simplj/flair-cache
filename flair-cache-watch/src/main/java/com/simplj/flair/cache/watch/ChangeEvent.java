package com.simplj.flair.cache.watch;

/**
 * Discriminated union of mutation events emitted by {@link WatchRegistry}.
 *
 * <p>Pattern-match with {@code instanceof} (Java 16+) or chain of type checks:</p>
 * <pre>{@code
 * if (event instanceof ChangeEvent.PutEvent<K,V> put) { ... }
 * }</pre>
 *
 * <p>All three subtypes expose a uniform {@link #source()} accessor. For {@link PutEvent}
 * and {@link DeleteEvent} the source is set by the caller. For {@link ExpireEvent} it is
 * always {@link Source#LOCAL} — TTL expiry is triggered locally and is never replicated
 * directly; the default implementation on this interface returns {@code LOCAL}.</p>
 */
public sealed interface ChangeEvent<K, V>
        permits ChangeEvent.PutEvent, ChangeEvent.DeleteEvent, ChangeEvent.ExpireEvent {

    /** Indicates whether a mutation originated locally or arrived via replication. */
    enum Source { LOCAL, REPLICATED }

    /**
     * Returns the origin of this event.
     * {@link PutEvent} and {@link DeleteEvent} override this with their stored component.
     * {@link ExpireEvent} inherits this default, which always returns {@link Source#LOCAL}.
     */
    default Source source() {
        return Source.LOCAL;
    }

    record PutEvent<K, V>(
            K      key,
            V      newValue,
            V      oldValue,   // null if this is a new entry
            Source source
    ) implements ChangeEvent<K, V> {}

    record DeleteEvent<K, V>(
            K      key,
            V      lastValue,
            Source source
    ) implements ChangeEvent<K, V> {}

    /**
     * TTL-expiry event. The source is always {@link Source#LOCAL} — expiry is driven by
     * the local expiry sweep and is never replicated directly. See {@link #source()}.
     */
    record ExpireEvent<K, V>(
            K key,
            V lastValue
    ) implements ChangeEvent<K, V> {}
}
