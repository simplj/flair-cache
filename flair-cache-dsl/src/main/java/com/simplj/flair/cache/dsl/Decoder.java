package com.simplj.flair.cache.dsl;

/**
 * Converts a raw value from the data source into a typed query object.
 *
 * <p>For already-typed maps where all values are guaranteed to be the correct type,
 * use {@link #identity()} as a zero-overhead shortcut. For maps that may contain
 * mixed value types, use {@link #typed(Class)} to get an early, descriptive
 * {@link ClassCastException} rather than heap pollution that surfaces far from
 * the decode site.</p>
 */
@FunctionalInterface
public interface Decoder<T> {

    T decode(Object raw);

    /**
     * Fast-path identity decoder. Assumes all source values are already of type {@code T}.
     * If the source contains mixed types, the unchecked cast succeeds silently here and
     * a {@link ClassCastException} surfaces later at the call site — use
     * {@link #typed(Class)} to catch this early.
     */
    @SuppressWarnings("unchecked")
    static <T> Decoder<T> identity() {
        return raw -> (T) raw;
    }

    /**
     * Type-checking decoder. Verifies that each raw value is an instance of {@code type}
     * before returning it, throwing a descriptive {@link ClassCastException} on mismatch.
     * Prefer this over {@link #identity()} when the source may contain mixed value types.
     */
    static <T> Decoder<T> typed(Class<T> type) {
        return raw -> {
            if (!type.isInstance(raw)) {
                throw new ClassCastException("Expected " + type.getName() + " but got "
                        + (raw == null ? "null" : raw.getClass().getName()));
            }
            return type.cast(raw);
        };
    }
}
