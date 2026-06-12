package com.simplj.flair.cache.store;

import java.util.Arrays;

final class ByteArrayKey {

    final byte[] data;
    private final int hash;

    ByteArrayKey(byte[] data) {
        this.data = data;
        this.hash = Arrays.hashCode(data);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteArrayKey)) return false;
        ByteArrayKey other = (ByteArrayKey) o;
        // Fast hash pre-check before the full array comparison
        return hash == other.hash && Arrays.equals(data, other.data);
    }
}
