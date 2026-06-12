package com.simplj.flair.cache.store;

import com.simplj.flair.cache.hlc.HLCTimestamp;

import java.util.UUID;

public record CacheEntry(
        byte[]       value,
        HLCTimestamp hlc,
        long         expiryEpochMs,
        long         accessEpochMs,
        long         hitCount,
        UUID         originNodeId) {

    public boolean isExpired(long nowMs) {
        return expiryEpochMs > 0 && nowMs > expiryEpochMs;
    }

    public CacheEntry withAccess(long nowMs) {
        return new CacheEntry(value, hlc, expiryEpochMs, nowMs, hitCount + 1, originNodeId);
    }
}
