package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.LWWConflictResolver;
import com.simplj.flair.cache.store.CacheEntry;

public final class LWWResolver implements ConflictResolver {

    public static final LWWResolver INSTANCE = new LWWResolver();

    private LWWResolver() {}

    @Override
    public CacheEntry resolve(CacheEntry existing, CacheEntry incoming) {
        return LWWConflictResolver.shouldReplace(
                existing.hlc(), existing.originNodeId(),
                incoming.hlc(), incoming.originNodeId()
        ) ? incoming : existing;
    }
}
