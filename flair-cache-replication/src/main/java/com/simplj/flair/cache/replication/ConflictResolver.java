package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.store.CacheEntry;

@FunctionalInterface
public interface ConflictResolver {
    CacheEntry resolve(CacheEntry existing, CacheEntry incoming);
}
