package org.apache.doris.common.util;

import com.github.benmanes.caffeine.cache.CacheLoader;

import java.util.concurrent.Semaphore;

public abstract class CacheLoaderWithBatchRefresh<K, V> implements CacheLoader<K, V> {
    private final Semaphore semaphore;

    public CacheLoaderWithBatchRefresh(int cnt) {
        this.semaphore = new Semaphore(cnt);
    }

    @Override
    public V reload(K key, V oldValue) throws Exception {
        try {
            if (semaphore.tryAcquire()) {
                return CacheLoader.super.reload(key, oldValue);
            } else {
                return oldValue;
            }
        } finally {
            semaphore.release();
        }
    }
}
