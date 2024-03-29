package com.jasmine.processors.merger;

import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;

/**
 * Cache Provider class
 */
class CacheProvider {
    private DistributedMapCacheClient cache;

    private CacheProvider() {}

    public DistributedMapCacheClient getCache() {
        return cache;
    }

    public static class CacheProviderBuilder {
        private CacheProvider cacheProvider;

        public CacheProviderBuilder() {
            this.cacheProvider = new CacheProvider();
        }

        public CacheProviderBuilder setCache(DistributedMapCacheClient cache) {
            this.cacheProvider.cache = cache;
            return this;
        }

        public CacheProvider build() {
            return this.cacheProvider;
        }
    }
}
