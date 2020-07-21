/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.snapshot;

import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SnapshotSourceFactory {

    public static InterestRatesLoader buildInterestRatesLoader(EntityTypeEnum entityTypeEnum,
                                                               RedisProperties redisProperties) {
        switch (entityTypeEnum) {
            case MEM_CACHE_WITH_INDEX_SUPPORT_ONLY:
                return new InterestRatesRedisImpl(redisProperties);
            case STORAGE_ONLY:
                throw new UnsupportedOperationException("S3 implementation required");
            default:
                throw new UnsupportedOperationException(String.format("Unsupported snapshot delegate: %s", entityTypeEnum));
        }
    }

}
