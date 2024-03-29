/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.example.pipeline.snapshot;

import com.ness.flink.storage.cache.EntityTypeEnum;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SnapshotSourceFactory {

    public static InterestRatesLoader buildInterestRatesLoader(EntityTypeEnum entityTypeEnum) {
        switch (entityTypeEnum) {
            case MEM_CACHE_WITH_INDEX_SUPPORT_ONLY:
                return new InterestRatesRedisImpl();
            case STORAGE_ONLY:
                throw new UnsupportedOperationException("S3 implementation required");
            default:
                throw new UnsupportedOperationException(String.format("Unsupported snapshot delegate: %s", entityTypeEnum));
        }
    }

}
