/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.snapshot;

import com.ness.flink.config.properties.RedisProperties;
import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.domain.TimeAware;
import com.ness.flink.snapshot.context.properties.ContextProperties;
import com.ness.flink.snapshot.context.ContextService;
import com.ness.flink.snapshot.context.ContextServiceProvider;
import com.ness.flink.snapshot.redis.RedisSnapshotExecutor;
import com.ness.flink.storage.cache.EntityTypeEnum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.AllArgsConstructor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class SnapshotSink<T extends TimeAware> implements Sink<T> {
    private static final long serialVersionUID = 6805501266870217945L;

    private final SnapshotMapper<T> snapshotMapper;
    private final EntityTypeEnum entityTypeEnum;
    private final ParameterTool parameterTool;

    @Override
    public SinkWriter<T> createWriter(InitContext initContext) {
        return new RedisWriter(parameterTool);
    }

    class RedisWriter implements SinkWriter<T> {
        private final ContextService contextService;
        private RedisClient redisClient;
        private StatefulRedisConnection<byte[], byte[]> connect;

        public RedisWriter(ParameterTool parameterTool) {
            RedisProperties redisProperties = RedisProperties.from(parameterTool);
            ContextProperties properties = ContextProperties.from(parameterTool);
            WatermarkProperties watermarkProperties = WatermarkProperties.from(parameterTool);
            contextService = ContextServiceProvider.create(properties, watermarkProperties);
            contextService.init();
            if (Objects.requireNonNull(entityTypeEnum) == EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY) {
                redisClient = RedisClient.create(redisProperties.build());
                connect = redisClient.connect(new ByteArrayCodec());
            }
        }

        @Override
        public void write(T element, Context context) throws IOException {
            final String contextName = element.getClass().getSimpleName();
            switch (entityTypeEnum) {
                case MEM_CACHE_WITH_INDEX_SUPPORT_ONLY:
                    new RedisSnapshotExecutor<>(snapshotMapper).execute(element, contextService, contextName, connect);
                    return;
                case MEM_CACHE_ONLY:
                    throw new UnsupportedOperationException("Implementation required");
                default:
                    throw new IllegalArgumentException(String.format("Unsupported mode: %s", entityTypeEnum));
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            connect.flushCommands();
        }

        @Override
        public void close() throws Exception {
            if (connect != null) {
                connect.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            if (contextService != null) {
                contextService.close();
            }
        }
    }
}
