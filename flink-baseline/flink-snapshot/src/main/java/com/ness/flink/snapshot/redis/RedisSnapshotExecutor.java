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

package com.ness.flink.snapshot.redis;

import com.ness.flink.domain.TimeAware;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.ContextService;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@AllArgsConstructor
public class RedisSnapshotExecutor<T extends TimeAware> implements SnapshotAwareRedisExecutor<T> {

    private static final long serialVersionUID = -6536967976207067864L;
    private static final int EXPIRE_AFTER_DAYS = 2;

    private final SnapshotMapper<T> snapshotMapper;

    @Override
    public void execute(@NonNull T data, ContextService contextService, @NonNull String contextName, StatefulRedisConnection<byte[], byte[]> connection) throws IOException {
        ContextMetadata ctx = contextService.generate(data, contextName);
        long contextId = ctx.getId();
        long expireAt = expireAt();
        RedisCommands<byte[], byte[]> commands = connection.sync();
        // write data to Redis in one Transaction
        commands.multi();

        commands.psetex(snapshotMapper.buildKey(data, ctx).getBytes(), expireAt, snapshotMapper.getValueFromData(data).getBytes());
        byte[] windowBytes = String.valueOf(contextId).getBytes();
        byte[] indexKey = snapshotMapper.buildSnapshotIndexKey(ctx).getBytes();
        commands.zadd(indexKey, new ZAddArgs(), contextId, windowBytes);
        TransactionResult result = commands.exec();
        if (result.wasDiscarded()) {
            throw new IOException("Transaction was aborted for item: " + data);
        }
        log.debug("W{} data has been written to Redis: {}", contextId, data);

    }

    protected long expireAt() {
        long now = System.currentTimeMillis();
        long future = LocalDate.now(ZoneOffset.UTC).plusDays(EXPIRE_AFTER_DAYS).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        return future - now;
    }
}
