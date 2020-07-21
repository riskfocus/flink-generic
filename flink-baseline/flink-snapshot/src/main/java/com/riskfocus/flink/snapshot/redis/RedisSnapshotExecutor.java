/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.snapshot.redis;

import com.riskfocus.flink.domain.TimeAware;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.context.ContextService;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.AllArgsConstructor;
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
    private static final int expireAfterDays = 2;

    private final SnapshotMapper<T> snapshotMapper;

    @Override
    public void execute(T data, ContextService contextService, String contextName, StatefulRedisConnection<byte[], byte[]> connection) throws IOException {
        ContextMetadata ctx = contextService.generate(data, contextName);
        long contextId = ctx.getId();
        long expireAt = expireAt();
        RedisCommands<byte[], byte[]> commands = connection.sync();
        // write data to Redis in one Transaction
        commands.multi();

        if (data != null) {
            commands.psetex(snapshotMapper.buildKey(data, ctx).getBytes(), expireAt, snapshotMapper.getValueFromData(data).getBytes());
            byte[] windowBytes = String.valueOf(contextId).getBytes();
            byte[] indexKey = snapshotMapper.buildSnapshotIndexKey(ctx).getBytes();
            commands.zadd(indexKey, new ZAddArgs(), (double) contextId, windowBytes);
            TransactionResult result = commands.exec();
            if (result.wasDiscarded()) {
                throw new RuntimeException("Transaction was aborted for item: " + data);
            }
            log.debug("W{} data has been written to Redis: {}", contextId, data);
        }
    }

    protected long expireAt() {
        long now = System.currentTimeMillis();
        long future = LocalDate.now(ZoneOffset.UTC).plusDays(expireAfterDays).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        return future - now;
    }
}
