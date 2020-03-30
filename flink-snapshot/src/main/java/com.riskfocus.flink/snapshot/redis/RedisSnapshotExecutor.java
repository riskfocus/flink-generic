package com.riskfocus.flink.snapshot.redis;

import com.riskfocus.flink.redis.RedisExecutor;
import com.riskfocus.flink.snapshot.SnapshotMapper;
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
public class RedisSnapshotExecutor<T> implements RedisExecutor<T> {

    private static final long serialVersionUID = -6536967976207067864L;
    private static final int expireAfterDays = 2;

    private final SnapshotMapper<T> snapshotMapper;

    @Override
    public void execute(T data, StatefulRedisConnection<byte[], byte[]> connection) throws IOException {
        long windowId = snapshotMapper.getWindowId(data);
        long expireAt = expireAt();
        RedisCommands<byte[], byte[]> commands = connection.sync();
        // write data to Redis in one Transaction
        commands.multi();

        if (data != null) {
            commands.psetex(snapshotMapper.buildKey(data, windowId), expireAt, snapshotMapper.getValueFromData(data));
            byte[] windowBytes = String.valueOf(windowId).getBytes();
            byte[] indexKey = snapshotMapper.buildIndexKey();
            commands.zadd(indexKey, new ZAddArgs(), (double) windowId, windowBytes);
            TransactionResult result = commands.exec();
            if (result.wasDiscarded()) {
                throw new RuntimeException("Transaction was aborted for item: " + data);
            }
            log.debug("W{} data has been written to Redis: {}", windowId, data);
        }
    }

    protected long expireAt() {
        long now = System.currentTimeMillis();
        long future = LocalDate.now(ZoneOffset.UTC).plusDays(expireAfterDays).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        return future - now;
    }
}
