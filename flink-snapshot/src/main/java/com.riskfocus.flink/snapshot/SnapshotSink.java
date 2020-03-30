package com.riskfocus.flink.snapshot;

import com.riskfocus.flink.snapshot.redis.RedisSnapshotExecutor;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author Khokhlov Pavel
 */
public class SnapshotSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 6805501266870217945L;

    private final EntityTypeEnum entityTypeEnum;
    private final SnapshotMapper<IN> snapshotMapper;
    private RedisURI redisURI;

    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<byte[], byte[]> connect;

    public SnapshotSink(SnapshotMapper<IN> snapshotMapper, EntityTypeEnum entityTypeEnum) {
        this.snapshotMapper = snapshotMapper;
        this.entityTypeEnum = entityTypeEnum;
    }

    public SnapshotSink(SnapshotMapper<IN> snapshotMapper, EntityTypeEnum entityTypeEnum, RedisURI redisURI) {
        this(snapshotMapper, entityTypeEnum);
        this.redisURI = redisURI;
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        switch (entityTypeEnum) {
            case MEM_CACHE_WITH_INDEX_SUPPORT_ONLY:
                new RedisSnapshotExecutor<>(snapshotMapper).execute(input, connect);
                return;
            case MEM_CACHE_ONLY:
                // todo
                // Cache.put
            default:
                throw new IllegalArgumentException("Implementation required");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        switch (entityTypeEnum) {
            case MEM_CACHE_WITH_INDEX_SUPPORT_ONLY:
                redisClient = RedisClient.create(redisURI);
                connect = redisClient.connect(new ByteArrayCodec());
                return;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connect != null) {
            connect.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
