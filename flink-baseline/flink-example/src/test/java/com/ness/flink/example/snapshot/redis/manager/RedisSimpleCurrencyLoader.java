package com.ness.flink.example.snapshot.redis.manager;

import com.google.common.io.Resources;
import com.ness.flink.config.properties.RedisProperties;
import com.ness.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.redis.RedisSnapshotConverterUtils;
import com.ness.flink.snapshot.redis.SnapshotData;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.ness.flink.util.ByteUtils.convert;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class RedisSimpleCurrencyLoader implements SimpleCurrencyLoader {

    private static final RedisSnapshotConverterUtils<SimpleCurrency> converter = new RedisSnapshotConverterUtils<>();

    private transient SnapshotMapper<SimpleCurrency> interestRatesMapper;
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<byte[], byte[]> connect;
    private transient RedisProperties redisProperties;

    private transient String scriptDigest;

    public RedisSimpleCurrencyLoader(RedisProperties redisProperties) {
        this.redisProperties = redisProperties;
        this.interestRatesMapper = new SimpleCurrencyMapper(":");
    }

    @Override
    public void init() throws IOException {
        redisClient = RedisClient.create(redisProperties.build());
        connect = redisClient.connect(new ByteArrayCodec());
        // load Lua script from class path
        URL url = Resources.getResource("snapshot-loader.lua");
        String luaScript = Resources.toString(url, StandardCharsets.UTF_8);
        // load script to Redis
        scriptDigest = connect.sync().scriptLoad(luaScript.getBytes());
    }

    @Override
    public Optional<SnapshotData<SimpleCurrency>> loadSimpleCurrency(ContextMetadata contextMetadata, String code) throws IOException {
        SimpleCurrency currency = SimpleCurrency.builder().code(code).build();
        String directKeyStr = interestRatesMapper.buildKey(currency, contextMetadata);
        String indexKeyStr = interestRatesMapper.buildSnapshotIndexKey(contextMetadata);
        String snapshotPrefixStr = interestRatesMapper.buildSnapshotPrefix(contextMetadata);
        byte[] directKey = directKeyStr.getBytes();
        byte[] indexKey = indexKeyStr.getBytes();
        byte[] snapshotPrefix = snapshotPrefixStr.getBytes();

        long ctxId = contextMetadata.getId();
        String dateStr = contextMetadata.getDate();

        byte[][] keys = convert(directKey, indexKey, snapshotPrefix);

        log.debug("directKey: {}, identifier: {}, ctxId: {}, date: {}", directKeyStr, code, ctxId, dateStr);
        byte[][] values = convert(code.getBytes(), String.valueOf(ctxId).getBytes(), dateStr.getBytes());

        byte[] data = connect.sync().evalsha(scriptDigest, ScriptOutputType.VALUE, keys, values);
        if (data != null) {
            return Optional.of(converter.convertTo(SimpleCurrency.class, data));
        }
        return Optional.empty();

    }

    @Override
    public void close() {
        log.debug("Closing Redis connection");
        if (connect != null) {
            connect.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
