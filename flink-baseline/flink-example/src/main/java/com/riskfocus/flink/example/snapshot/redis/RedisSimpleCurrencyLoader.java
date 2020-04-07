package com.riskfocus.flink.example.snapshot.redis;

import com.google.common.io.Resources;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.domain.SimpleCurrency;
import com.riskfocus.flink.example.snapshot.SimpleCurrencyLoader;
import com.riskfocus.flink.example.snapshot.SimpleCurrencyMapper;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.redis.RedisSnapshotConverterUtils;
import com.riskfocus.flink.snapshot.redis.SnapshotData;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.riskfocus.flink.util.ByteUtils.convert;

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
