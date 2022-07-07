/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.riskfocus.flink.example.pipeline.snapshot;

import com.google.common.io.Resources;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.snapshot.SnapshotMapper;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.redis.RedisSnapshotConverterUtils;
import com.riskfocus.flink.snapshot.redis.SnapshotData;
import com.riskfocus.flink.example.pipeline.config.sink.mapper.InterestRatesMapper;
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
 * Redis implementation loading InterestRates
 *
 * @author Khokhlov Pavel
 */
@Slf4j
public class InterestRatesRedisImpl implements InterestRatesLoader {

    private static final long serialVersionUID = 3101876746410677069L;

    private static final RedisSnapshotConverterUtils<InterestRates> converter = new RedisSnapshotConverterUtils<>();

    private transient SnapshotMapper<InterestRates> interestRatesMapper;
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<byte[], byte[]> connect;
    private transient RedisProperties redisProperties;

    private transient String scriptDigest;

    public InterestRatesRedisImpl(RedisProperties redisProperties) {
        this.redisProperties = redisProperties;
        this.interestRatesMapper = new InterestRatesMapper(":");
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
    public Optional<SnapshotData<InterestRates>> loadInterestRates(ContextMetadata context) throws IOException {
        byte[] getKey = interestRatesMapper.buildKey(InterestRates.EMPTY, context).getBytes();
        byte[] indexKey = interestRatesMapper.buildSnapshotIndexKey(context).getBytes();
        byte[] snapshotPrefix = interestRatesMapper.buildSnapshotPrefix(context).getBytes();

        byte[][] keys = convert(getKey, indexKey, snapshotPrefix);
        byte[][] values = convert(InterestRates.EMPTY.getCurrency().getBytes(), String.valueOf(context.getId()).getBytes(), context.getDate().getBytes());

        byte[] data = connect.sync().evalsha(scriptDigest, ScriptOutputType.VALUE, keys, values);
        if (data != null) {
            return Optional.of(converter.convertTo(InterestRates.class, data));
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
