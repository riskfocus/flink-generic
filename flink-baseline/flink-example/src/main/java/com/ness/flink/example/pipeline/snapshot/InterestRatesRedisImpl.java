//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.example.pipeline.snapshot;

import com.google.common.io.Resources;
import com.ness.flink.config.properties.RedisProperties;
import com.ness.flink.example.pipeline.config.sink.mapper.InterestRatesMapper;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.snapshot.SnapshotMapper;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.redis.RedisSnapshotConverterUtils;
import com.ness.flink.snapshot.redis.SnapshotData;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.ness.flink.util.ByteUtils.convert;

/**
 * Redis implementation loading InterestRates
 *
 * @author Khokhlov Pavel
 */
@Slf4j
public class InterestRatesRedisImpl implements InterestRatesLoader {

    private static final long serialVersionUID = 3101876746410677069L;

    //private static final RedisSnapshotConverterUtils<InterestRates> CONVERTER = new RedisSnapshotConverterUtils<>();

    private transient SnapshotMapper<InterestRates> interestRatesMapper;
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<byte[], byte[]> connect;

    private transient String scriptDigest;

    @Override
    public void init(ParameterTool parameterTool) throws IOException {
        RedisProperties redisProperties = RedisProperties.from(parameterTool);
        interestRatesMapper = new InterestRatesMapper(":");
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
        byte[] getKey = interestRatesMapper.buildKey(InterestRates.EMPTY_RATES, context).getBytes();
        byte[] indexKey = interestRatesMapper.buildSnapshotIndexKey(context).getBytes();
        byte[] snapshotPrefix = interestRatesMapper.buildSnapshotPrefix(context).getBytes();

        byte[][] keys = convert(getKey, indexKey, snapshotPrefix);
        byte[][] values = convert(InterestRates.EMPTY_RATES.getCurrency().getBytes(), String.valueOf(context.getId()).getBytes(), context.getDate().getBytes());

        byte[] data = connect.sync().evalsha(scriptDigest, ScriptOutputType.VALUE, keys, values);
        if (data != null) {
            return Optional.of(RedisSnapshotConverterUtils.convertTo(InterestRates.class, data));
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
