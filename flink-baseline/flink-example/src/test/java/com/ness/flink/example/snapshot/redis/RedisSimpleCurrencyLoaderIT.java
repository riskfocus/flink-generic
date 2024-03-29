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

package com.ness.flink.example.snapshot.redis;

import com.ness.flink.config.properties.RedisProperties;
import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.ness.flink.example.snapshot.redis.manager.RedisSimpleCurrencyLoader;
import com.ness.flink.example.snapshot.redis.manager.SimpleCurrencyLoader;
import com.ness.flink.example.snapshot.redis.manager.SimpleCurrencyMapper;
import com.ness.flink.snapshot.SnapshotSink;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.properties.ContextProperties;
import com.ness.flink.snapshot.context.ContextService;
import com.ness.flink.snapshot.context.ContextServiceProvider;
import com.ness.flink.snapshot.redis.SnapshotData;
import com.ness.flink.storage.cache.EntityTypeEnum;
import com.ness.flink.stream.StreamBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;


/**
 * Test which saves data to EmbeddedRedis and checks if it was saved or not (load data from Redis)
 * @author Khokhlov Pavel
 */
@Slf4j
class RedisSimpleCurrencyLoaderIT {

    private static final long windowSize = 10_000;

    private static ContextService contextService;
    private static SimpleCurrencyLoader currencyLoader;
    private static final int REDIS_PORT = 6379;
    private static ParameterTool parameterTool;
    private static GenericContainer redis;


    @SneakyThrows
    @BeforeAll
    static void setup() {

        redis = new GenericContainer(DockerImageName.parse("redis:7.2.4"))
            .withExposedPorts(REDIS_PORT).withLogConsumer(new Slf4jLogConsumer(log));
        redis.start();
        parameterTool = ParameterTool.fromMap(Map.of(
            "redis.port", String.valueOf(redis.getMappedPort(REDIS_PORT)),
            "redis.password", "",
            "watermark.windowSizeMs", Long.toString(windowSize)));

        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());
        currencyLoader = new RedisSimpleCurrencyLoader(RedisProperties.from(parameterTool));
        currencyLoader.init();
        WatermarkProperties watermarkProperties = WatermarkProperties.from(parameterTool);
        contextService = ContextServiceProvider.create(ContextProperties.from(parameterTool), watermarkProperties);

    }

    @SneakyThrows
    @AfterAll
    static void close() {
        if (currencyLoader != null) {
            currencyLoader.close();
        }
        if (redis != null) {
            redis.close();
        }
    }


    @Test
    void shouldLoadSimpleCurrency() throws Exception {

        StreamBuilder streamBuilder = StreamBuilder.from(parameterTool);
        StreamExecutionEnvironment env = streamBuilder.getEnv();
        env.setParallelism(2);

        SnapshotSink<SimpleCurrency> sink = new SnapshotSink<>(new SimpleCurrencyMapper(":"),
            EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY, parameterTool);

        long now = 1586253482643L;
        SimpleCurrency usd = save(now, "USD", 10);
        SimpleCurrency eur = save(now + (windowSize * 2), "EUR", 20);
        SimpleCurrency gbp = save(now + (windowSize * 3), "GBP", 30);

        // create a stream of custom elements and apply transformations
        env.fromElements(usd, eur, gbp).name("source").uid("source")
                .sinkTo(sink).uid("test").name("test");
        env.execute();

        Optional<SnapshotData<SimpleCurrency>> loadedUsd = loadByTime(now, "USD");
        Assertions.assertTrue(loadedUsd.isPresent());
        Assertions.assertEquals("USD", loadedUsd.get().getElement().getCode());

        Optional<SnapshotData<SimpleCurrency>> loadedEUR = loadByTime(now, "EUR");
        Assertions.assertFalse(loadedEUR.isPresent());

        Optional<SnapshotData<SimpleCurrency>> loadedGBP = loadByTime(now, "GBP");
        Assertions.assertFalse(loadedGBP.isPresent());

        loadedUsd = loadByTime(now - windowSize, "USD");
        Assertions.assertFalse(loadedUsd.isPresent());

        long expectedContextId = generateCtx(now).getContextId();
        ContextMetadata ctx = generateCtx(now + windowSize);

        loadedUsd = load(ctx, "USD");
        Assertions.assertTrue(loadedUsd.isPresent());
        Assertions.assertEquals(loadedUsd.get().getContextId(), expectedContextId);
        Assertions.assertEquals("USD", loadedUsd.get().getElement().getCode());

    }

    static Optional<SnapshotData<SimpleCurrency>> loadByTime(long timestamp, String code) throws IOException {
        ContextMetadata ctx = contextService.generate(() -> timestamp, SimpleCurrency.class.getSimpleName());
        return load(ctx, code);
    }

    static ContextMetadata generateCtx(long timestamp) {
        return contextService.generate(() -> timestamp, SimpleCurrency.class.getSimpleName());
    }

    static Optional<SnapshotData<SimpleCurrency>> load(ContextMetadata contextMetadata, String code)
        throws IOException {
        return currencyLoader.loadSimpleCurrency(contextMetadata, code);
    }

    static SimpleCurrency save(long timestamp, String code, double rate) {
        return build(timestamp, code, rate);
    }

    static SimpleCurrency build(long timestamp, String code, double rate) {
        return SimpleCurrency.builder()
            .timestamp(timestamp)
            .code(code).rate(rate).build();
    }

}