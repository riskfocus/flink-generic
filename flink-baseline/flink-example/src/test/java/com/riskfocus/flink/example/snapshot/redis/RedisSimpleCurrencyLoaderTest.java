/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.snapshot.redis;

import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.redis.WithEmbeddedRedis;
import com.riskfocus.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.riskfocus.flink.example.snapshot.redis.manager.RedisSimpleCurrencyLoader;
import com.riskfocus.flink.example.snapshot.redis.manager.SimpleCurrencyLoader;
import com.riskfocus.flink.example.snapshot.redis.manager.SimpleCurrencyMapper;
import com.riskfocus.flink.snapshot.SnapshotSink;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.context.ContextService;
import com.riskfocus.flink.snapshot.context.ContextServiceProvider;
import com.riskfocus.flink.snapshot.redis.SnapshotData;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import org.apache.flink.api.java.utils.ParameterTool;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.riskfocus.flink.config.redis.RedisProperties.REDIS_PASSWORD_PARAM_NAME;
import static com.riskfocus.flink.config.redis.RedisProperties.REDIS_PORT_PARAM_NAME;


/**
 * @author Khokhlov Pavel
 */
public class RedisSimpleCurrencyLoaderTest extends WithEmbeddedRedis {

    private static final long windowSize = 10_000;

    private SimpleCurrencyLoader currencyLoader;
    private SnapshotSink<SimpleCurrency> sink;
    private ContextService contextService;

    @Test
    public void testLoadSimpleCurrency() throws Exception {
        long now = 1586253482643L;
        SimpleCurrency usd = save(now, "USD", 10);
        Assert.assertNotNull(usd);
        SimpleCurrency eur = save(now + (windowSize * 2), "EUR", 20);
        Assert.assertNotNull(eur);
        SimpleCurrency gbp = save(now + (windowSize * 3), "GBP", 30);
        Assert.assertNotNull(gbp);


        Optional<SnapshotData<SimpleCurrency>> loadedUsd = loadByTime(now, "USD");
        Assert.assertTrue(loadedUsd.isPresent());
        Assert.assertEquals(loadedUsd.get().getElement().getCode(), "USD");

        Optional<SnapshotData<SimpleCurrency>> loadedEUR = loadByTime(now, "EUR");
        Assert.assertFalse(loadedEUR.isPresent());

        Optional<SnapshotData<SimpleCurrency>> loadedGBP = loadByTime(now, "GBP");
        Assert.assertFalse(loadedGBP.isPresent());

        loadedUsd = loadByTime(now - windowSize, "USD");
        Assert.assertFalse(loadedUsd.isPresent());

        long expectedContextId = generateCtx(now).getId();
        ContextMetadata ctx = generateCtx(now + windowSize);

        loadedUsd = load(ctx, "USD");
        Assert.assertTrue(loadedUsd.isPresent());
        Assert.assertEquals(loadedUsd.get().getContextId(), expectedContextId);
        Assert.assertEquals(loadedUsd.get().getElement().getCode(), "USD");

    }

    private Optional<SnapshotData<SimpleCurrency>> loadByTime(long timestamp, String code) throws IOException {
        ContextMetadata ctx = contextService.generate(() -> timestamp, SimpleCurrency.class.getSimpleName());
        return load(ctx, code);
    }

    private ContextMetadata generateCtx(long timestamp) {
        return contextService.generate(() -> timestamp, SimpleCurrency.class.getSimpleName());
    }

    private Optional<SnapshotData<SimpleCurrency>> load(ContextMetadata contextMetadata, String code) throws IOException {
        return currencyLoader.loadSimpleCurrency(contextMetadata, code);
    }

    private SimpleCurrency save(long timestamp, String code, double rate) throws Exception {
        SimpleCurrency rateObj = build(timestamp, code, rate);
        sink.invoke(rateObj, null);
        return rateObj;
    }

    private SimpleCurrency build(long timestamp, String code, double rate) {
        return SimpleCurrency.builder()
                .timestamp(timestamp)
                .code(code).rate(rate).build();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(REDIS_PORT_PARAM_NAME, String.valueOf(REDIS_PORT));
        properties.put(REDIS_PASSWORD_PARAM_NAME, "");

        properties.put(WindowGeneratorProvider.WINDOW_SIZE_PARAM_NAME, Long.toString(windowSize));

        ParamUtils paramUtils = new ParamUtils(ParameterTool.fromMap(properties));
        RedisProperties redisProperties = new RedisProperties(paramUtils);

        contextService = ContextServiceProvider.create(paramUtils);
        currencyLoader = new RedisSimpleCurrencyLoader(redisProperties);
        currencyLoader.init();

        sink = new SnapshotSink<>(paramUtils, new SimpleCurrencyMapper(":"),
                EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY,
                new RedisProperties(paramUtils).build());
        sink.open(null);
    }
}