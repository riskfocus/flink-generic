/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.Sink;
import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkMetaInfo;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.config.sink.mapper.InterestRatesMapper;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.snapshot.SnapshotSink;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class InterestRatesSink implements Sink<InterestRates>, SinkMetaInfo<InterestRates> {

    private final ParamUtils paramUtils;
    private final RedisProperties redisProperties;
    private final EntityTypeEnum entityTypeEnum;

    @Override
    public SinkFunction<InterestRates> build() {
        return new SnapshotSink<>(paramUtils, new InterestRatesMapper(entityTypeEnum.getDelimiter()), entityTypeEnum, redisProperties.build());
    }

    @Override
    public SinkInfo<InterestRates> buildSink() {
        return new SinkInfo<>("interestRatesSnapshotSink", build());
    }
}
