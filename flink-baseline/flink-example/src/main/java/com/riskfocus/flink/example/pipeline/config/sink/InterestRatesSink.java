/*
 * Copyright 2020 Risk Focus Inc
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
