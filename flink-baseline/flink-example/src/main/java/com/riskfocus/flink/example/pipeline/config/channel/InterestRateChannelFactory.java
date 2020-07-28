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

package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkUtils;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.example.pipeline.config.sink.InterestRatesKafkaSink;
import com.riskfocus.flink.example.pipeline.config.sink.InterestRatesSink;
import com.riskfocus.flink.example.pipeline.config.source.InterestRateSource;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.storage.cache.EntityTypeEnum;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
public class InterestRateChannelFactory {

    public Source<InterestRate> buildSource(ParamUtils paramUtils) {
        return new InterestRateSource(paramUtils);
    }

    public Collection<SinkInfo<InterestRates>> buildSinks(ParamUtils paramUtils, ChannelProperties channelProperties) {
        InterestRatesSink interestRatesSink = new InterestRatesSink(paramUtils, new RedisProperties(paramUtils), EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY);

        SinkUtils<InterestRates> sinkUtils = new SinkUtils<>();
        if (channelProperties.isInterestRatesKafkaSnapshotEnabled()) {
            return sinkUtils.build(interestRatesSink, new InterestRatesKafkaSink(paramUtils));
        } else {
            return sinkUtils.build(interestRatesSink);
        }
    }

}
