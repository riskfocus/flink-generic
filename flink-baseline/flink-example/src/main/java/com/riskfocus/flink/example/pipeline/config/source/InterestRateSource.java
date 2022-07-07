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

package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.TimeAwareWithIdlePeriodAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class InterestRateSource extends KafkaSource<InterestRate> {

    public InterestRateSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<InterestRate> build(StreamExecutionEnvironment env) {
        int interestRatesConsumerParallelism = paramUtils.getInt("interests.consumer.parallelism", env.getParallelism());
        String topic = paramUtils.getString("interest.rates.inbound.topic", "ycInputsLive");
        FlinkKafkaConsumer<InterestRate> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(InterestRate.class), buildConsumerProps());
        sourceFunction.assignTimestampsAndWatermarks(new TimeAwareWithIdlePeriodAssigner<>(getMaxIdleTimeMs(), 0));
        env.registerType(InterestRate.class);

        return env.addSource(sourceFunction)
                .setParallelism(interestRatesConsumerParallelism)
                .name("interestRateSource").uid("interestRateSource");
    }
}
