/*
 * Copyright 2020-2023 Ness USA, Inc.
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

package com.ness.flink.example.pipeline.manager.stream;

import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.domain.IncomingEvent;
import com.ness.flink.example.pipeline.config.sink.InterestRatesSink;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.example.pipeline.manager.stream.function.ProcessRatesFunction;
import com.ness.flink.storage.cache.EntityTypeEnum;
import com.ness.flink.stream.StreamBuilder;
import com.ness.flink.stream.StreamBuilder.FlinkDataStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterestRateStream {

    public static void build(@NonNull StreamBuilder streamBuilder, boolean interestRatesKafkaSnapshotEnabled) {

        FlinkDataStream<InterestRate> interestRateFlinkDataStream = streamBuilder.stream()
            .sourcePojo("interest.rates.source",
                InterestRate.class,
                (SerializableTimestampAssigner<InterestRate>) (event, recordTimestamp) -> event.getTimestamp());

        KeyedProcessorDefinition<String, InterestRate, InterestRates> ratesKeyedProcessorDefinition =
            new KeyedProcessorDefinition<>(OperatorProperties.from("reduceByUSDCurrency.operator",
                streamBuilder.getParameterTool()), v -> InterestRates.EMPTY.getCurrency(),
            new ProcessRatesFunction());

        FlinkDataStream<InterestRates> interestRatesDataStream = interestRateFlinkDataStream.addKeyedProcessor(
            ratesKeyedProcessorDefinition);

        buildSinks(streamBuilder, interestRatesKafkaSnapshotEnabled, interestRatesDataStream);

    }

    static void buildSinks(StreamBuilder streamBuilder,
        boolean interestRatesKafkaSnapshotEnabled, FlinkDataStream<InterestRates> interestRatesDataStream) {

        if (interestRatesKafkaSnapshotEnabled) {
            streamBuilder.stream().sinkPojo(interestRatesDataStream,
                "interest.rates.snapshot.sink", InterestRates.class, InterestRates::kafkaKey,
                IncomingEvent::getTimestamp);
        } else {
            OperatorProperties operatorProperties = OperatorProperties
                    .from("interest.rates.snapshot.redis.sink", streamBuilder.getParameterTool());
            InterestRatesSink interestRatesSink = InterestRatesSink.builder()
                    .parameterTool(streamBuilder.getParameterTool())
                    .operatorProperties(operatorProperties)
                    .entityTypeEnum(EntityTypeEnum.MEM_CACHE_WITH_INDEX_SUPPORT_ONLY)
                    .build();
            streamBuilder.stream().sink(interestRatesDataStream, interestRatesSink);
        }
    }
}
