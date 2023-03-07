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

package com.ness.flink.example.pipeline.manager.stream;

import com.ness.flink.config.operator.KeyedProcessorDefinition;
import com.ness.flink.config.properties.OperatorProperties;
import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.example.pipeline.manager.stream.function.ProcessSmoothingFunction;
import com.ness.flink.stream.StreamBuilder;
import com.ness.flink.stream.StreamBuilder.FlinkDataStream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Builder
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OptionPriceStream {

    public static void build(@NonNull StreamBuilder streamBuilder) {

        FlinkDataStream<OptionPrice> optionPriceStream = streamBuilder.stream()
            .sourcePojo("option.price.source", OptionPrice.class,
                (SerializableTimestampAssigner<OptionPrice>) (event, recordTimestamp) -> event.getTimestamp());

        KeyedProcessorDefinition<String, OptionPrice, SmoothingRequest> ratesKeyedProcessorDefinition =
            new KeyedProcessorDefinition<>(OperatorProperties.from("interestRatesEnricher.operator",
                streamBuilder.getParameterTool()), v -> v.getUnderlying().getName(),
                new ProcessSmoothingFunction());

        FlinkDataStream<SmoothingRequest> interestRatesDataStream = optionPriceStream.addKeyedProcessor(
            ratesKeyedProcessorDefinition);

        streamBuilder.stream().sinkPojo(interestRatesDataStream, "smoothing.request.sink",
            SmoothingRequest.class, SmoothingRequest::kafkaKey, SmoothingRequest::getTimestamp);
    }
}
