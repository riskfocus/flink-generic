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

package com.riskfocus.flink.example.pipeline.manager.stream;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.example.pipeline.config.channel.OptionPriceChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.OptionPrice;
import com.riskfocus.flink.example.pipeline.domain.SmoothingRequest;
import com.riskfocus.flink.example.pipeline.manager.stream.function.ProcessSmoothingFunction;
import com.riskfocus.flink.util.ParamUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Builder
public class OptionPriceStream {

    public static void build(@NonNull ParamUtils paramUtils, @NonNull StreamExecutionEnvironment env,
                             @NonNull OptionPriceChannelFactory channelFactory) {

        int defaultParallelism = env.getParallelism();
        DataStream<OptionPrice> optionPriceStream = channelFactory.buildSource(paramUtils).build(env);
        Collection<SinkInfo<SmoothingRequest>> requestSinks = channelFactory.buildSinks(paramUtils);

        SingleOutputStreamOperator<SmoothingRequest> smoothingRequestStream = optionPriceStream
                .keyBy(value -> value.getUnderlying().getName())
                .process(new ProcessSmoothingFunction())
                .name("interestRatesEnricher").uid("interestRatesEnricher")
                .setParallelism(defaultParallelism);

        requestSinks.forEach(sink -> smoothingRequestStream.addSink(sink.getFunction())
                .setParallelism(defaultParallelism)
                .name(sink.getName()).uid(sink.getName()));

    }
}
