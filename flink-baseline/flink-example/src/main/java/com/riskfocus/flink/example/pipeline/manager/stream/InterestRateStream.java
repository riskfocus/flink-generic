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

package com.riskfocus.flink.example.pipeline.manager.stream;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.example.pipeline.config.channel.ChannelProperties;
import com.riskfocus.flink.example.pipeline.config.channel.InterestRateChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.example.pipeline.manager.stream.function.ProcessRatesFunction;
import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterestRateStream {

    public static void build(@NonNull ParamUtils paramUtils,
                             @NonNull StreamExecutionEnvironment env,
                             @NonNull InterestRateChannelFactory channelFactory,
                             @NonNull ChannelProperties channelProperties) {

        final int ratesParallelism = 1;

        DataStream<InterestRate> rates = channelFactory.buildSource(paramUtils).build(env);
        Collection<SinkInfo<InterestRates>> ratesSinks = channelFactory.buildSinks(paramUtils, channelProperties);

        SingleOutputStreamOperator<InterestRates> reducedByCurrency = rates
                .keyBy(value -> InterestRates.EMPTY.getCurrency())
                .process(new ProcessRatesFunction())
                .setParallelism(ratesParallelism)
                .uid("reduceByUSDCurrency").name("reduceByUSDCurrency");

        ratesSinks.forEach(sink -> reducedByCurrency
                .addSink(sink.getFunction())
                .setParallelism(ratesParallelism)
                .name(sink.getName()).uid(sink.getName()));
    }
}
