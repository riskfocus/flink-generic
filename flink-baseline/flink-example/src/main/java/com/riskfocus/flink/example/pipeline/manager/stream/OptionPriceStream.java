/*
 * Copyright (c) 2020 Risk Focus, Inc.
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
