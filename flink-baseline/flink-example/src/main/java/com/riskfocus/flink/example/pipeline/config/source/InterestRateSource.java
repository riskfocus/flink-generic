/*
 * Copyright (c) 2020 Risk Focus, Inc.
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
