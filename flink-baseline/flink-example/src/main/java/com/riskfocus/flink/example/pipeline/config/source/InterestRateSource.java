package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
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
        long maxLagTimeMs = getMaxLagTimeMs();
        int interestRatesConsumerParallelism = paramUtils.getInt("interests.consumer.parallelism", env.getParallelism());
        String topic = paramUtils.getString("interest.rates.inbound.topic", "ycInputsLive");
        FlinkKafkaConsumer<InterestRate> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(InterestRate.class), buildConsumerProps());
        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(maxLagTimeMs, 0));
        env.registerType(InterestRate.class);

        return env.addSource(sourceFunction)
                .setParallelism(interestRatesConsumerParallelism)
                .name("interestRateSource").uid("interestRateSource");
    }
}
