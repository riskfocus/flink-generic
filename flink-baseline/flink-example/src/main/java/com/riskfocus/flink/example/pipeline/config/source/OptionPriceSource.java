package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.TimeAwareWithIdlePeriodAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.example.pipeline.domain.OptionPrice;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class OptionPriceSource extends KafkaSource<OptionPrice> {

    public OptionPriceSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<OptionPrice> build(StreamExecutionEnvironment env) {
        final int defaultParallelism = env.getParallelism();
        int pricesConsumerParallelism = paramUtils.getInt("parallelism", defaultParallelism);
        String topic = paramUtils.getString("options.prices.inbound.topic", "optionsPricesLive");
        FlinkKafkaConsumer<OptionPrice> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(OptionPrice.class), buildConsumerProps());
        sourceFunction.assignTimestampsAndWatermarks(new TimeAwareWithIdlePeriodAssigner<>(getMaxIdleTimeMs(), 0));
        env.registerType(OptionPrice.class);

        return env.addSource(sourceFunction)
                .setParallelism(pricesConsumerParallelism)
                .name("optionPriceSource").uid("optionPriceSource");
    }
}