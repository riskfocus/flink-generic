package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.kafka.KafkaSink;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.schema.EventSerializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author Khokhlov Pavel
 */
public class InterestRatesKafkaSink extends KafkaSink<InterestRates> {

    public InterestRatesKafkaSink(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public SinkFunction<InterestRates> build() {
        String outputTopic = paramUtils.getString("snapshot.interestRates.topic", "interestRatesSnapshot");
        FlinkKafkaProducer<InterestRates> kafkaProducer = new FlinkKafkaProducer<>(outputTopic,
                new EventSerializationSchema<>(outputTopic), producerProps(), getSemantic());
        kafkaProducer.setWriteTimestampToKafka(true);
        return kafkaProducer;
    }

    public SinkInfo<InterestRates> buildSink() {
        return new SinkInfo<>("interestRatesSnapshotKafkaSink", build());
    }
}