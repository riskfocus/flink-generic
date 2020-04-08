package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.kafka.KafkaSink;
import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.schema.EventSerializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author Khokhlov Pavel
 */
public class PurchaseKafkaSink extends KafkaSink<Purchase> {

    public PurchaseKafkaSink(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public SinkFunction<Purchase> build() {
        String outputTopic = paramUtils.getString("output.topic", "purchase");
        FlinkKafkaProducer<Purchase> kafkaProducer = new FlinkKafkaProducer<>(outputTopic,
                new EventSerializationSchema<>(outputTopic), producerProps(), getSemantic());
        kafkaProducer.setWriteTimestampToKafka(true);
        return kafkaProducer;
    }

    @Override
    public SinkInfo<Purchase> buildSink() {
        return new SinkInfo<>("purchase", build());
    }
}
