package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.kafka.KafkaSink;
import com.riskfocus.flink.example.pipeline.domain.SmoothingRequest;
import com.riskfocus.flink.schema.EventSerializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author Khokhlov Pavel
 */
public class SmoothingRequestKafkaSink extends KafkaSink<SmoothingRequest> {

    public SmoothingRequestKafkaSink(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public SinkFunction<SmoothingRequest> build() {
        String outputTopic = paramUtils.getString("output.topic", "smoothingInputsLatest");
        FlinkKafkaProducer<SmoothingRequest> kafkaProducer = new FlinkKafkaProducer<>(outputTopic,
                new EventSerializationSchema<>(outputTopic), producerProps(), getSemantic());
        kafkaProducer.setWriteTimestampToKafka(true);
        return kafkaProducer;
    }

    public SinkInfo<SmoothingRequest> buildSink() {
        return new SinkInfo<>("smoothingRequestKafkaSink", build());
    }
}