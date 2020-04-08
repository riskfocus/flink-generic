package com.riskfocus.flink.example.pipeline.config.sink;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.kafka.KafkaSink;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.schema.EventSerializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author Khokhlov Pavel
 */
public class CustomerAccountKafkaSink extends KafkaSink<CustomerAndAccount> {

    public CustomerAccountKafkaSink(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public SinkFunction<CustomerAndAccount> build() {
        String outputTopic = paramUtils.getString("customerAndAccount.output.topic", "customerAndAccount");
        FlinkKafkaProducer<CustomerAndAccount> kafkaProducer = new FlinkKafkaProducer<>(outputTopic,
                new EventSerializationSchema<>(outputTopic), producerProps(), getSemantic());
        kafkaProducer.setWriteTimestampToKafka(true);
        return kafkaProducer;
    }

    @Override
    public SinkInfo<CustomerAndAccount> buildSink() {
        return new SinkInfo<>("customerAndAccountKafkaSink", build());
    }
}
